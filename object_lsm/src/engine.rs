//! Engine implementation: open/recovery, commit pipeline, segment flush,
//! merge compaction, garbage collection, manifest publishing and the
//! [`Engine`] trait impl.

use std::{
  collections::BTreeMap,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

use parking_lot::RwLock;
use wedb_embed_engine::Engine;

use crate::{
  batch::ObjectLsmBatch,
  cache::{BlockCache, IndexCache},
  config::Config,
  error::{Error, Result},
  journal::{Group, Op, decode_group_stream, encode_group},
  keys::{
    current_key, journal_key, journal_prefix, manifest_key, manifest_prefix, parse_tail_seq,
    segment_key, segment_root,
  },
  lease::{Lease, LeaseOptions},
  manifest::{Manifest, PartitionMeta},
  partition::ObjectLsmPartition,
  segment::{
    BLOCK_HEADER_LEN, BlockMeta, SegmentEntries, SegmentIndex, SegmentMeta, TAIL_LEN,
    build_segment_meta, decode_block, decode_index, encode_segment, find_block, parse_tail,
  },
  state::{EngineState, MemEntry, PartitionState},
  store::Store,
};

/// Shared internals behind [`ObjectLsm`].
pub struct Inner {
  pub store: Arc<dyn Store>,
  pub state: RwLock<EngineState>,
  pub block_cache: BlockCache,
  pub index_cache: IndexCache,
  /// Stops the background group-commit journal flusher thread.
  pub journal_stop: AtomicBool,
}

/// Object-storage-backed LSM engine implementing the wedb_embed_engine traits.
///
/// # Consistency model
/// - every [`Batch`] commit first uploads one immutable journal group object
///   (the atomic durability point), then applies the group to memtables;
/// - memtables spill into immutable block-indexed segment objects once they
///   exceed `Config::max_memtable_bytes`;
/// - a single manifest object chain records live segments + per-partition
///   journal watermarks;
/// - opening re-reads `current -> manifest` and replays journal groups newer
///   than each partition watermark, so a crash loses nothing that was acked.
///
/// # Compaction & GC (M3)
/// - partitions with more than `Config::max_segments_before_compact` segments
///   are merged into one fresh segment (newest-wins, tombstones dropped only
///   after every older segment has been folded in);
/// - merge publishing order is `upload new -> publish manifest -> delete old`,
///   so a crash between steps only leaves orphan objects, never lost data;
/// - applied journal groups (`seq <= min partition watermark`) are deleted;
/// - opening garbage-collects segment objects not referenced by the current
///   manifest and superseded manifest snapshots.
///
/// [`Batch`]: wedb_embed_engine::Batch
#[derive(Clone)]
pub struct ObjectLsm {
  pub(crate) inner: Arc<Inner>,
  /// Held writer lease when opened via [`ObjectLsm::open_leased`]; keeps the
  /// heartbeat alive for the engine's lifetime and releases on drop.
  pub(crate) lease: Option<Arc<Lease>>,
}

impl std::fmt::Debug for ObjectLsm {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ObjectLsm")
      .field("prefix", &self.inner.state.read().cfg.prefix)
      .finish()
  }
}

impl ObjectLsm {
  /// Open (or create) an engine instance in `store` under `cfg.prefix`.
  pub fn open(store: Arc<dyn Store>, cfg: Config) -> Result<Self> {
    Self::build(store, cfg)
  }

  /// Open as the exclusive writer of `cfg.prefix`, acquiring an expiring
  /// object lease first (for multi-instance access to a shared bucket).
  ///
  /// Fails once [`LeaseOptions::timeout`] elapses while another writer holds
  /// the lease. The lease is renewed by a background heartbeat and released
  /// when the last handle to this engine is dropped.
  pub fn open_leased(store: Arc<dyn Store>, cfg: Config, opts: LeaseOptions) -> Result<Self> {
    let lease = Lease::acquire(store.clone(), &cfg.prefix, opts)?;
    let mut engine = Self::build(store, cfg)?;
    engine.lease = Some(Arc::new(lease));
    Ok(engine)
  }

  fn build(store: Arc<dyn Store>, cfg: Config) -> Result<Self> {
    let block_cache = BlockCache::new(cfg.cache_capacity);
    let index_cache = IndexCache::default();
    let state = recover(&*store, &cfg, &index_cache)?;
    let inner = Arc::new(Inner {
      store,
      state: RwLock::new(state),
      block_cache,
      index_cache,
      journal_stop: AtomicBool::new(false),
    });
    if cfg.journal_window_ms.is_some() {
      spawn_journal_flusher(inner.clone());
    }
    Ok(Self { inner, lease: None })
  }
}

impl Drop for ObjectLsm {
  fn drop(&mut self) {
    // Graceful shutdown: flush buffered group-commit journal before stopping
    // the background flusher, so a clean close does not lose acked writes.
    if self.inner.state.read().cfg.journal_window_ms.is_some() {
      let mut st = self.inner.state.write();
      let _ = flush_journal_pending(&*self.inner.store, &mut st);
    }
    self.inner.journal_stop.store(true, Ordering::SeqCst);
    if let Some(lease) = self.lease.take() {
      drop(lease);
    }
  }
}

/// Rebuild in-memory state from the durable manifest + journal tail, then run
/// startup compaction/G C.
fn recover(store: &dyn Store, cfg: &Config, index_cache: &IndexCache) -> Result<EngineState> {
  let mut st = EngineState::new(cfg.clone());
  let prefix = &cfg.prefix;

  if let Some(cur) = store.get(&current_key(prefix))? {
    let text =
      std::str::from_utf8(&cur).map_err(|e| Error::Corrupt(format!("current not utf-8: {e}")))?;
    let mseq: u64 = text
      .trim()
      .parse()
      .map_err(|e| Error::Corrupt(format!("current not a seq: {e}")))?;
    let man_bytes = store
      .get(&manifest_key(prefix, mseq))?
      .ok_or_else(|| Error::Corrupt(format!("manifest {mseq} missing")))?;
    let man = Manifest::decode(&man_bytes)?;
    st.manifest_seq = man.seq;
    st.next_segment_id = man.next_segment_id;
    st.journal_seq = man.next_journal_seq;
    for (name, pm) in man.partitions {
      for seg in &pm.segments {
        if let Some(index) = &seg.index {
          index_cache.insert(seg.id, Arc::new(index.clone()));
        }
      }
      st.partitions.insert(
        name.clone(),
        PartitionState {
          name,
          mem: BTreeMap::new(),
          mem_bytes: 0,
          segments: pm.segments,
          watermark: pm.watermark,
          dropped: pm.dropped,
        },
      );
    }
  }

  // Replay every journal group newer than its partition watermark.
  let list = store.list(&journal_prefix(prefix))?;
  let mut max_seq = st.journal_seq;
  for k in &list {
    if let Some(s) = parse_tail_seq(k) {
      max_seq = max_seq.max(s);
      st.journal_seqs.insert(s);
    }
  }
  st.journal_seq = max_seq;
  let min_wm = st
    .partitions
    .values()
    .map(|p| p.watermark)
    .min()
    .unwrap_or(0);
  let mut seqs: Vec<u64> = list.iter().filter_map(|k| parse_tail_seq(k)).collect();
  seqs.sort_unstable();
  for s in seqs {
    // Groups in an object whose end-seq is at/below every partition watermark
    // are already folded into durable segments; skip the whole object.
    if s <= min_wm {
      continue;
    }
    let Some(bytes) = store.get(&journal_key(prefix, s))? else {
      continue;
    };
    for group in decode_group_stream(&bytes)? {
      apply_group(&mut st, &group)?;
    }
  }

  // Flush replayed memtables that already exceeded the budget.
  let over: Vec<String> = st
    .partitions
    .values()
    .filter(|p| !p.dropped && p.mem_bytes > cfg.max_memtable_bytes)
    .map(|p| p.name.clone())
    .collect();
  for name in over {
    flush_partition(store, &mut st, &name)?;
  }
  maybe_compact_all(store, &mut st)?;
  gc_journal(store, &mut st);
  gc_objects_at_open(store, &mut st)?;
  Ok(st)
}

/// Apply a committed group to partition memtables, skipping partitions whose
/// watermark already folded the group into durable segments.
fn apply_group(st: &mut EngineState, group: &Group) -> Result<()> {
  for op in &group.ops {
    let wm = st
      .partitions
      .get(&op.part)
      .map(|p| p.watermark)
      .unwrap_or(0);
    if group.seq <= wm {
      continue;
    }
    let ps = st
      .partitions
      .entry(op.part.clone())
      .or_insert_with(|| PartitionState::new(op.part.clone()));
    ps.apply(&op.key, op.value.as_deref());
  }
  Ok(())
}

/// Publish a new manifest snapshot: write `manifest/<seq+1>` then flip
/// `manifest/current` (the single atomic visibility point).
fn publish_manifest(store: &dyn Store, st: &mut EngineState) -> Result<()> {
  // In windowed (group-commit) mode make every pending group durable before
  // publishing a manifest whose watermarks may fold those groups into
  // segments or drops.
  if st.cfg.journal_window_ms.is_some() && !st.pending.is_empty() {
    flush_journal_pending(store, st)?;
  }
  let mut man = Manifest {
    seq: st.manifest_seq + 1,
    next_segment_id: st.next_segment_id,
    next_journal_seq: st.journal_seq,
    partitions: BTreeMap::new(),
  };
  for (name, ps) in &st.partitions {
    man.partitions.insert(
      name.clone(),
      PartitionMeta {
        segments: ps.segments.clone(),
        watermark: ps.watermark,
        dropped: ps.dropped,
      },
    );
  }
  st.manifest_seq = man.seq;
  let bytes = man.encode()?;
  store.put(&manifest_key(&st.cfg.prefix, man.seq), &bytes)?;
  store.put(&current_key(&st.cfg.prefix), man.seq.to_string().as_bytes())?;
  Ok(())
}

/// Flush one partition's memtable into an immutable block-indexed segment and
/// advance its watermark.
fn flush_partition(store: &dyn Store, st: &mut EngineState, name: &str) -> Result<()> {
  {
    let ps = match st.partitions.get_mut(name) {
      Some(ps) if !ps.dropped => ps,
      _ => return Ok(()),
    };
    if ps.mem.is_empty() {
      return Ok(());
    }
    let entries: SegmentEntries = ps
      .mem
      .iter()
      .map(|(k, e)| {
        let v = match e {
          MemEntry::Value(v) => Some(v.clone()),
          MemEntry::Tombstone => None,
        };
        (k.clone(), v)
      })
      .collect();
    let encoded = encode_segment(&entries, st.cfg.block_size as usize)?;
    let id = st.next_segment_id;
    st.next_segment_id += 1;
    store.put(&segment_key(&st.cfg.prefix, name, id), &encoded)?;
    let meta = build_segment_meta(id, st.journal_seq, &encoded, &entries)?;
    ps.segments.push(meta);
    ps.watermark = st.journal_seq;
    ps.mem.clear();
    ps.mem_bytes = 0;
  }
  publish_manifest(store, st)
}

/// Read every entry of a segment object via tail/index/block Range GETs
/// (no caching; used by the compaction merge).
fn read_segment_entries(
  store: &dyn Store,
  prefix: &str,
  part: &str,
  seg: &SegmentMeta,
) -> Result<SegmentEntries> {
  let key = segment_key(prefix, part, seg.id);
  let tail_raw = store
    .get_range(
      &key,
      seg.bytes.saturating_sub(TAIL_LEN as u64),
      TAIL_LEN as u64,
    )?
    .ok_or_else(|| Error::Corrupt(format!("segment {} tail missing", seg.id)))?;
  let tail = parse_tail(&tail_raw)?;
  let idx_raw = store
    .get_range(&key, tail.index_offset as u64, tail.index_len as u64)?
    .ok_or_else(|| Error::Corrupt(format!("segment {} index missing", seg.id)))?;
  let index = decode_index(&idx_raw)?;
  let mut out = SegmentEntries::new();
  for bm in &index.blocks {
    let raw = store
      .get_range(&key, bm.offset as u64, bm.len as u64)?
      .ok_or_else(|| Error::Corrupt(format!("segment {} block missing", seg.id)))?;
    if raw.len() < BLOCK_HEADER_LEN {
      return Err(Error::Corrupt("block shorter than header".into()));
    }
    out.extend(decode_block(&raw)?);
  }
  Ok(out)
}

/// Merge every segment of a partition into one fresh segment: read newest ->
/// oldest, keep only the surviving values, drop tombstones only after all
/// older segments have been folded in, publish, then (when eager deletion is
/// enabled) delete the old objects.
fn compact_partition_locked(store: &dyn Store, st: &mut EngineState, name: &str) -> Result<bool> {
  flush_partition(store, st, name)?;
  let prefix = st.cfg.prefix.clone();
  let eager = st.cfg.eager_object_delete;
  let need = {
    let ps = st.partitions.get(name);
    matches!(ps, Some(p) if !p.dropped && p.segments.len() > 1)
  };
  if !need {
    return Ok(false);
  }
  // 1. fold all segments newest -> oldest into a single decision map.
  let mut map: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
  {
    let ps = st.partitions.get(name).expect("partition present");
    for seg in ps.segments.iter().rev() {
      let entries = read_segment_entries(store, &prefix, name, seg)?;
      for (k, v) in entries {
        map.entry(k).or_insert(v);
      }
    }
  }
  // 2. drop tombstones: nothing older than the merged run survives.
  let mut out: SegmentEntries = Vec::with_capacity(map.len());
  for (k, v) in map {
    if let Some(val) = v {
      out.push((k, Some(val)));
    }
  }
  // 3. write the merged result (or none), publish, then delete the old run.
  let ps = st.partitions.get_mut(name).expect("partition present");
  let old = std::mem::take(&mut ps.segments);
  if !out.is_empty() {
    let encoded = encode_segment(&out, st.cfg.block_size as usize)?;
    let id = st.next_segment_id;
    st.next_segment_id += 1;
    store.put(&segment_key(&prefix, name, id), &encoded)?;
    let meta = build_segment_meta(id, st.journal_seq, &encoded, &out)?;
    ps.segments.push(meta);
  }
  ps.watermark = st.journal_seq;
  publish_manifest(store, st)?;
  if eager {
    for seg in &old {
      let _ = store.delete(&segment_key(&prefix, name, seg.id));
    }
  }
  st.compactions_completed += 1;
  Ok(true)
}

/// Compact every partition that reached the configured segment limit.
fn maybe_compact_all(store: &dyn Store, st: &mut EngineState) -> Result<()> {
  let names: Vec<String> = st
    .partitions
    .values()
    .filter(|p| !p.dropped && p.segments.len() >= st.cfg.max_segments_before_compact)
    .map(|p| p.name.clone())
    .collect();
  for name in names {
    compact_partition_locked(store, st, &name)?;
  }
  Ok(())
}

/// Delete journal objects whose seq is at or below every partition watermark
/// (they are folded into segments or were deliberately discarded).
fn gc_journal(store: &dyn Store, st: &mut EngineState) {
  let min_wm = st
    .partitions
    .values()
    .map(|p| p.watermark)
    .min()
    .unwrap_or(0);
  if min_wm == 0 {
    return;
  }
  let doomed: Vec<u64> = st.journal_seqs.range(..=min_wm).copied().collect();
  for s in doomed {
    let _ = store.delete(&journal_key(&st.cfg.prefix, s));
    st.journal_seqs.remove(&s);
  }
}

/// Startup GC: when eager deletion is enabled, delete segment objects not
/// referenced by the current manifest; always delete manifest snapshots
/// superseded by `current` (they are cheap metadata, not the high-volume
/// segment DELETE path).
fn gc_objects_at_open(store: &dyn Store, st: &mut EngineState) -> Result<()> {
  let prefix = st.cfg.prefix.clone();
  if st.cfg.eager_object_delete {
    let seg_root = segment_root(&prefix);
    let seg_prefix = format!("{prefix}/seg/");
    let live: BTreeMap<String, Vec<u64>> = st
      .partitions
      .iter()
      .filter(|(_, p)| !p.dropped)
      .map(|(name, p)| (name.clone(), p.segments.iter().map(|s| s.id).collect()))
      .collect();
    for key in store.list(&seg_root)? {
      let rest = match key.strip_prefix(&seg_prefix) {
        Some(r) => r,
        None => continue,
      };
      let Some((part, id_s)) = rest.rsplit_once('/') else {
        continue;
      };
      let Ok(id) = id_s.parse::<u64>() else {
        continue;
      };
      let referenced = live.get(part).map(|ids| ids.contains(&id)).unwrap_or(false);
      if !referenced {
        let _ = store.delete(&key);
      }
    }
  }
  let cur = st.manifest_seq;
  for key in store.list(&manifest_prefix(&prefix))? {
    if let Some(seq) = parse_tail_seq(&key)
      && seq != cur
    {
      let _ = store.delete(&key);
    }
  }
  Ok(())
}

/// Flush the pending group-commit buffer into one journal object covering
/// groups `pending_lo..=journal_seq`.
fn flush_journal_pending(store: &dyn Store, st: &mut EngineState) -> Result<()> {
  if st.pending.is_empty() {
    return Ok(());
  }
  let end = st.journal_seq;
  store.put(&journal_key(&st.cfg.prefix, end), &st.pending)?;
  st.journal_seqs.insert(end);
  st.pending.clear();
  st.pending_lo = 0;
  Ok(())
}

/// Background flusher for windowed group-commit mode: every `ms` it turns the
/// pending buffer into one journal object (and garbage-collects folded ones).
fn spawn_journal_flusher(inner: Arc<Inner>) {
  let Some(ms) = inner.state.read().cfg.journal_window_ms else {
    return;
  };
  thread::Builder::new()
    .name("objectlsm-journal".into())
    .spawn(move || {
      loop {
        if inner.journal_stop.load(Ordering::SeqCst) {
          return;
        }
        thread::sleep(Duration::from_millis(ms));
        if inner.journal_stop.load(Ordering::SeqCst) {
          return;
        }
        {
          let mut st = inner.state.write();
          if !st.pending.is_empty() && flush_journal_pending(&*inner.store, &mut st).is_ok() {
            gc_journal(&*inner.store, &mut st);
          }
        }
      }
    })
    .ok();
}

impl Inner {
  /// Atomically commit an op group (journal PUT first, then apply).
  pub(crate) fn commit_ops(&self, ops: Vec<Op>) -> Result<()> {
    if ops.is_empty() {
      return Ok(());
    }
    let mut st = self.state.write();
    let seq = st.journal_seq + 1;
    st.journal_seq = seq;
    let group = Group { seq, ops };
    let bytes = encode_group(&group)?;
    // Buffer the encoded group. Strict mode (no window) flushes it as its own
    // durable object before applying, preserving per-commit durability.
    // Windowed mode batches queued groups into one object on a timer.
    if st.pending.is_empty() {
      st.pending_lo = seq;
    }
    st.pending.extend_from_slice(&bytes);
    if st.cfg.journal_window_ms.is_none()
      || st.pending.len() as u64 >= st.cfg.journal_max_buffer_bytes
    {
      flush_journal_pending(&*self.store, &mut st)?;
    }
    apply_group(&mut st, &group)?;
    let over: Vec<String> = st
      .partitions
      .values()
      .filter(|p| !p.dropped && p.mem_bytes > st.cfg.max_memtable_bytes)
      .map(|p| p.name.clone())
      .collect();
    for name in over {
      flush_partition(&*self.store, &mut st, &name)?;
    }
    maybe_compact_all(&*self.store, &mut st)?;
    gc_journal(&*self.store, &mut st);
    Ok(())
  }

  /// Ensure a partition exists and is not marked dropped (re-create clears the
  /// dropped flag, keeping its watermark to avoid stale journal replays).
  pub(crate) fn touch_partition(&self, name: &str) -> Result<()> {
    let mut st = self.state.write();
    let need_publish = match st.partitions.get_mut(name) {
      Some(ps) => {
        if ps.dropped {
          ps.dropped = false;
          true
        } else {
          false
        }
      }
      None => {
        st.partitions
          .insert(name.to_string(), PartitionState::new(name));
        false
      }
    };
    if need_publish {
      publish_manifest(&*self.store, &mut st)?;
    }
    Ok(())
  }

  /// Load (and cache) the block index of a segment via tail + index Range GETs.
  pub(crate) fn load_index(
    &self,
    prefix: &str,
    part: &str,
    seg: &SegmentMeta,
  ) -> Result<Arc<SegmentIndex>> {
    if let Some(idx) = self.index_cache.get(seg.id) {
      return Ok(idx);
    }
    if let Some(idx) = &seg.index {
      let idx = Arc::new(idx.clone());
      self.index_cache.insert(seg.id, idx.clone());
      return Ok(idx);
    }
    let key = segment_key(prefix, part, seg.id);
    let tail_raw = self
      .store
      .get_range(
        &key,
        seg.bytes.saturating_sub(TAIL_LEN as u64),
        TAIL_LEN as u64,
      )?
      .ok_or_else(|| Error::Corrupt(format!("segment {} tail missing", seg.id)))?;
    let tail = parse_tail(&tail_raw)?;
    let idx_raw = self
      .store
      .get_range(&key, tail.index_offset as u64, tail.index_len as u64)?
      .ok_or_else(|| Error::Corrupt(format!("segment {} index missing", seg.id)))?;
    let index = Arc::new(decode_index(&idx_raw)?);
    self.index_cache.insert(seg.id, index.clone());
    Ok(index)
  }

  /// Load (and cache) one decoded block of a segment via a Range GET.
  pub(crate) fn load_block(
    &self,
    prefix: &str,
    part: &str,
    seg: &SegmentMeta,
    block: &BlockMeta,
  ) -> Result<Arc<SegmentEntries>> {
    if let Some(entries) = self.block_cache.get(seg.id, block.offset) {
      return Ok(entries);
    }
    let key = segment_key(prefix, part, seg.id);
    let raw = self
      .store
      .get_range(&key, block.offset as u64, block.len as u64)?
      .ok_or_else(|| {
        Error::Corrupt(format!(
          "segment {} block @{} missing",
          seg.id, block.offset
        ))
      })?;
    if raw.len() < BLOCK_HEADER_LEN {
      return Err(Error::Corrupt("block shorter than header".into()));
    }
    let entries = Arc::new(decode_block(&raw)?);
    self
      .block_cache
      .insert(seg.id, block.offset, entries.clone(), block.len as u64);
    Ok(entries)
  }

  /// Point lookup: memtable, then segments newest -> oldest using the block
  /// index to fetch only the candidate block.
  pub(crate) fn lookup(&self, name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let st = self.state.read();
    let prefix = st.cfg.prefix.clone();
    let Some(ps) = st.partitions.get(name) else {
      return Ok(None);
    };
    if let Some(e) = ps.mem.get(key) {
      return Ok(match e {
        MemEntry::Value(v) => Some(v.clone()),
        MemEntry::Tombstone => None,
      });
    }
    for seg in ps.segments.iter().rev() {
      if seg.first.as_slice() > key || seg.last.as_slice() < key {
        continue;
      }
      let index = self.load_index(&prefix, name, seg)?;
      let Some(bi) = find_block(&index, key) else {
        continue;
      };
      let block = &index.blocks[bi];
      let entries = self.load_block(&prefix, name, seg, block)?;
      let idx = entries.partition_point(|(k, _)| k.as_slice() < key);
      if let Some((k, v)) = entries.get(idx)
        && k.as_slice() == key
      {
        // A tombstone here shadows any older segment value.
        return Ok(v.clone());
      }
    }
    Ok(None)
  }

  pub(crate) fn clear_partition(&self, name: &str) -> Result<()> {
    let mut st = self.state.write();
    let prefix = st.cfg.prefix.clone();
    let eager = st.cfg.eager_object_delete;
    let tail_seq = st.journal_seq;
    {
      let ps = match st.partitions.get_mut(name) {
        Some(ps) => ps,
        None => return Ok(()),
      };
      for seg in ps.segments.drain(..) {
        if eager {
          let _ = self.store.delete(&segment_key(&prefix, name, seg.id));
        }
        self.index_cache.remove(seg.id);
      }
      ps.mem.clear();
      ps.mem_bytes = 0;
      ps.watermark = tail_seq;
      ps.dropped = false;
    }
    publish_manifest(&*self.store, &mut st)?;
    gc_journal(&*self.store, &mut st);
    Ok(())
  }

  pub(crate) fn rm_partition(&self, name: &str) -> Result<()> {
    let mut st = self.state.write();
    let prefix = st.cfg.prefix.clone();
    let eager = st.cfg.eager_object_delete;
    let tail_seq = st.journal_seq;
    {
      let ps = match st.partitions.get_mut(name) {
        Some(ps) => ps,
        None => return Ok(()),
      };
      for seg in ps.segments.drain(..) {
        if eager {
          let _ = self.store.delete(&segment_key(&prefix, name, seg.id));
        }
        self.index_cache.remove(seg.id);
      }
      ps.mem.clear();
      ps.mem_bytes = 0;
      ps.watermark = tail_seq;
      ps.dropped = true;
    }
    publish_manifest(&*self.store, &mut st)?;
    gc_journal(&*self.store, &mut st);
    Ok(())
  }

  /// Flush + merge-compact a single partition.
  pub(crate) fn compact_partition(&self, name: &str) -> Result<()> {
    let mut st = self.state.write();
    compact_partition_locked(&*self.store, &mut st, name)?;
    gc_journal(&*self.store, &mut st);
    Ok(())
  }

  /// Flush + merge-compact every non-dropped partition.
  pub(crate) fn compact_all(&self) -> Result<()> {
    let mut st = self.state.write();
    let names: Vec<String> = st
      .partitions
      .values()
      .filter(|p| !p.dropped && (!p.mem.is_empty() || p.segments.len() > 1))
      .map(|p| p.name.clone())
      .collect();
    for name in names {
      compact_partition_locked(&*self.store, &mut st, &name)?;
    }
    gc_journal(&*self.store, &mut st);
    Ok(())
  }
}

impl Engine for ObjectLsm {
  type Error = Error;
  type Partition = ObjectLsmPartition;
  type Batch = ObjectLsmBatch;

  fn partition(&self, name: &str) -> Result<Self::Partition> {
    self.inner.touch_partition(name)?;
    Ok(ObjectLsmPartition {
      name: name.to_string(),
      inner: self.inner.clone(),
    })
  }

  fn partition_exists(&self, name: &str) -> bool {
    self
      .inner
      .state
      .read()
      .partitions
      .get(name)
      .map(|p| !p.dropped)
      .unwrap_or(false)
  }

  fn list_partitions(&self) -> Result<Vec<String>> {
    Ok(
      self
        .inner
        .state
        .read()
        .partitions
        .iter()
        .filter(|(_, p)| !p.dropped)
        .map(|(n, _)| n.clone())
        .collect(),
    )
  }

  fn rm_partition(&self, partition: &Self::Partition) -> Result<()> {
    self.inner.rm_partition(&partition.name)
  }

  fn write_buffer_size(&self) -> u64 {
    self
      .inner
      .state
      .read()
      .partitions
      .values()
      .map(|p| p.mem_bytes)
      .sum()
  }

  fn cache_size(&self) -> u64 {
    self.inner.block_cache.used()
  }

  fn cache_capacity(&self) -> u64 {
    self.inner.block_cache.capacity()
  }

  fn compactions_completed(&self) -> usize {
    self.inner.state.read().compactions_completed as usize
  }

  fn batch(&self) -> Self::Batch {
    ObjectLsmBatch::new(self.inner.clone())
  }

  fn batch_with_capacity(&self, capacity: usize) -> Self::Batch {
    ObjectLsmBatch::with_capacity(self.inner.clone(), capacity)
  }

  fn persist(&self) -> Result<()> {
    // Strict mode: every committed group is already a durable journal object.
    // Windowed mode: force a synchronous flush so this call only returns once
    // everything acknowledged so far is durable.
    if self.inner.state.read().cfg.journal_window_ms.is_some() {
      let mut st = self.inner.state.write();
      flush_journal_pending(&*self.inner.store, &mut st)?;
      gc_journal(&*self.inner.store, &mut st);
    }
    Ok(())
  }

  fn disk_space(&self) -> Result<u64> {
    let st = self.inner.state.read();
    Ok(
      st.partitions
        .values()
        .map(|p| p.segments.iter().map(|s| s.bytes).sum::<u64>() + p.mem_bytes)
        .sum(),
    )
  }

  fn compact(&self) -> Result<()> {
    self.inner.compact_all()
  }
}
