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

use parking_lot::{Mutex, RwLock};
use wedb_embed_engine::Engine;

use crate::{
  batch::ObjectLsmBatch,
  cache::{BlockCache, IndexCache},
  config::Config,
  error::{Error, Result},
  journal::{Group, Op, decode_group_stream, encode_group},
  keys::{
    current_key, journal_key_epoch, journal_prefix_epoch, manifest_key, manifest_prefix,
    parse_tail_seq, segment_key, segment_root,
  },
  lease::{Lease, LeaseOptions},
  manifest::Manifest,
  partition::ObjectLsmPartition,
  segment::{
    BLOCK_HEADER_LEN, BlockMeta, SegmentEntries, SegmentIndex, SegmentMeta, TAIL_LEN,
    build_segment_meta, decode_block, decode_index, encode_segment, find_block, parse_tail,
  },
  state::{
    EngineState, MemEntry, PartitionLock, PartitionState, PartitionTable, PendingFlush, ReaderGate,
  },
  store::Store,
};

/// Shared internals behind [`ObjectLsm`].
pub struct Inner {
  pub store: Arc<dyn Store>,
  pub state: RwLock<EngineState>,
  /// Serializes journal object PUTs so they never race while the global lock
  /// is free for partition/flush work (strict mode commits PUT outside `state`).
  pub journal_lock: Mutex<()>,
  /// Gates segment-object deletion against in-flight readers.
  pub readers: ReaderGate,
  /// Per-partition data-plane lock table (independent of `state`'s lock).
  pub partitions: PartitionTable,
  pub block_cache: BlockCache,
  pub index_cache: IndexCache,
  /// Stops the background group-commit journal flusher thread.
  pub journal_stop: Arc<AtomicBool>,
  /// Shared lease-lost flag used for best-effort write fencing; the lease
  /// object itself is owned by the `ObjectLsm` handle (not by `Inner`), so
  /// partition handles cannot prolong its lifetime.
  pub lease_lost: parking_lot::Mutex<Option<Arc<AtomicBool>>>,
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
  /// Writer lease (owned by this handle); released when the last handle drops.
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
    {
      let mut st = engine.inner.state.write();
      st.fence_epoch = lease.epoch();
    }
    *engine.inner.lease_lost.lock() = Some(lease.lost_flag());
    engine.lease = Some(Arc::new(lease));
    Ok(engine)
  }

  fn build(store: Arc<dyn Store>, cfg: Config) -> Result<Self> {
    let block_cache = BlockCache::new(cfg.cache_capacity);
    let index_cache = IndexCache::default();
    let partitions = PartitionTable::default();
    let readers = ReaderGate::default();
    let state = recover(&*store, &cfg, &index_cache, &partitions, &readers)?;
    let journal_stop = Arc::new(AtomicBool::new(false));
    let inner = Arc::new(Inner {
      store,
      state: RwLock::new(state),
      journal_lock: Mutex::new(()),
      readers,
      partitions,
      block_cache,
      index_cache,
      journal_stop: journal_stop.clone(),
      lease_lost: parking_lot::Mutex::new(None),
    });
    if cfg.journal_window_ms.is_some() {
      spawn_journal_flusher(inner.clone());
    }
    if cfg.background_flush {
      spawn_background_flusher(inner.clone());
    }
    Ok(Self { inner, lease: None })
  }
}

impl Drop for ObjectLsm {
  fn drop(&mut self) {
    if let Some(lease) = self.lease.take() {
      drop(lease);
    }
  }
}

impl Drop for Inner {
  fn drop(&mut self) {
    // Graceful shutdown: flush buffered group-commit journal before stopping
    // the background flusher, so a clean close does not lose acked writes.
    if self.state.read().cfg.journal_window_ms.is_some() {
      let mut st = self.state.write();
      let _ = flush_journal_pending(&*self.store, &mut st);
    }
    self.journal_stop.store(true, Ordering::SeqCst);
  }
}

fn sync_partition_meta(st: &mut EngineState, ps: &PartitionState) {
  if let Some(pm) = st.partitions.get_mut(&ps.name) {
    *pm = ps.meta.clone();
  }
}

/// Rebuild in-memory state from the durable manifest + journal tail, then run
/// startup compaction/GC.
fn recover(
  store: &dyn Store,
  cfg: &Config,
  index_cache: &IndexCache,
  partitions: &PartitionTable,
  readers: &ReaderGate,
) -> Result<EngineState> {
  let mut st = EngineState::new(cfg.clone());
  let prefix = &cfg.prefix;

  if let Some(cur) = store.get(&current_key(prefix))? {
    let text =
      std::str::from_utf8(&cur).map_err(|e| Error::Corrupt(format!("current not utf-8: {e}")))?;
    let (seq_text, epoch) = match text.split_once('\n') {
      Some((seq, epoch)) => (
        seq,
        Some(
          epoch
            .trim()
            .parse::<u128>()
            .map_err(|e| Error::Corrupt(format!("current epoch: {e}")))?,
        ),
      ),
      None => (text, None),
    };
    let mseq: u64 = seq_text
      .trim()
      .parse()
      .map_err(|e| Error::Corrupt(format!("current not a seq: {e}")))?;
    st.fence_epoch = epoch.unwrap_or(0);
    st.current_bytes = Some(cur.clone());
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
      let lock = partitions.create(&name);
      lock.write().meta = pm.clone();
      st.partitions.insert(name, pm);
    }
  }

  // Replay every journal group newer than its partition watermark.
  let list = store.list(&journal_prefix_epoch(prefix, st.fence_epoch))?;
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
    let Some(bytes) = store.get(&journal_key_epoch(prefix, s, st.fence_epoch))? else {
      continue;
    };
    st.journal_sizes.insert(s, bytes.len() as u64);
    for group in decode_group_stream(&bytes)? {
      if group.epoch != st.fence_epoch {
        continue;
      }
      apply_group_recover(&mut st, &group, partitions)?;
    }
  }

  // Flush replayed memtables that already exceeded the budget.
  let over: Vec<String> = partitions
    .snapshot()
    .iter()
    .filter_map(|lock| {
      let ps = lock.read();
      (!ps.meta.dropped && ps.mem_bytes > cfg.max_memtable_bytes).then(|| ps.name.clone())
    })
    .collect();
  for name in over {
    let lock = partitions.get(&name).expect("partition present");
    let mut ps = lock.write();
    flush_partition(store, &mut st, &mut ps)?;
  }
  maybe_compact_all_state(store, &mut st, partitions, readers)?;
  gc_journal(store, &mut st);
  gc_objects_at_open(store, &mut st)?;
  Ok(st)
}

/// Apply a committed group during recovery. Unlike the online commit path,
/// this is single-threaded startup state construction.
fn apply_group_recover(
  st: &mut EngineState,
  group: &Group,
  partitions: &PartitionTable,
) -> Result<()> {
  for op in &group.ops {
    let wm = st
      .partitions
      .get(&op.part)
      .map(|pm| pm.watermark)
      .unwrap_or(0);
    if group.seq <= wm {
      continue;
    }
    let lock = partitions.create(&op.part);
    let mut ps = lock.write();
    ps.apply(&op.key, op.value.as_deref());
    st.ensure_meta(&op.part);
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
    fence_epoch: st.fence_epoch,
    partitions: BTreeMap::new(),
  };
  for (name, pm) in &st.partitions {
    man.partitions.insert(name.clone(), pm.clone());
  }
  st.manifest_seq = man.seq;
  let bytes = man.encode()?;
  st.manifest_bytes = bytes.len() as u64;
  store.put(&manifest_key(&st.cfg.prefix, man.seq), &bytes)?;

  let new_current = if st.fence_epoch != 0 {
    format!("{}\n{}", man.seq, st.fence_epoch).into_bytes()
  } else {
    man.seq.to_string().into_bytes()
  };
  if st.fence_epoch != 0 {
    let ok = match &st.current_bytes {
      Some(expected) => {
        store.put_if_matches(&current_key(&st.cfg.prefix), expected, &new_current)?
      }
      None => store.create(&current_key(&st.cfg.prefix), &new_current)?,
    };
    if !ok {
      return Err(Error::store("fenced: manifest current changed"));
    }
  } else {
    store.put(&current_key(&st.cfg.prefix), &new_current)?;
  }
  st.current_bytes = Some(new_current);
  Ok(())
}

/// Flush one partition's memtable into an immutable block-indexed segment and
/// advance its watermark. The caller must hold both the partition write lock
/// (via `ps`) and the global write lock (via `st`).
fn flush_partition(store: &dyn Store, st: &mut EngineState, ps: &mut PartitionState) -> Result<()> {
  if ps.meta.dropped || ps.mem.is_empty() {
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
  store.put(&segment_key(&st.cfg.prefix, &ps.name, id), &encoded)?;
  let meta = build_segment_meta(
    id,
    st.journal_seq,
    &encoded,
    &entries,
    st.cfg.manifest_embed_index,
  )?;
  ps.meta.segments.push(meta);
  ps.meta.watermark = st.journal_seq;
  ps.mem.clear();
  ps.mem_bytes = 0;
  sync_partition_meta(st, ps);
  publish_manifest(store, st)
}

/// Detach a full memtable snapshot for background upload.
fn take_partition_flush(st: &mut EngineState, ps: &mut PartitionState) -> Option<PendingFlush> {
  if ps.meta.dropped
    || ps.mem.is_empty()
    || st.pending_flushes.contains_key(&ps.name)
    || ps.mem_bytes < st.cfg.max_memtable_bytes
  {
    return None;
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
  let encoded = encode_segment(&entries, st.cfg.block_size as usize).ok()?;
  let segment_id = st.next_segment_id;
  st.next_segment_id += 1;
  let pending = PendingFlush {
    partition: ps.name.clone(),
    watermark: ps.meta.watermark.max(st.journal_seq),
    segment_id,
    encoded,
    entries,
  };
  ps.mem.clear();
  ps.mem_bytes = 0;
  st.pending_flushes
    .insert(pending.partition.clone(), pending.clone());
  Some(pending)
}

/// Publish a completed background segment upload into the partition metadata.
fn finish_partition_flush(inner: &Inner, part: &str) -> Result<()> {
  let Some(active) = ({
    let mut st = inner.state.write();
    st.pending_flushes.remove(part)
  }) else {
    return Ok(());
  };
  let (key, embed_index) = {
    let st = inner.state.read();
    (
      segment_key(&st.cfg.prefix, part, active.segment_id),
      st.cfg.manifest_embed_index,
    )
  };
  inner.store.put(&key, &active.encoded)?;
  let meta = build_segment_meta(
    active.segment_id,
    active.watermark,
    &active.encoded,
    &active.entries,
    embed_index,
  )?;
  let lock = inner
    .partitions
    .get(part)
    .ok_or_else(|| Error::store("flush partition disappeared"))?;
  let mut ps = lock.write();
  let mut st = inner.state.write();
  ps.meta.segments.push(meta);
  ps.meta.watermark = ps.meta.watermark.max(active.watermark);
  sync_partition_meta(&mut st, &ps);
  publish_manifest(&*inner.store, &mut st)?;
  gc_journal(&*inner.store, &mut st);
  Ok(())
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

/// Merge every segment of a partition into one fresh segment. The caller must
/// hold both the partition write lock and the global write lock.
fn compact_partition_locked(
  store: &dyn Store,
  st: &mut EngineState,
  ps: &mut PartitionState,
) -> Result<Option<Vec<SegmentMeta>>> {
  flush_partition(store, st, ps)?;
  let prefix = st.cfg.prefix.clone();
  if !(!ps.meta.dropped && ps.meta.segments.len() > 1) {
    return Ok(None);
  }

  // 1. fold all segments newest -> oldest into a single decision map.
  let mut map: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
  for seg in ps.meta.segments.iter().rev() {
    let entries = read_segment_entries(store, &prefix, &ps.name, seg)?;
    for (k, v) in entries {
      map.entry(k).or_insert(v);
    }
  }

  // 2. drop tombstones: nothing older than the merged run survives.
  let mut out: SegmentEntries = Vec::with_capacity(map.len());
  for (k, v) in map {
    if let Some(val) = v {
      out.push((k, Some(val)));
    }
  }

  // 3. upload the merged result first (failure keeps the old run intact),
  //    then atomically swap the segment list and publish the manifest. Old
  //    objects are deleted by the caller after releasing the partition lock.
  let new_meta = if out.is_empty() {
    None
  } else {
    let encoded = encode_segment(&out, st.cfg.block_size as usize)?;
    let id = st.next_segment_id;
    st.next_segment_id += 1;
    store.put(&segment_key(&prefix, &ps.name, id), &encoded)?;
    Some(build_segment_meta(
      id,
      st.journal_seq,
      &encoded,
      &out,
      st.cfg.manifest_embed_index,
    )?)
  };

  let new_segments = new_meta.map(|m| vec![m]).unwrap_or_default();
  let old = std::mem::replace(&mut ps.meta.segments, new_segments);
  ps.meta.watermark = st.journal_seq;
  sync_partition_meta(st, ps);
  publish_manifest(store, st)?;
  st.compactions_completed += 1;
  Ok(Some(old))
}

/// Compact every partition that reached the configured segment limit.
fn maybe_compact_all_state(
  store: &dyn Store,
  st: &mut EngineState,
  partitions: &PartitionTable,
  _readers: &ReaderGate,
) -> Result<()> {
  let limit = st.cfg.max_segments_before_compact;
  for name in partitions.names() {
    let Some(lock) = partitions.get(&name) else {
      continue;
    };
    let mut ps = lock.write();
    if !ps.meta.dropped
      && ps.meta.segments.len() >= limit
      && st.cfg.eager_object_delete
      && let Some(old) = compact_partition_locked(store, st, &mut ps)?
    {
      for seg in old {
        let _ = store.delete(&segment_key(&st.cfg.prefix, &name, seg.id));
      }
    }
  }
  Ok(())
}

/// Delete journal objects whose seq is at or below every partition watermark
/// (they are folded into segments or were deliberately discarded).
fn gc_journal(store: &dyn Store, st: &mut EngineState) {
  let min_wm = st
    .partitions
    .values()
    .map(|pm| pm.watermark)
    .min()
    .unwrap_or(0);
  if min_wm == 0 {
    return;
  }
  let doomed: Vec<u64> = st.journal_seqs.range(..=min_wm).copied().collect();
  for s in doomed {
    let _ = store.delete(&journal_key_epoch(&st.cfg.prefix, s, st.fence_epoch));
    st.journal_seqs.remove(&s);
    st.journal_sizes.remove(&s);
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
      .filter(|(_, pm)| !pm.dropped)
      .map(|(name, pm)| (name.clone(), pm.segments.iter().map(|s| s.id).collect()))
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

/// Detach the pending group-commit buffer for an out-of-lock upload.
///
/// The caller holds the global state lock only while taking the buffer and
/// reserving its end sequence. The remote object PUT happens afterwards
/// without blocking commits; `finish_flushed_journal` records the object
/// after the upload succeeds.
fn take_journal_pending(st: &mut EngineState) -> Option<(String, Vec<u8>)> {
  if st.pending.is_empty() || st.journal_flushing {
    return None;
  }
  let end = st.journal_seq;
  st.journal_flushing = true;
  Some((
    journal_key_epoch(&st.cfg.prefix, end, st.fence_epoch),
    std::mem::take(&mut st.pending),
  ))
}

/// Record a successfully uploaded detached journal object.
fn finish_flushed_journal(st: &mut EngineState, key: &str, bytes_len: u64) {
  st.journal_flushing = false;
  if let Some(end) = parse_tail_seq(key) {
    st.journal_seqs.insert(end);
    st.journal_sizes.insert(end, bytes_len);
  }
  st.pending_lo = 0;
}

/// Abort a detached upload and restore its buffer for a later flush.
fn abort_journal_flush(st: &mut EngineState, buffer: Vec<u8>) {
  st.journal_flushing = false;
  if st.pending.is_empty() {
    st.pending = buffer;
  }
}

/// Flush the pending group-commit buffer into one journal object covering
/// groups `pending_lo..=journal_seq`.
fn flush_journal_pending(store: &dyn Store, st: &mut EngineState) -> Result<()> {
  let Some((key, buffer)) = take_journal_pending(st) else {
    return Ok(());
  };
  let bytes = buffer.len() as u64;
  store.put(&key, &buffer)?;
  finish_flushed_journal(st, &key, bytes);
  Ok(())
}

/// Background flusher for windowed group-commit mode: every `ms` it turns the
/// pending buffer into one journal object (and garbage-collects folded ones).
fn spawn_journal_flusher(inner: Arc<Inner>) {
  let Some(ms) = inner.state.read().cfg.journal_window_ms else {
    return;
  };
  let weak = Arc::downgrade(&inner);
  let stop = inner.journal_stop.clone();
  thread::Builder::new()
    .name("objectlsm-journal".into())
    .spawn(move || {
      loop {
        if stop.load(Ordering::SeqCst) {
          return;
        }
        thread::sleep(Duration::from_millis(ms));
        if stop.load(Ordering::SeqCst) {
          return;
        }
        let Some(inner) = weak.upgrade() else {
          return;
        };
        if let Some((key, buffer)) = {
          let mut st = inner.state.write();
          take_journal_pending(&mut st)
        } {
          match inner.store.put(&key, &buffer) {
            Ok(()) => {
              let bytes = buffer.len() as u64;
              let mut st = inner.state.write();
              finish_flushed_journal(&mut st, &key, bytes);
              gc_journal(&*inner.store, &mut st);
            }
            Err(_) => {
              let mut st = inner.state.write();
              abort_journal_flush(&mut st, buffer);
            }
          }
        }
      }
    })
    .ok();
}

fn spawn_background_flusher(inner: Arc<Inner>) {
  let weak = Arc::downgrade(&inner);
  thread::Builder::new()
    .name("objectlsm-flush".into())
    .spawn(move || {
      loop {
        thread::sleep(Duration::from_millis(10));
        let Some(inner) = weak.upgrade() else {
          return;
        };
        if !inner.state.read().cfg.background_flush {
          continue;
        }
        let parts: Vec<String> = {
          let st = inner.state.read();
          st.pending_flushes.keys().cloned().collect()
        };
        for part in parts {
          let _ = finish_partition_flush(&inner, &part);
        }
      }
    })
    .ok();
}

impl Inner {
  /// Best-effort fencing: reject mutations once the held writer lease has
  /// been marked lost (e.g. heartbeat renewal failed after expiry).
  fn ensure_writer(&self) -> Result<()> {
    let lost = self
      .lease_lost
      .lock()
      .as_ref()
      .map(|flag| flag.load(Ordering::SeqCst))
      .unwrap_or(false);
    if lost {
      return Err(Error::store("writer lease lost"));
    }
    Ok(())
  }

  pub(crate) fn partition_lock(&self, name: &str) -> Option<PartitionLock> {
    self.partitions.get(name)
  }

  fn ensure_partitions(&self, names: &[String]) {
    for name in names {
      self.partitions.create(name);
    }
    let mut st = self.state.write();
    for name in names {
      st.ensure_meta(name);
    }
  }

  fn partition_locks(&self, names: &[String]) -> BTreeMap<String, PartitionLock> {
    names
      .iter()
      .filter_map(|name| self.partitions.get(name).map(|lock| (name.clone(), lock)))
      .collect()
  }

  /// Atomically commit an op group (journal PUT first, then apply).
  ///
  /// Locks for every involved partition are acquired in sorted-name order,
  /// then the global metadata lock is taken only for seq allocation / journal
  /// buffering / flush-compact-GC. Different partitions therefore commit
  /// concurrently and only briefly share the global lock.
  pub(crate) fn commit_ops(&self, ops: Vec<Op>) -> Result<()> {
    if ops.is_empty() {
      return Ok(());
    }
    self.ensure_writer()?;
    let mut names: Vec<String> = ops.iter().map(|op| op.part.clone()).collect();
    names.sort();
    names.dedup();

    self.ensure_partitions(&names);
    let locks = self.partition_locks(&names);
    let mut guards = BTreeMap::new();
    for name in &names {
      guards.insert(
        name.clone(),
        locks.get(name).expect("partition lock").write(),
      );
    }

    // Allocate the journal seq and (in windowed mode) append to the pending
    // buffer under the global lock. In strict mode the group is serialized
    // here but its object PUT happens afterwards outside the global lock, so a
    // network write no longer blocks unrelated partition reads/writes.
    let (group, strict_put) = {
      let mut st = self.state.write();
      let seq = st.journal_seq + 1;
      st.journal_seq = seq;
      let group = Group {
        seq,
        epoch: st.fence_epoch,
        ops,
      };
      let bytes = encode_group(&group)?;
      let strict_put = if st.cfg.journal_window_ms.is_none() {
        Some((
          journal_key_epoch(&st.cfg.prefix, seq, st.fence_epoch),
          bytes,
        ))
      } else {
        if st.pending.is_empty() {
          st.pending_lo = seq;
        }
        st.pending.extend_from_slice(&bytes);
        if st.pending.len() as u64 >= st.cfg.journal_max_buffer_bytes {
          flush_journal_pending(&*self.store, &mut st)?;
        }
        None
      };
      (group, strict_put)
    };

    if let Some((jkey, bytes)) = strict_put {
      // Durability before visibility: write the journal object first, then
      // apply. The journal lock only serializes journal PUTs.
      {
        let _guard = self.journal_lock.lock();
        self.store.put(&jkey, &bytes)?;
      }
      let mut st = self.state.write();
      st.journal_seqs.insert(group.seq);
      st.journal_sizes.insert(group.seq, bytes.len() as u64);
    }

    for op in &group.ops {
      let ps = guards.get_mut(&op.part).expect("partition guard");
      if group.seq <= ps.meta.watermark {
        continue;
      }
      ps.apply(&op.key, op.value.as_deref());
    }

    let mut deleted: Vec<(String, Vec<SegmentMeta>)> = Vec::new();
    {
      let mut st = self.state.write();
      let mem_limit = st.cfg.max_memtable_bytes;
      let over: Vec<String> = names
        .iter()
        .filter(|name| {
          guards
            .get(*name)
            .map(|ps| !ps.meta.dropped && ps.mem_bytes > mem_limit)
            .unwrap_or(false)
        })
        .cloned()
        .collect();
      let background_flush = st.cfg.background_flush;
      for name in over {
        if background_flush {
          let _ = take_partition_flush(&mut st, guards.get_mut(&name).expect("guard"));
        } else {
          // Maintenance after the durability point must not turn an already
          // committed batch into an error: leave the memtable for a later flush.
          let _ = flush_partition(&*self.store, &mut st, guards.get_mut(&name).expect("guard"));
        }
      }

      let seg_limit = st.cfg.max_segments_before_compact;
      for name in &names {
        let ps = guards.get_mut(name).expect("guard");
        if !ps.meta.dropped
          && ps.meta.segments.len() >= seg_limit
          && let Ok(Some(old)) = compact_partition_locked(&*self.store, &mut st, ps)
        {
          deleted.push((name.clone(), old));
        }
      }
      gc_journal(&*self.store, &mut st);
    }
    drop(guards);
    for (name, old) in deleted {
      self.delete_old_segments(&name, old);
    }
    Ok(())
  }

  /// Ensure a partition exists and is not marked dropped (re-create clears the
  /// dropped flag, keeping its watermark to avoid stale journal replays).
  pub(crate) fn touch_partition(&self, name: &str) -> Result<()> {
    self.ensure_writer()?;
    let lock = self.partitions.create(name);
    let mut ps = lock.write();
    if ps.meta.dropped {
      ps.meta.dropped = false;
      let mut st = self.state.write();
      sync_partition_meta(&mut st, &ps);
      publish_manifest(&*self.store, &mut st)?;
    } else {
      let mut st = self.state.write();
      st.ensure_meta(name);
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
    // Reader token prevents segment-object deletion while this lookup runs.
    let _gate = self.readers.enter();
    let prefix = self.state.read().cfg.prefix.clone();
    let Some(lock) = self.partitions.get(name) else {
      return Ok(None);
    };
    let segments = {
      let ps = lock.read();
      if let Some(e) = ps.mem.get(key) {
        return Ok(match e {
          MemEntry::Value(v) => Some(v.clone()),
          MemEntry::Tombstone => None,
        });
      }
      ps.meta.segments.clone()
    };
    if let Some(pending) = self.state.read().pending_flushes.get(name)
      && let Some(e) = pending.entries.iter().find(|(k, _)| k.as_slice() == key)
    {
      return Ok(e.1.clone());
    }

    for seg in segments.iter().rev() {
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
    self.ensure_writer()?;
    // Gate deletes against readers (before taking the partition lock), and
    // publish the cleared manifest before deleting old objects: a crash only
    // leaves orphan objects, never a manifest pointing at deleted segments.
    let _del = self.readers.exclusive();
    let Some(lock) = self.partition_lock(name) else {
      return Ok(());
    };
    let mut ps = lock.write();
    let mut st = self.state.write();
    let prefix = st.cfg.prefix.clone();
    let eager = st.cfg.eager_object_delete;
    let tail_seq = st.journal_seq;
    let old = std::mem::take(&mut ps.meta.segments);
    ps.mem.clear();
    ps.mem_bytes = 0;
    ps.meta.watermark = tail_seq;
    st.pending_flushes.remove(name);
    ps.meta.dropped = false;
    sync_partition_meta(&mut st, &ps);
    publish_manifest(&*self.store, &mut st)?;
    gc_journal(&*self.store, &mut st);
    for seg in old {
      if eager {
        let _ = self.store.delete(&segment_key(&prefix, name, seg.id));
      }
      self.index_cache.remove(seg.id);
    }
    Ok(())
  }

  pub(crate) fn rm_partition(&self, name: &str) -> Result<()> {
    self.ensure_writer()?;
    let _del = self.readers.exclusive();
    let Some(lock) = self.partition_lock(name) else {
      return Ok(());
    };
    let mut ps = lock.write();
    let mut st = self.state.write();
    let prefix = st.cfg.prefix.clone();
    let eager = st.cfg.eager_object_delete;
    let tail_seq = st.journal_seq;
    let old = std::mem::take(&mut ps.meta.segments);
    ps.mem.clear();
    ps.mem_bytes = 0;
    ps.meta.watermark = tail_seq;
    st.pending_flushes.remove(name);
    ps.meta.dropped = true;
    sync_partition_meta(&mut st, &ps);
    publish_manifest(&*self.store, &mut st)?;
    gc_journal(&*self.store, &mut st);
    for seg in old {
      if eager {
        let _ = self.store.delete(&segment_key(&prefix, name, seg.id));
      }
      self.index_cache.remove(seg.id);
    }
    Ok(())
  }

  /// Flush + merge-compact a single partition.
  pub(crate) fn compact_partition(&self, name: &str) -> Result<()> {
    self.ensure_writer()?;
    if self.state.read().pending_flushes.contains_key(name) {
      finish_partition_flush(self, name)?;
    }
    let Some(lock) = self.partition_lock(name) else {
      return Ok(());
    };
    let old = {
      let mut ps = lock.write();
      let mut st = self.state.write();
      let old = compact_partition_locked(&*self.store, &mut st, &mut ps)?;
      gc_journal(&*self.store, &mut st);
      old
    };
    if let Some(old) = old {
      self.delete_old_segments(name, old);
    }
    Ok(())
  }

  /// Flush + merge-compact every non-dropped partition.
  pub(crate) fn compact_all(&self) -> Result<()> {
    self.ensure_writer()?;
    let pending: Vec<String> = self.state.read().pending_flushes.keys().cloned().collect();
    for name in pending {
      finish_partition_flush(self, &name)?;
    }
    let names = self.partitions.names();
    for name in names {
      let Some(lock) = self.partition_lock(&name) else {
        continue;
      };
      let old = {
        let mut ps = lock.write();
        let mut st = self.state.write();
        let old = if !ps.meta.dropped && (!ps.mem.is_empty() || ps.meta.segments.len() > 1) {
          compact_partition_locked(&*self.store, &mut st, &mut ps)?
        } else {
          None
        };
        gc_journal(&*self.store, &mut st);
        old
      };
      if let Some(old) = old {
        self.delete_old_segments(&name, old);
      }
    }
    Ok(())
  }

  /// Delete old segment objects after the partition lock has been released,
  /// waiting for any in-flight readers to drain first.
  fn delete_old_segments(&self, part: &str, old: Vec<SegmentMeta>) {
    if old.is_empty() {
      return;
    }
    let _del = self.readers.exclusive();
    let (eager, prefix) = {
      let st = self.state.read();
      (st.cfg.eager_object_delete, st.cfg.prefix.clone())
    };
    for seg in old {
      if eager {
        let _ = self.store.delete(&segment_key(&prefix, part, seg.id));
      }
      self.index_cache.remove(seg.id);
    }
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
      .partitions
      .get(name)
      .map(|lock| !lock.read().meta.dropped)
      .unwrap_or(false)
  }

  fn list_partitions(&self) -> Result<Vec<String>> {
    Ok(
      self
        .inner
        .partitions
        .snapshot()
        .iter()
        .filter(|lock| !lock.read().meta.dropped)
        .map(|lock| lock.read().name.clone())
        .collect(),
    )
  }

  fn rm_partition(&self, partition: &Self::Partition) -> Result<()> {
    self.inner.rm_partition(&partition.name)
  }

  fn write_buffer_size(&self) -> u64 {
    self
      .inner
      .partitions
      .snapshot()
      .iter()
      .map(|lock| lock.read().mem_bytes)
      .sum()
  }

  fn journal_count(&self) -> usize {
    self.inner.state.read().journal_seqs.len()
  }

  fn journal_disk_space(&self) -> Result<u64> {
    let st = self.inner.state.read();
    Ok(st.journal_sizes.values().copied().sum())
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
    // Background memtable uploads are durable through journal objects even
    // before their segments are published, but persist() also drains any
    // already-detached segment snapshots to reach the fully folded state.
    let parts: Vec<String> = self
      .inner
      .state
      .read()
      .pending_flushes
      .keys()
      .cloned()
      .collect();
    for part in parts {
      finish_partition_flush(&self.inner, &part)?;
    }
    Ok(())
  }

  fn disk_space(&self) -> Result<u64> {
    let segments_mem: u64 = self
      .inner
      .partitions
      .snapshot()
      .iter()
      .map(|lock| {
        let ps = lock.read();
        ps.meta.segments.iter().map(|s| s.bytes).sum::<u64>() + ps.mem_bytes
      })
      .sum();
    let st = self.inner.state.read();
    Ok(segments_mem + st.journal_sizes.values().copied().sum::<u64>() + st.manifest_bytes)
  }

  fn compact(&self) -> Result<()> {
    self.inner.compact_all()
  }
}
