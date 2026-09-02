//! Engine implementation: open/recovery, commit pipeline, segment flush,
//! manifest publishing, block/range read path and the [`Engine`] trait impl.

use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;
use wedb_embed_engine::Engine;

use crate::{
  batch::ObjectLsmBatch,
  cache::{BlockCache, IndexCache},
  config::Config,
  error::{Error, Result},
  journal::{Group, Op, decode_group, encode_group},
  keys::{current_key, journal_key, journal_prefix, manifest_key, parse_tail_seq, segment_key},
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
}

/// Object-storage-backed LSM engine implementing the wedb_embed_engine traits.
///
/// # Consistency model (M1)
/// - every [`Batch`] commit first uploads one immutable journal group object
///   (the atomic durability point), then applies the group to memtables;
/// - memtables spill into immutable block-indexed segment objects once they
///   exceed `Config::max_memtable_bytes`;
/// - a single manifest object chain records live segments + per-partition
///   journal watermarks;
/// - opening re-reads `current -> manifest` and replays journal groups newer
///   than each partition watermark, so a crash loses nothing that was acked.
///
/// [`Batch`]: wedb_embed_engine::Batch
#[derive(Clone)]
pub struct ObjectLsm {
  pub(crate) inner: Arc<Inner>,
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
    let state = recover(&*store, &cfg)?;
    let block_cache = BlockCache::new(cfg.cache_capacity);
    let index_cache = IndexCache::default();
    Ok(Self {
      inner: Arc::new(Inner {
        store,
        state: RwLock::new(state),
        block_cache,
        index_cache,
      }),
    })
  }
}

/// Rebuild in-memory state from the durable manifest + journal tail.
fn recover(store: &dyn Store, cfg: &Config) -> Result<EngineState> {
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
  let mut seqs = Vec::new();
  for k in &list {
    if let Some(s) = parse_tail_seq(k) {
      max_seq = max_seq.max(s);
      seqs.push(s);
    }
  }
  seqs.sort_unstable();
  st.journal_seq = max_seq;
  for s in seqs {
    let Some(bytes) = store.get(&journal_key(prefix, s))? else {
      continue;
    };
    let group = decode_group(&bytes)?;
    apply_group(&mut st, &group)?;
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
    self.store.put(&journal_key(&st.cfg.prefix, seq), &bytes)?;
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

  /// Snapshot all live entries of a partition within `(lower, upper)`, sorted.
  pub(crate) fn collect(
    &self,
    name: &str,
    lower: std::ops::Bound<&[u8]>,
    upper: std::ops::Bound<&[u8]>,
  ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let st = self.state.read();
    let prefix = st.cfg.prefix.clone();
    let Some(ps) = st.partitions.get(name) else {
      return Ok(Vec::new());
    };
    let mut map: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    for (k, e) in &ps.mem {
      if !in_bounds(k, lower, upper) {
        continue;
      }
      let v = match e {
        MemEntry::Value(v) => Some(v.clone()),
        MemEntry::Tombstone => None,
      };
      map.insert(k.clone(), v);
    }
    for seg in ps.segments.iter().rev() {
      if !seg_overlaps(seg, lower, upper) {
        continue;
      }
      let index = self.load_index(&prefix, name, seg)?;
      for block in &index.blocks {
        let entries = self.load_block(&prefix, name, seg, block)?;
        for (k, v) in entries.iter() {
          if !in_bounds(k, lower, upper) {
            continue;
          }
          map.entry(k.clone()).or_insert(v.clone());
        }
      }
    }
    Ok(
      map
        .into_iter()
        .filter_map(|(k, v)| v.map(|val| (k, val)))
        .collect(),
    )
  }

  pub(crate) fn clear_partition(&self, name: &str) -> Result<()> {
    let mut st = self.state.write();
    let prefix = st.cfg.prefix.clone();
    let tail_seq = st.journal_seq;
    {
      let ps = match st.partitions.get_mut(name) {
        Some(ps) => ps,
        None => return Ok(()),
      };
      for seg in ps.segments.drain(..) {
        let _ = self.store.delete(&segment_key(&prefix, name, seg.id));
        self.index_cache.remove(seg.id);
      }
      ps.mem.clear();
      ps.mem_bytes = 0;
      ps.watermark = tail_seq;
      ps.dropped = false;
    }
    publish_manifest(&*self.store, &mut st)
  }

  pub(crate) fn rm_partition(&self, name: &str) -> Result<()> {
    let mut st = self.state.write();
    let prefix = st.cfg.prefix.clone();
    let tail_seq = st.journal_seq;
    {
      let ps = match st.partitions.get_mut(name) {
        Some(ps) => ps,
        None => return Ok(()),
      };
      for seg in ps.segments.drain(..) {
        let _ = self.store.delete(&segment_key(&prefix, name, seg.id));
        self.index_cache.remove(seg.id);
      }
      ps.mem.clear();
      ps.mem_bytes = 0;
      ps.watermark = tail_seq;
      ps.dropped = true;
    }
    publish_manifest(&*self.store, &mut st)
  }

  pub(crate) fn compact_partition(&self, name: &str) -> Result<()> {
    let mut st = self.state.write();
    flush_partition(&*self.store, &mut st, name)
  }

  pub(crate) fn flush_all(&self) -> Result<()> {
    let mut st = self.state.write();
    let names: Vec<String> = st
      .partitions
      .values()
      .filter(|p| !p.dropped && !p.mem.is_empty())
      .map(|p| p.name.clone())
      .collect();
    for name in names {
      flush_partition(&*self.store, &mut st, &name)?;
    }
    Ok(())
  }
}

fn in_bounds(k: &[u8], lower: std::ops::Bound<&[u8]>, upper: std::ops::Bound<&[u8]>) -> bool {
  let lo = match lower {
    std::ops::Bound::Included(x) => k >= x,
    std::ops::Bound::Excluded(x) => k > x,
    std::ops::Bound::Unbounded => true,
  };
  let hi = match upper {
    std::ops::Bound::Included(x) => k <= x,
    std::ops::Bound::Excluded(x) => k < x,
    std::ops::Bound::Unbounded => true,
  };
  lo && hi
}

fn seg_overlaps(
  seg: &SegmentMeta,
  lower: std::ops::Bound<&[u8]>,
  upper: std::ops::Bound<&[u8]>,
) -> bool {
  let lo_ok = match lower {
    std::ops::Bound::Unbounded => true,
    std::ops::Bound::Included(x) => seg.last.as_slice() >= x,
    std::ops::Bound::Excluded(x) => seg.last.as_slice() > x,
  };
  let hi_ok = match upper {
    std::ops::Bound::Unbounded => true,
    std::ops::Bound::Included(x) => seg.first.as_slice() <= x,
    std::ops::Bound::Excluded(x) => seg.first.as_slice() < x,
  };
  lo_ok && hi_ok
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

  fn batch(&self) -> Self::Batch {
    ObjectLsmBatch::new(self.inner.clone())
  }

  fn batch_with_capacity(&self, capacity: usize) -> Self::Batch {
    ObjectLsmBatch::with_capacity(self.inner.clone(), capacity)
  }

  fn persist(&self) -> Result<()> {
    // M1: every committed group is already a durable journal object, so
    // persist() is a no-op consistency point.
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
    // M1: fold every memtable into a segment. Segment-merging compaction is M3.
    self.inner.flush_all()
  }
}
