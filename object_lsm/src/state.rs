//! In-memory engine state (partitions + counters).

use std::{
  collections::{BTreeMap, BTreeSet},
  sync::Arc,
};

use parking_lot::RwLock;

use crate::{config::Config, manifest::PartitionMeta};

/// A value in a partition memtable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemEntry {
  Value(Vec<u8>),
  Tombstone,
}

impl MemEntry {
  /// Estimated contribution of an entry to the memtable byte budget.
  pub fn contrib(key_len: usize, e: Option<&MemEntry>) -> u64 {
    let overhead = 48u64;
    match e {
      Some(MemEntry::Value(v)) => overhead + key_len as u64 + v.len() as u64,
      Some(MemEntry::Tombstone) => overhead + key_len as u64 + 8,
      None => 0,
    }
  }
}

/// Runtime state of a single partition.
///
/// The memtable is guarded by this partition's own lock so different
/// partitions can be read/written concurrently. Durable metadata is mirrored
/// into [`EngineState::partitions`] whenever it changes; that mirror is what
/// manifest publishing and global GC read under the global lock.
#[derive(Debug)]
pub struct PartitionState {
  pub name: String,
  /// Newest layer: unflushed entries (values and tombstones).
  pub mem: BTreeMap<Vec<u8>, MemEntry>,
  pub mem_bytes: u64,
  /// Flushed immutable segments + durable metadata.
  pub meta: PartitionMeta,
}

impl PartitionState {
  pub fn new(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      mem: BTreeMap::new(),
      mem_bytes: 0,
      meta: PartitionMeta::default(),
    }
  }

  /// Apply one mutation to the memtable, maintaining the byte budget.
  pub fn apply(&mut self, key: &[u8], value: Option<&[u8]>) {
    let prev_contrib = MemEntry::contrib(key.len(), self.mem.get(key));
    let new = match value {
      Some(v) => MemEntry::Value(v.to_vec()),
      None => MemEntry::Tombstone,
    };
    self.mem.insert(key.to_vec(), new);
    let new_contrib = MemEntry::contrib(key.len(), self.mem.get(key));
    self.mem_bytes = self
      .mem_bytes
      .saturating_add(new_contrib)
      .saturating_sub(prev_contrib);
  }
}

/// Shared, independently lockable partition state.
pub type PartitionLock = Arc<RwLock<PartitionState>>;

/// Whole-engine state guarded by a single read/write lock.
///
/// Only global metadata lives here. `partitions` is the durable/manifest view;
/// `partition_locks` is the per-partition data-plane lock table. Partition
/// memtables are mutated while holding the corresponding partition lock, never
/// while holding this global lock.
#[derive(Debug)]
pub struct EngineState {
  pub cfg: Config,
  /// Durable per-partition metadata used by manifest publishing and GC.
  pub partitions: BTreeMap<String, PartitionMeta>,
  /// Lock table for partition data plane.
  pub partition_locks: BTreeMap<String, PartitionLock>,
  /// Last assigned journal group seq (0 = none).
  pub journal_seq: u64,
  /// Last manifest seq written (0 = none).
  pub manifest_seq: u64,
  pub next_segment_id: u64,
  /// Journal object end-seqs whose objects still exist (maintained for GC).
  pub journal_seqs: BTreeSet<u64>,
  /// Encoded groups waiting for group-commit flush (windowed mode).
  pub pending: Vec<u8>,
  /// First seq present in `pending` (0 when empty).
  pub pending_lo: u64,
  /// Completed merge compactions (metrics).
  pub compactions_completed: u64,
}

impl EngineState {
  pub fn new(cfg: Config) -> Self {
    Self {
      cfg,
      partitions: BTreeMap::new(),
      partition_locks: BTreeMap::new(),
      journal_seq: 0,
      manifest_seq: 0,
      next_segment_id: 0,
      journal_seqs: BTreeSet::new(),
      compactions_completed: 0,
      pending: Vec::new(),
      pending_lo: 0,
    }
  }

  /// Insert a brand-new partition into both the lock table and the durable
  /// metadata mirror. Callers must hold the global write lock.
  pub fn insert_partition(&mut self, name: impl Into<String>) -> PartitionLock {
    let name = name.into();
    let lock = Arc::new(RwLock::new(PartitionState::new(name.clone())));
    self.partition_locks.insert(name.clone(), lock.clone());
    self.partitions.insert(name, PartitionMeta::default());
    lock
  }
}
