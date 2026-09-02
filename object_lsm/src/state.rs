//! In-memory engine state (partitions + counters).

use std::collections::{BTreeMap, BTreeSet};

use crate::{config::Config, segment::SegmentMeta};

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
#[derive(Debug)]
pub struct PartitionState {
  pub name: String,
  /// Newest layer: unflushed entries (values and tombstones).
  pub mem: BTreeMap<Vec<u8>, MemEntry>,
  pub mem_bytes: u64,
  /// Flushed immutable segments, oldest first.
  pub segments: Vec<SegmentMeta>,
  /// Highest journal seq folded into segments or discarded.
  pub watermark: u64,
  /// See [`crate::manifest::PartitionMeta::dropped`].
  pub dropped: bool,
}

impl PartitionState {
  pub fn new(name: impl Into<String>) -> Self {
    Self {
      name: name.into(),
      mem: BTreeMap::new(),
      mem_bytes: 0,
      segments: Vec::new(),
      watermark: 0,
      dropped: false,
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

/// Whole-engine state guarded by a single read/write lock.
#[derive(Debug)]
pub struct EngineState {
  pub cfg: Config,
  pub partitions: BTreeMap<String, PartitionState>,
  /// Last assigned journal group seq (0 = none).
  pub journal_seq: u64,
  /// Last manifest seq written (0 = none).
  pub manifest_seq: u64,
  pub next_segment_id: u64,
  /// Journal group seqs whose objects still exist (maintained for GC).
  pub journal_seqs: BTreeSet<u64>,
  /// Completed merge compactions (metrics).
  pub compactions_completed: u64,
}

impl EngineState {
  pub fn new(cfg: Config) -> Self {
    Self {
      cfg,
      partitions: BTreeMap::new(),
      journal_seq: 0,
      manifest_seq: 0,
      next_segment_id: 0,
      journal_seqs: BTreeSet::new(),
      compactions_completed: 0,
    }
  }
}
