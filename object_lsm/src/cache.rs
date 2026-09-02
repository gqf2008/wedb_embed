//! Block cache + segment-index cache.
//!
//! The block cache keeps decoded block entry lists in memory with
//! insertion-order eviction (a cheap stand-in for LRU, M2). Segment indexes are
//! cached separately because they are tiny and hot.

use std::{
  collections::{HashMap, VecDeque},
  sync::{Arc, Mutex},
};

use crate::segment::{SegmentEntries, SegmentIndex};

struct CachedBlock {
  entries: Arc<SegmentEntries>,
  size: u64,
}

struct BlockCacheInner {
  capacity: u64,
  used: u64,
  map: HashMap<(u64, u32), CachedBlock>,
  order: VecDeque<(u64, u32)>,
}

/// Bounded cache of decoded segment blocks, keyed by `(segment_id, block_idx)`.
#[derive(Clone)]
pub struct BlockCache {
  inner: Arc<Mutex<BlockCacheInner>>,
}

impl BlockCache {
  pub fn new(capacity: u64) -> Self {
    Self {
      inner: Arc::new(Mutex::new(BlockCacheInner {
        capacity,
        used: 0,
        map: HashMap::new(),
        order: VecDeque::new(),
      })),
    }
  }

  pub fn get(&self, seg: u64, block: u32) -> Option<Arc<SegmentEntries>> {
    self
      .inner
      .lock()
      .unwrap()
      .map
      .get(&(seg, block))
      .map(|c| c.entries.clone())
  }

  pub fn insert(&self, seg: u64, block: u32, entries: Arc<SegmentEntries>, size: u64) {
    let mut g = self.inner.lock().unwrap();
    if g.capacity == 0 {
      return;
    }
    if let Some(old) = g.map.get(&(seg, block)) {
      g.used = g.used.saturating_sub(old.size);
    }
    g.map.insert((seg, block), CachedBlock { entries, size });
    g.used += size;
    g.order.push_back((seg, block));
    while g.used > g.capacity {
      let Some(key) = g.order.pop_front() else {
        break;
      };
      if let Some(removed) = g.map.remove(&key) {
        g.used = g.used.saturating_sub(removed.size);
      }
    }
  }

  pub fn used(&self) -> u64 {
    self.inner.lock().unwrap().used
  }

  pub fn capacity(&self) -> u64 {
    self.inner.lock().unwrap().capacity
  }
}

/// Unbounded cache of parsed segment block indexes (small, hot metadata).
#[derive(Clone, Default)]
pub struct IndexCache {
  map: Arc<Mutex<HashMap<u64, Arc<SegmentIndex>>>>,
}

impl IndexCache {
  pub fn get(&self, seg: u64) -> Option<Arc<SegmentIndex>> {
    self.map.lock().unwrap().get(&seg).cloned()
  }

  pub fn insert(&self, seg: u64, index: Arc<SegmentIndex>) {
    self.map.lock().unwrap().insert(seg, index);
  }

  pub fn remove(&self, seg: u64) {
    self.map.lock().unwrap().remove(&seg);
  }
}
