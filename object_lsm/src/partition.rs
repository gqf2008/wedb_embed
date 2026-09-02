//! [`Partition`] trait implementation with streaming iterators.

use std::{ops::Bound, sync::Arc};

use wedb_embed_engine::{KvEntry, Partition};

use crate::{
  engine::Inner,
  error::{Error, Result},
  journal::Op,
  scan::{BackMerge, Bounds, FwdMerge},
};

/// Partition handle: identifies a keyspace by name and shares engine state.
#[derive(Clone)]
pub struct ObjectLsmPartition {
  pub name: String,
  pub(crate) inner: Arc<Inner>,
}

/// Owned key-value entry produced by iterators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLsmEntry {
  pub key: Vec<u8>,
  pub value: Vec<u8>,
}

impl KvEntry for ObjectLsmEntry {
  type Key = Vec<u8>;
  type Value = Vec<u8>;

  fn key(&self) -> &Self::Key {
    &self.key
  }

  fn value(&self) -> &Self::Value {
    &self.value
  }
}

/// Materialized fallback used once a scan mixes forward and backward pulls.
struct Mat {
  entries: Vec<ObjectLsmEntry>,
  front: usize,
  back: usize,
}

/// Ordered iterator over a partition snapshot.
///
/// Pure `next()` streams through a forward K-way merge (one block per source
/// in memory); pure `next_back()` streams backward the same way. Mixing the two
/// directions materializes the remaining result set once to stay correct.
pub struct ObjectLsmIter {
  inner: Arc<Inner>,
  part: String,
  bounds: Bounds,
  fwd: Option<FwdMerge>,
  back: Option<BackMerge>,
  mat: Option<Mat>,
  front_used: bool,
  back_used: bool,
  front_done: usize,
  back_done: usize,
  err: Option<Error>,
}

impl ObjectLsmIter {
  fn new(inner: Arc<Inner>, part: String, bounds: Bounds) -> Self {
    Self {
      inner,
      part,
      bounds,
      fwd: None,
      back: None,
      mat: None,
      front_used: false,
      back_used: false,
      front_done: 0,
      back_done: 0,
      err: None,
    }
  }

  fn materialize(&mut self) -> Result<()> {
    if self.mat.is_some() {
      return Ok(());
    }
    let lower = bound_as_slice(&self.bounds.lower);
    let upper = bound_as_slice(&self.bounds.upper);
    let pairs = self.inner.collect(&self.part, lower, upper)?;
    let entries = pairs
      .into_iter()
      .map(|(key, value)| ObjectLsmEntry { key, value })
      .collect();
    self.mat = Some(Mat {
      entries,
      front: self.front_done,
      back: self.back_done,
    });
    self.fwd = None;
    self.back = None;
    Ok(())
  }

  fn take_mat_front(&mut self) -> Option<Result<ObjectLsmEntry>> {
    let m = self.mat.as_mut()?;
    if m.front + m.back >= m.entries.len() {
      return None;
    }
    let e = m.entries[m.front].clone();
    m.front += 1;
    Some(Ok(e))
  }

  fn take_mat_back(&mut self) -> Option<Result<ObjectLsmEntry>> {
    let m = self.mat.as_mut()?;
    if m.front + m.back >= m.entries.len() {
      return None;
    }
    let idx = m.entries.len() - 1 - m.back;
    let e = m.entries[idx].clone();
    m.back += 1;
    Some(Ok(e))
  }
}

fn bound_as_slice(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
  match b {
    Bound::Included(x) => Bound::Included(x.as_slice()),
    Bound::Excluded(x) => Bound::Excluded(x.as_slice()),
    Bound::Unbounded => Bound::Unbounded,
  }
}

fn bound_owned(b: Bound<&[u8]>) -> Bound<Vec<u8>> {
  match b {
    Bound::Included(x) => Bound::Included(x.to_vec()),
    Bound::Excluded(x) => Bound::Excluded(x.to_vec()),
    Bound::Unbounded => Bound::Unbounded,
  }
}

impl Iterator for ObjectLsmIter {
  type Item = Result<ObjectLsmEntry>;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(e) = self.err.take() {
      return Some(Err(e));
    }
    if self.mat.is_some() {
      return self.take_mat_front();
    }
    self.front_used = true;
    if self.back_used {
      if let Err(e) = self.materialize() {
        self.err = Some(e.clone());
        return Some(Err(e));
      }
      return self.take_mat_front();
    }
    if self.fwd.is_none() {
      match FwdMerge::new(self.inner.clone(), self.part.clone(), self.bounds.clone()) {
        Ok(m) => self.fwd = Some(m),
        Err(e) => {
          self.err = Some(e.clone());
          return Some(Err(e));
        }
      }
    }
    match self.fwd.as_mut().unwrap().next() {
      Ok(Some((key, value))) => {
        self.front_done += 1;
        Some(Ok(ObjectLsmEntry { key, value }))
      }
      Ok(None) => None,
      Err(e) => {
        self.err = Some(e.clone());
        Some(Err(e))
      }
    }
  }
}

impl DoubleEndedIterator for ObjectLsmIter {
  fn next_back(&mut self) -> Option<Self::Item> {
    if let Some(e) = self.err.take() {
      return Some(Err(e));
    }
    if self.mat.is_some() {
      return self.take_mat_back();
    }
    self.back_used = true;
    if self.front_used {
      if let Err(e) = self.materialize() {
        self.err = Some(e.clone());
        return Some(Err(e));
      }
      return self.take_mat_back();
    }
    if self.back.is_none() {
      match BackMerge::new(self.inner.clone(), self.part.clone(), self.bounds.clone()) {
        Ok(m) => self.back = Some(m),
        Err(e) => {
          self.err = Some(e.clone());
          return Some(Err(e));
        }
      }
    }
    match self.back.as_mut().unwrap().next() {
      Ok(Some((key, value))) => {
        self.back_done += 1;
        Some(Ok(ObjectLsmEntry { key, value }))
      }
      Ok(None) => None,
      Err(e) => {
        self.err = Some(e.clone());
        Some(Err(e))
      }
    }
  }
}

impl ObjectLsmPartition {
  fn stream(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> ObjectLsmIter {
    ObjectLsmIter::new(
      self.inner.clone(),
      self.name.clone(),
      Bounds {
        lower: bound_owned(lower),
        upper: bound_owned(upper),
      },
    )
  }
}

impl Partition for ObjectLsmPartition {
  type Error = Error;
  type Value = Vec<u8>;
  type Entry<'a> = ObjectLsmEntry;
  type Iter<'a> = ObjectLsmIter;

  fn get(&self, key: &[u8]) -> Result<Option<Self::Value>> {
    self.inner.lookup(&self.name, key)
  }

  fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
    self.inner.commit_ops(vec![Op::put(
      self.name.clone(),
      key.to_vec(),
      value.to_vec(),
    )])
  }

  fn rm(&self, key: &[u8]) -> Result<()> {
    self
      .inner
      .commit_ops(vec![Op::delete(self.name.clone(), key.to_vec())])
  }

  fn clear(&self) -> Result<()> {
    self.inner.clear_partition(&self.name)
  }

  fn iter(&self) -> Self::Iter<'_> {
    self.stream(Bound::Unbounded, Bound::Unbounded)
  }

  fn prefix(&self, prefix: &[u8]) -> Self::Iter<'_> {
    let mut end = prefix.to_vec();
    let upper = match end.last_mut() {
      None => Bound::Unbounded,
      Some(last) if *last == u8::MAX => Bound::Unbounded,
      Some(last) => {
        *last += 1;
        Bound::Excluded(end.as_slice())
      }
    };
    self.stream(Bound::Included(prefix), upper)
  }

  fn range(&self, range: (Bound<&[u8]>, Bound<&[u8]>)) -> Self::Iter<'_> {
    self.stream(range.0, range.1)
  }

  fn approximate_len(&self) -> Result<usize> {
    let st = self.inner.state.read();
    let Some(ps) = st.partitions.get(&self.name) else {
      return Ok(0);
    };
    // O(#segments + memtable), never a full partition scan: live values in the
    // memtable plus per-segment raw entries minus tombstones (duplicates across
    // segments intentionally overcount, matching an approximate LSM count).
    let seg: usize = ps
      .segments
      .iter()
      .map(|s| (s.count - s.tombstones) as usize)
      .sum();
    let mem = ps
      .mem
      .values()
      .filter(|e| matches!(e, crate::state::MemEntry::Value(_)))
      .count();
    Ok(seg + mem)
  }

  fn table_count(&self) -> usize {
    self
      .inner
      .state
      .read()
      .partitions
      .get(&self.name)
      .map(|p| p.segments.len())
      .unwrap_or(0)
  }

  fn disk_space(&self) -> Result<u64> {
    let st = self.inner.state.read();
    Ok(
      st.partitions
        .get(&self.name)
        .map(|p| p.segments.iter().map(|s| s.bytes).sum::<u64>())
        .unwrap_or(0),
    )
  }

  fn compact(&self) -> Result<()> {
    self.inner.compact_partition(&self.name)
  }
}
