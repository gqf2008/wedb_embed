//! [`Partition`] trait implementation.

use std::{ops::Bound, sync::Arc};

use wedb_embed_engine::{KvEntry, Partition};

use crate::{
  engine::Inner,
  error::{Error, Result},
  journal::Op,
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

/// Snapshot iterator over live entries (M1 materializes the result set; a
/// streaming block iterator replaces this in M2).
pub struct ObjectLsmIter {
  inner: std::vec::IntoIter<Result<ObjectLsmEntry>>,
}

impl Iterator for ObjectLsmIter {
  type Item = Result<ObjectLsmEntry>;

  fn next(&mut self) -> Option<Self::Item> {
    self.inner.next()
  }
}

impl DoubleEndedIterator for ObjectLsmIter {
  fn next_back(&mut self) -> Option<Self::Item> {
    self.inner.next_back()
  }
}

impl ObjectLsmPartition {
  fn collect_iter(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> ObjectLsmIter {
    let items = match self.inner.collect(&self.name, lower, upper) {
      Ok(pairs) => pairs
        .into_iter()
        .map(|(key, value)| Ok(ObjectLsmEntry { key, value }))
        .collect(),
      Err(e) => vec![Err(e)],
    };
    ObjectLsmIter {
      inner: items.into_iter(),
    }
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
    self.collect_iter(Bound::Unbounded, Bound::Unbounded)
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
    self.collect_iter(Bound::Included(prefix), upper)
  }

  fn range(&self, range: (Bound<&[u8]>, Bound<&[u8]>)) -> Self::Iter<'_> {
    self.collect_iter(range.0, range.1)
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
