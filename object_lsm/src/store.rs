//! Object-storage abstraction + in-memory implementation for tests.

use std::{
  collections::BTreeMap,
  sync::{Arc, Mutex},
};

use crate::error::Result;

/// Minimal object-storage interface required by the engine.
///
/// All methods are synchronous; a remote implementation (AWS S3 / Cloudflare R2 /
/// MinIO via `object_store`) bridges async SDK calls with an internal runtime.
pub trait Store: Send + Sync + 'static {
  /// Fetch an object; `None` when the key does not exist.
  fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

  /// Atomically create or overwrite an object.
  fn put(&self, key: &str, data: &[u8]) -> Result<()>;

  /// Delete an object; deleting a missing key is a no-op.
  fn delete(&self, key: &str) -> Result<()>;

  /// List object keys under `prefix` in lexicographic order.
  fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

#[derive(Default)]
struct MemoryInner {
  objects: BTreeMap<String, Vec<u8>>,
}

/// In-memory [`Store`] used for unit tests and offline development.
#[derive(Clone, Default)]
pub struct MemoryStore {
  inner: Arc<Mutex<MemoryInner>>,
}

impl MemoryStore {
  /// Create an empty in-memory store.
  pub fn new() -> Self {
    Self::default()
  }
}

impl Store for MemoryStore {
  fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
    Ok(self.inner.lock().unwrap().objects.get(key).cloned())
  }

  fn put(&self, key: &str, data: &[u8]) -> Result<()> {
    self
      .inner
      .lock()
      .unwrap()
      .objects
      .insert(key.to_string(), data.to_vec());
    Ok(())
  }

  fn delete(&self, key: &str) -> Result<()> {
    self.inner.lock().unwrap().objects.remove(key);
    Ok(())
  }

  fn list(&self, prefix: &str) -> Result<Vec<String>> {
    Ok(
      self
        .inner
        .lock()
        .unwrap()
        .objects
        .range(prefix.to_string()..)
        .take_while(|(k, _)| k.starts_with(prefix))
        .map(|(k, _)| k.clone())
        .collect(),
    )
  }
}
