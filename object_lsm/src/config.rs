//! Engine configuration.

/// Default per-partition memtable budget before it is flushed to a segment.
pub const DEFAULT_MAX_MEMTABLE_BYTES: u64 = 16 * 1024 * 1024;
/// Default target data-block payload size inside segment objects.
pub const DEFAULT_BLOCK_SIZE: u32 = 32 * 1024;
/// Default block cache capacity.
pub const DEFAULT_CACHE_CAPACITY: u64 = 64 * 1024 * 1024;
/// Default object-key prefix.
pub const DEFAULT_PREFIX: &str = "wedb/objectlsm";

/// Engine configuration.
#[derive(Clone, Debug)]
pub struct Config {
  /// Object key prefix that isolates this engine instance inside a bucket.
  pub prefix: String,
  /// Flush a partition's memtable to an immutable segment once its estimated
  /// byte size exceeds this budget.
  pub max_memtable_bytes: u64,
  /// Target data-block payload size inside segment objects (bytes).
  pub block_size: u32,
  /// Block cache capacity in bytes (0 disables the cache).
  pub cache_capacity: u64,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      prefix: DEFAULT_PREFIX.into(),
      max_memtable_bytes: DEFAULT_MAX_MEMTABLE_BYTES,
      block_size: DEFAULT_BLOCK_SIZE,
      cache_capacity: DEFAULT_CACHE_CAPACITY,
    }
  }
}

impl Config {
  /// Config with a custom object-key prefix.
  pub fn new(prefix: impl Into<String>) -> Self {
    Self {
      prefix: prefix.into(),
      ..Self::default()
    }
  }

  /// Set the per-partition memtable flush budget.
  pub fn max_memtable_bytes(mut self, bytes: u64) -> Self {
    self.max_memtable_bytes = bytes;
    self
  }

  /// Set the target segment data-block payload size.
  pub fn block_size(mut self, bytes: u32) -> Self {
    self.block_size = bytes;
    self
  }

  /// Set the block cache capacity in bytes.
  pub fn cache_capacity(mut self, bytes: u64) -> Self {
    self.cache_capacity = bytes;
    self
  }
}
