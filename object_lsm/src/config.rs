//! Engine configuration.

/// Default per-partition memtable budget before it is flushed to a segment.
pub const DEFAULT_MAX_MEMTABLE_BYTES: u64 = 16 * 1024 * 1024;
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
}

impl Default for Config {
  fn default() -> Self {
    Self {
      prefix: DEFAULT_PREFIX.into(),
      max_memtable_bytes: DEFAULT_MAX_MEMTABLE_BYTES,
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
}
