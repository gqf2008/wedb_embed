//! Object key layout helpers.
//!
//! Layout under the instance `prefix`:
//! ```text
//! <prefix>/manifest/current          pointer object -> latest manifest seq
//! <prefix>/manifest/<seq:020>        immutable manifest snapshots
//! <prefix>/journal/<seq:020>         immutable committed record groups
//! <prefix>/seg/<partition>/<id:020>  immutable sorted segment objects
//! ```
//! Zero-padded numeric suffixes keep `list` lexicographic = numeric order.

/// Pointer object holding the latest manifest sequence number (decimal).
pub fn current_key(prefix: &str) -> String {
  format!("{prefix}/manifest/current")
}

pub fn manifest_key(prefix: &str, seq: u64) -> String {
  format!("{prefix}/manifest/{seq:020}")
}

pub fn journal_key(prefix: &str, seq: u64) -> String {
  format!("{prefix}/journal/{seq:020}")
}

pub fn segment_key(prefix: &str, part: &str, id: u64) -> String {
  format!("{prefix}/seg/{part}/{id:020}")
}

pub fn journal_prefix(prefix: &str) -> String {
  format!("{prefix}/journal/")
}

pub fn manifest_prefix(prefix: &str) -> String {
  format!("{prefix}/manifest/")
}

/// Extract the trailing numeric sequence from a key like `<...>/<seq:020>`.
pub fn parse_tail_seq(key: &str) -> Option<u64> {
  let (_, tail) = key.rsplit_once('/')?;
  tail.parse::<u64>().ok()
}
