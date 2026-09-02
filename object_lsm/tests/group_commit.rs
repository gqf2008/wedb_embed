//! Group-commit journal batching tests (Config::journal_window_ms).

use std::{sync::Arc, thread, time::Duration};

use wedb_embed_engine::{Engine, Partition};
use wedb_object_lsm::{
  Config, MemoryStore, ObjectLsm, Store, journal::decode_group_stream, keys::journal_prefix,
};

fn window_cfg(prefix: &str, window_ms: u64) -> Config {
  Config::new(prefix)
    .max_memtable_bytes(1 << 20)
    .block_size(1024)
    .journal_window_ms(Some(window_ms))
}

#[test]
fn grouped_window_batches_many_commits_into_one_object() {
  let store = MemoryStore::new();
  let cfg = window_cfg("gc/batch", 60_000); // long window: no auto flush
  let eng = ObjectLsm::open(Arc::new(store.clone()), cfg).unwrap();
  let p = eng.partition("data").unwrap();
  for i in 0..50u32 {
    p.insert(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes())
      .unwrap();
  }
  // Nothing flushed yet (window is long).
  assert!(store.list(&journal_prefix("gc/batch")).unwrap().is_empty());
  // persist() forces one synchronous journal object for all 50 groups.
  eng.persist().unwrap();
  let objs = store.list(&journal_prefix("gc/batch")).unwrap();
  assert_eq!(
    objs.len(),
    1,
    "expected 1 batched journal object, got {objs:?}"
  );
  let bytes = store.get(&objs[0]).unwrap().unwrap();
  let groups = decode_group_stream(&bytes).unwrap();
  assert_eq!(groups.len(), 50);
  assert_eq!(groups[0].seq, 1);
  assert_eq!(groups[49].seq, 50);

  drop(p);
  drop(eng);
  let eng2 = ObjectLsm::open(Arc::new(store.clone()), window_cfg("gc/batch", 60_000)).unwrap();
  let p2 = eng2.partition("data").unwrap();
  assert_eq!(p2.len().unwrap(), 50);
  assert_eq!(p2.get(b"k049").unwrap().unwrap(), b"v49");
}

#[test]
fn background_flusher_persists_without_persist() {
  let store = MemoryStore::new();
  let cfg = window_cfg("gc/auto", 40);
  let eng = ObjectLsm::open(Arc::new(store.clone()), cfg).unwrap();
  let p = eng.partition("data").unwrap();
  for i in 0..20u32 {
    p.insert(format!("k{i:03}").as_bytes(), b"v").unwrap();
  }
  // Wait past the flush window; the background thread must have flushed.
  thread::sleep(Duration::from_millis(200));
  let objs = store.list(&journal_prefix("gc/auto")).unwrap();
  assert!(
    !objs.is_empty(),
    "background flusher should have written journal objects"
  );
  drop(p);
  drop(eng);

  let eng2 = ObjectLsm::open(Arc::new(store.clone()), window_cfg("gc/auto", 40)).unwrap();
  let p2 = eng2.partition("data").unwrap();
  assert_eq!(p2.len().unwrap(), 20);
}

#[test]
fn grouped_journal_survives_memtable_flush_and_reopen() {
  let store = MemoryStore::new();
  let cfg = window_cfg("gc/flush", 5).max_memtable_bytes(512);
  let eng = ObjectLsm::open(Arc::new(store.clone()), cfg).unwrap();
  let a = eng.partition("a").unwrap();
  let b = eng.partition("b").unwrap();
  for i in 0..80u32 {
    a.insert(format!("a{i:03}").as_bytes(), format!("va{i}").as_bytes())
      .unwrap();
  }
  for i in 0..10u32 {
    b.insert(format!("b{i:03}").as_bytes(), format!("vb{i}").as_bytes())
      .unwrap();
  }
  eng.persist().unwrap();
  assert!(
    a.table_count() >= 1,
    "partition a should have flushed to segments"
  );
  drop(a);
  drop(b);
  drop(eng);

  let eng2 = ObjectLsm::open(
    Arc::new(store.clone()),
    window_cfg("gc/flush", 5).max_memtable_bytes(512),
  )
  .unwrap();
  let a2 = eng2.partition("a").unwrap();
  let b2 = eng2.partition("b").unwrap();
  assert_eq!(a2.len().unwrap(), 80);
  assert_eq!(b2.len().unwrap(), 10);
  assert_eq!(a2.get(b"a042").unwrap().unwrap(), b"va42");
  assert_eq!(b2.get(b"b007").unwrap().unwrap(), b"vb7");
}

#[test]
fn strict_mode_keeps_one_object_per_commit() {
  let store = MemoryStore::new();
  let cfg = Config::new("gc/strict")
    .max_memtable_bytes(1 << 20)
    .journal_window_ms(None);
  let eng = ObjectLsm::open(Arc::new(store.clone()), cfg).unwrap();
  let p = eng.partition("data").unwrap();
  for i in 0..5u32 {
    p.insert(format!("k{i}").as_bytes(), b"v").unwrap();
  }
  let objs = store.list(&journal_prefix("gc/strict")).unwrap();
  assert_eq!(
    objs.len(),
    5,
    "strict mode writes one durable object per commit"
  );
}
