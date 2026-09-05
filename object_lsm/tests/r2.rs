//! Live Cloudflare R2 integration (feature `r2`).
//!
//! Reads `R2_BUCKET` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` and
//! `R2_ENDPOINT` (or `R2_ACCOUNT_ID`) from the environment; skips when absent.
//! Uses a unique prefix and cleans up after itself.

#![cfg(feature = "r2")]

use std::{
  sync::{Arc, Barrier},
  thread,
};

use wedb_embed_engine::{Engine, Partition};
use wedb_object_lsm::{Config, LeaseOptions, ObjectLsm, R2Store, Store, keys::segment_root};

fn env_ok() -> bool {
  ["R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"]
    .iter()
    .all(|k| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false))
}

fn prefix() -> String {
  let pid = std::process::id();
  let nanos = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap()
    .as_nanos();
  format!("wedb_test/r2e_{pid}_{nanos}")
}

#[test]
fn r2_engine_roundtrip() -> wedb_object_lsm::Result<()> {
  if !env_ok() {
    eprintln!("R2 env not configured; skipping live test");
    return Ok(());
  }
  let prefix = prefix();
  let cfg = Config::new(&prefix)
    .max_memtable_bytes(16 * 1024)
    .block_size(2048)
    .max_segments_before_compact(1_000_000);
  let store = Arc::new(R2Store::from_env()?);

  // phase 1: writes + flush + point reads + scan
  let eng = ObjectLsm::open(store.clone(), cfg.clone())?;
  let p = eng.partition("data")?;
  let n = 120u32;
  for i in 0..n {
    p.insert(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())?;
  }
  eng.compact()?; // flush memtable into one remote segment
  assert_eq!(p.table_count(), 1);
  for i in (0..n).step_by(5) {
    assert_eq!(
      p.get(format!("k{i:04}").as_bytes())?,
      Some(format!("v{i}").into_bytes())
    );
  }
  // overwrite every 3rd, delete every 7th
  let mut expected = 0u64;
  for i in 0..n {
    let k = format!("k{i:04}").into_bytes();
    if i % 7 == 0 {
      p.rm(&k)?;
    } else {
      expected += 1;
      if i % 3 == 0 {
        p.insert(&k, format!("v{i}-x").as_bytes())?;
      }
    }
  }
  eng.compact()?; // rewrite to a single compacted segment
  assert_eq!(p.len()?, expected as usize);
  assert_eq!(p.get(b"k0000")?, None, "deleted key must stay gone");

  // phase 2: reopen from R2 and verify persistence
  drop(eng);
  let store2 = Arc::new(R2Store::from_env()?);
  let eng2 = ObjectLsm::open(store2.clone(), cfg)?;
  let p2 = eng2.partition("data")?;
  assert_eq!(p2.len()?, expected as usize);
  for i in 1..n {
    if i % 7 != 0 {
      let want = if i % 3 == 0 {
        format!("v{i}-x")
      } else {
        format!("v{i}")
      };
      assert_eq!(
        p2.get(format!("k{i:04}").as_bytes())?,
        Some(want.into_bytes()),
        "key {i}"
      );
    }
  }
  let seg_objects = store2.list(&segment_root(&prefix))?;
  assert_eq!(
    seg_objects.len(),
    p2.table_count(),
    "segment object count matches manifest"
  );

  // cleanup: best-effort delete every object under the prefix
  for key in store2.list(&prefix)? {
    let _ = store2.delete(&key);
  }
  Ok(())
}

#[test]
fn r2_multiple_instances_concurrent() -> wedb_object_lsm::Result<()> {
  if !env_ok() {
    eprintln!("R2 env not configured; skipping live test");
    return Ok(());
  }
  let workers: usize = std::env::var("OBJLSM_CONCURRENCY")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(4);
  let writes: usize = std::env::var("OBJLSM_WRITES")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(120);
  let prefix = prefix();
  let barrier = Arc::new(Barrier::new(workers));
  let mut handles = Vec::with_capacity(workers);

  for w in 0..workers {
    let barrier = barrier.clone();
    let base = prefix.clone();
    handles.push(thread::spawn(move || -> wedb_object_lsm::Result<usize> {
      let store = Arc::new(R2Store::from_env()?);
      let cfg = Config::for_shard(&base, w as u64)
        .max_memtable_bytes(64 * 1024)
        .block_size(2048)
        .max_segments_before_compact(1_000_000);
      let eng = ObjectLsm::open(store.clone(), cfg.clone())?;
      let p = eng.partition("data")?;
      barrier.wait();
      for j in 0..writes {
        let k = format!("{w:02}-{j:05}");
        let v = format!("w{w}:{j}");
        p.insert(k.as_bytes(), v.as_bytes())?;
      }
      eng.compact()?;
      drop(eng);

      // Reopen from R2 and verify this writer's data survived.
      let eng2 = ObjectLsm::open(store, cfg)?;
      let p2 = eng2.partition("data")?;
      assert_eq!(p2.len()?, writes, "writer {w} lost keys");
      for j in (0..writes).step_by(7) {
        let k = format!("{w:02}-{j:05}");
        let v = format!("w{w}:{j}");
        assert_eq!(p2.get(k.as_bytes())?, Some(v.into_bytes()));
      }
      Ok(writes)
    }));
  }

  let mut total = 0usize;
  for h in handles {
    total += h
      .join()
      .map_err(|_| wedb_object_lsm::Error::store("writer panicked"))??;
  }
  assert_eq!(total, workers * writes);

  // Cleanup: best-effort delete every object under the shared base prefix.
  let store = Arc::new(R2Store::from_env()?);
  for key in store.list(&prefix)? {
    let _ = store.delete(&key);
  }
  Ok(())
}

fn lease_opts(owner: &str) -> LeaseOptions {
  LeaseOptions {
    owner: owner.into(),
    ttl: std::time::Duration::from_secs(300),
    timeout: std::time::Duration::from_millis(800),
    heartbeat: false,
  }
}

#[test]
fn r2_same_prefix_lease_fencing() -> wedb_object_lsm::Result<()> {
  if !env_ok() {
    eprintln!("R2 env not configured; skipping live test");
    return Ok(());
  }
  let prefix = prefix();
  let store = Arc::new(R2Store::from_env()?);

  let e0 = ObjectLsm::open_leased(
    store.clone(),
    Config::new(&prefix)
      .max_memtable_bytes(1 << 20)
      .journal_window_ms(Some(10)),
    lease_opts("w0"),
  )?;
  let p0 = e0.partition("data")?;
  for j in 0..5u32 {
    p0.insert(format!("w0-{j:02}").as_bytes(), b"v")?;
  }
  e0.compact()?; // fold into a manifest segment so the next epoch can read it

  // A second writer on the SAME prefix must be refused while w0 holds lease.
  let second = ObjectLsm::open_leased(
    store.clone(),
    Config::new(&prefix)
      .max_memtable_bytes(1 << 20)
      .journal_window_ms(Some(10)),
    lease_opts("w1"),
  );
  assert!(
    second.is_err(),
    "second writer should not acquire the same-prefix lease"
  );

  // Dropping w0 releases the lease; a new writer can take over and see data.
  drop(e0);
  drop(p0);
  let e1 = ObjectLsm::open_leased(
    store.clone(),
    Config::new(&prefix)
      .max_memtable_bytes(1 << 20)
      .journal_window_ms(Some(10)),
    lease_opts("w1"),
  )?;
  let p1 = e1.partition("data")?;
  assert_eq!(p1.len()?, 5, "takeover writer must see w0 data");
  for j in 0..5u32 {
    assert_eq!(
      p1.get(format!("w0-{j:02}").as_bytes())?,
      Some(b"v".to_vec())
    );
  }
  for j in 0..5u32 {
    p1.insert(format!("w1-{j:02}").as_bytes(), b"v")?;
  }
  e1.compact()?;
  drop(e1);
  drop(p1);

  // Reopen after takeover and verify all writers' data is durable.
  let store2 = Arc::new(R2Store::from_env()?);
  let e2 = ObjectLsm::open_leased(
    store2.clone(),
    Config::new(&prefix)
      .max_memtable_bytes(1 << 20)
      .journal_window_ms(Some(10)),
    lease_opts("w2"),
  )?;
  let p2 = e2.partition("data")?;
  assert_eq!(p2.len()?, 10);
  assert_eq!(p2.get(b"w0-00")?, Some(b"v".to_vec()));
  assert_eq!(p2.get(b"w1-04")?, Some(b"v".to_vec()));

  // Cleanup.
  for key in store2.list(&prefix)? {
    let _ = store2.delete(&key);
  }
  Ok(())
}
