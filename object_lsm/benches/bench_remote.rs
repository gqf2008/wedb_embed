//! Backend comparison benchmark: wedb_embed::Fjall (local disk) vs ObjectLsm
//! over the in-memory Store vs ObjectLsm over Cloudflare R2.
//!
//! Run:
//! ```sh
//! cargo bench -p wedb_object_lsm --features "r2 wedb" --bench bench_remote
//! # R2 benches require R2_* env; without them only local backends run
//! ```
//!
//! Timings are measured with std::time at engine level (Engine/Partition):
//! - insert: one commit per key (ObjectLsm commits are durable R2 PUTs; fjall
//!   writes into its WAL with manual journal persist semantics)
//! - point reads after a flush/compact (block cache warm)
//! - full scans

#![cfg(all(feature = "r2", feature = "wedb"))]

use std::{sync::Arc, time::Instant};

use tempfile::tempdir;
use wedb_embed::Fjall;
use wedb_embed_engine::{Engine, Partition};
use wedb_object_lsm::{Config, MemoryStore, ObjectLsm, R2Store, Store};

fn env_ok() -> bool {
  ["R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"]
    .iter()
    .all(|k| std::env::var(k).map(|v| !v.is_empty()).unwrap_or(false))
}

fn n_default() -> usize {
  std::env::var("BENCH_N")
    .ok()
    .and_then(|s| s.parse().ok())
    .unwrap_or(200)
}

/// Optional group-commit window from `OBJLSM_WINDOW_MS` (None = strict).
fn window_ms() -> Option<u64> {
  std::env::var("OBJLSM_WINDOW_MS")
    .ok()
    .and_then(|s| s.parse().ok())
}

fn label(name: &str) -> String {
  match window_ms() {
    Some(ms) => format!("{name} (grouped {ms}ms)"),
    None => name.to_string(),
  }
}

fn mk_cfg(prefix: &str) -> Config {
  Config::new(prefix)
    .max_memtable_bytes(1 << 20)
    .block_size(4096)
    .journal_window_ms(window_ms())
}

fn key(i: usize) -> Vec<u8> {
  format!("bench-key-{i:08}").into_bytes()
}

const VAL: &[u8] = b"payload-64-bytes-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

fn report(name: &str, what: &str, n: usize, dur: std::time::Duration) {
  let per_ns = dur.as_nanos() as f64 / n as f64;
  let per_us = per_ns / 1000.0;
  println!(
    "{name:>22} {what:<22} {n:>7} ops  total {:>10.1?}  per-op {:>10.2} us",
    dur, per_us
  );
}

fn bench_engine<E: Engine>(name: &str, eng: &E, n: usize) {
  let p = eng.partition("bench").expect("partition");
  // insert (commit per key)
  let t = Instant::now();
  for i in 0..n {
    p.insert(&key(i), VAL).expect("insert");
  }
  report(name, "insert (commit)", n, t.elapsed());

  // flush / compact so reads hit durable segments
  let t = Instant::now();
  eng.compact().expect("compact");
  report(name, "compact/flush", n, t.elapsed());

  // point reads (warm cache)
  let t = Instant::now();
  for i in 0..n {
    assert!(p.get(&key(i)).expect("get").is_some());
  }
  report(name, "point read (warm)", n, t.elapsed());

  // full scan
  let t = Instant::now();
  let mut count = 0usize;
  for e in p.iter() {
    let _ = e.expect("iter");
    count += 1;
  }
  report(name, "scan entries", count, t.elapsed());
}

fn main() {
  let n = n_default();
  println!("ObjectLsm backend comparison  (n = {n} keys)");
  println!();

  // fjall: local disk
  let dir = tempdir().expect("tempdir");
  let fj = Fjall::open(dir.path()).expect("fjall open");
  bench_engine("fjall (local disk)", &fj, n);

  // objectlsm over in-memory store
  let store = MemoryStore::new();
  let mem = ObjectLsm::open(Arc::new(store), mk_cfg("bench/remote/mem")).expect("obj mem open");
  bench_engine(&label("objectlsm (memory)"), &mem, n);

  // objectlsm over Cloudflare R2
  if !env_ok() {
    println!("R2 env not configured; skipping R2 backend benches");
    return;
  }
  let r2store = Arc::new(R2Store::from_env().expect("r2 store"));
  let r2 = ObjectLsm::open(r2store.clone(), mk_cfg("wedb_bench/remote")).expect("obj r2 open");
  bench_engine(&label("objectlsm (R2)"), &r2, n);

  // cleanup R2 prefix (best effort)
  for key in r2store.list("wedb_bench/remote").expect("list") {
    let _ = r2store.delete(&key);
  }
}
