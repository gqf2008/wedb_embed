#![cfg(feature = "wedb")]
//! M4 compatibility harness: run identical wedb_embed Redis-style scripts
//! against Fjall (reference) and ObjectLsm, asserting byte-identical results,
//! plus reopen persistence for ObjectLsm.

use std::{fmt::Debug, sync::Arc};

use tempfile::tempdir;
use wedb_embed::{Db, Fjall, StreamId, WeDb};
use wedb_object_lsm::{Config, MemoryStore, ObjectLsm};

fn object_db(prefix: &str) -> (Db<ObjectLsm>, MemoryStore) {
  let store = MemoryStore::new();
  // Small memtable + small blocks + low compact threshold: forces flushing,
  // multi-block segments and merge compaction during the script.
  let cfg = Config::new(prefix)
    .max_memtable_bytes(1024)
    .block_size(256)
    .max_segments_before_compact(6);
  let engine = ObjectLsm::open(Arc::new(store.clone()), cfg).expect("open objectlsm");
  let db = WeDb::new(engine).ns(0).expect("ns").db(0).expect("db");
  (db, store)
}

fn rec<T: Debug>(log: &mut Vec<String>, tag: &str, r: wedb_embed::Result<T>) {
  log.push(format!("{tag}={r:?}"));
}

fn script<E>(db: &Db<E>) -> wedb_embed::Result<Vec<String>>
where
  E: wedb_embed::Engine,
  wedb_embed::Error: From<E::Error>,
{
  let mut log = Vec::new();

  // --- string ----------------------------------------------------------
  rec(&mut log, "set1", db.set("s1", "hello", []));
  rec(&mut log, "get1", db.get("s1"));
  rec(
    &mut log,
    "set2",
    db.set("s1", "world", [wedb_embed::Set::Nx]),
  );
  rec(&mut log, "get2", db.get("s1"));
  rec(&mut log, "append", db.append("s1", "!!"));
  rec(&mut log, "strlen", db.strlen("s1"));
  rec(&mut log, "getrange", db.getrange("s1", (0, 4)));
  rec(&mut log, "incr", db.incr("cnt"));
  rec(&mut log, "incrby", db.incrby("cnt", 10));
  rec(&mut log, "decr", db.decr("cnt"));
  rec(&mut log, "mset", db.mset(&[("m1", "a"), ("m2", "b")]));
  rec(&mut log, "mget", db.mget(&["m1", "m2", "nope"]));
  rec(&mut log, "del1", db.del(&["m1", "m2", "nope"]));
  rec(&mut log, "del2", db.del(&["s1"]));
  rec(&mut log, "get_del", db.get("s1"));

  // --- cross-type WRONGTYPE --------------------------------------------
  rec(&mut log, "set_ctype", db.set("conflict", "str", []));
  rec(
    &mut log,
    "hget_ctype_err",
    Ok(db.hget("conflict", "f").is_err()),
  );
  rec(&mut log, "llen_ctype_err", Ok(db.llen("conflict").is_err()));
  rec(
    &mut log,
    "zscore_ctype_err",
    Ok(db.zscore("conflict", b"m").is_err()),
  );

  // --- hash ------------------------------------------------------------
  rec(
    &mut log,
    "hset1",
    db.hset("h1", &[("f1", "a"), ("f2", "b"), ("f3", "c")]),
  );
  rec(&mut log, "hset2", db.hset("h1", &[("f1", "a2")]));
  rec(&mut log, "hget1", db.hget("h1", "f1"));
  rec(&mut log, "hexists1", db.hexists("h1", "f1"));
  rec(&mut log, "hexists2", db.hexists("h1", "zz"));
  rec(&mut log, "hlen1", db.hlen("h1"));
  rec(&mut log, "hkeys1", db.hkeys("h1"));
  rec(&mut log, "hvals1", db.hvals("h1"));
  rec(&mut log, "hgetall1", db.hgetall("h1"));
  rec(&mut log, "hincrby1", db.hincrby("h1", "n", 5));
  rec(&mut log, "hmget1", db.hmget("h1", &["f2", "zz"]));
  rec(&mut log, "hdel1", db.hdel("h1", &["f3", "zz"]));
  rec(&mut log, "hlen2", db.hlen("h1"));

  // --- list ------------------------------------------------------------
  rec(&mut log, "rpush1", db.rpush("l1", &["a", "b", "c"]));
  rec(&mut log, "lpush1", db.lpush("l1", &["z"]));
  rec(&mut log, "llen1", db.llen("l1"));
  rec(&mut log, "lrange1", db.lrange("l1", (0, -1)));
  rec(&mut log, "lrange2", db.lrange("l1", (0, 1)));
  rec(&mut log, "lindex1", db.lindex("l1", 1));
  rec(&mut log, "lpop1", db.lpop("l1", 1));
  rec(&mut log, "rpop1", db.rpop("l1", 2));
  rec(&mut log, "llen2", db.llen("l1"));

  // --- set -------------------------------------------------------------
  rec(&mut log, "sadd1", db.sadd("st1", &["x", "y", "z"]));
  rec(&mut log, "sadd2", db.sadd("st1", &["y", "w"]));
  rec(&mut log, "scard1", db.scard("st1"));
  rec(&mut log, "sismember1", db.sismember("st1", "x"));
  rec(&mut log, "sismember2", db.sismember("st1", "nope"));
  rec(&mut log, "smembers1", db.smembers("st1"));
  rec(&mut log, "srem1", db.srem("st1", &["y", "nope"]));
  rec(&mut log, "smembers2", db.smembers("st1"));

  // --- zset ------------------------------------------------------------
  rec(
    &mut log,
    "zadd1",
    db.zadd(
      "z1",
      &[
        (1.0, b"a".as_slice()),
        (3.0, b"c".as_slice()),
        (2.0, b"b".as_slice()),
      ],
      [],
    ),
  );
  rec(
    &mut log,
    "zadd2",
    db.zadd("z1", &[(2.5, b"b".as_slice())], []),
  );
  rec(&mut log, "zcard1", db.zcard("z1"));
  rec(&mut log, "zscore1", db.zscore("z1", b"b"));
  rec(&mut log, "zscore2", db.zscore("z1", b"zz"));
  rec(&mut log, "zrange1", db.zrange("z1", b"0", b"-1", []));
  rec(&mut log, "zrank1", db.zrank("z1", b"c"));
  rec(&mut log, "zrem1", db.zrem("z1", &[b"a".as_slice()]));
  rec(&mut log, "zrange2", db.zrange("z1", b"0", b"-1", []));

  // --- bitmap ----------------------------------------------------------
  rec(&mut log, "setbit1", db.setbit("bm1", 0, 1));
  rec(&mut log, "setbit2", db.setbit("bm1", 10, 1));
  rec(&mut log, "setbit3", db.setbit("bm1", 0, 0));
  rec(&mut log, "getbit1", db.getbit("bm1", 0));
  rec(&mut log, "getbit2", db.getbit("bm1", 10));
  rec(&mut log, "bitcount1", db.bitcount("bm1", []));

  // --- stream ----------------------------------------------------------
  rec(
    &mut log,
    "xadd1",
    db.xadd(
      "st1",
      Some(StreamId::new(1000, 0)),
      &[("sensor", "1"), ("temp", "25")],
    ),
  );
  rec(
    &mut log,
    "xadd2",
    db.xadd(
      "st1",
      Some(StreamId::new(1000, 1)),
      &[("sensor", "2"), ("temp", "26")],
    ),
  );
  rec(
    &mut log,
    "xadd3",
    db.xadd(
      "st1",
      Some(StreamId::new(2000, 0)),
      &[("sensor", "3"), ("temp", "27")],
    ),
  );
  rec(
    &mut log,
    "xadd_bad",
    Ok(
      db.xadd("st1", Some(StreamId::new(1500, 0)), &[("k", "v")])
        .is_err(),
    ),
  );
  rec(&mut log, "xlen1", db.xlen("st1"));
  rec(&mut log, "xlast", db.xlast_id("st1"));
  rec(
    &mut log,
    "xrange1",
    db.xrange("st1", (StreamId::new(1000, 0), StreamId::new(2000, 0))),
  );
  rec(
    &mut log,
    "xrev1",
    db.xrevrange("st1", (StreamId::max(), StreamId::min(), 2)),
  );
  rec(&mut log, "xdel1", db.xdel("st1", &[StreamId::new(1000, 1)]));
  rec(&mut log, "xlen2", db.xlen("st1"));

  // --- namespaces / multi-db ------------------------------------------
  let other = db.wedb().ns(7)?.db(0)?;
  rec(&mut log, "set_ns7", other.set("ns7key", "v", []));
  rec(&mut log, "get_ns7", other.get("ns7key"));
  rec(&mut log, "get_default_ns7", db.get("ns7key"));
  rec(&mut log, "rm_ns7", db.wedb().ns(7)?.rm());
  rec(&mut log, "get_ns7_after_rm", other.get("ns7key"));

  Ok(log)
}

#[test]
fn parity_with_fjall() -> wedb_embed::Result<()> {
  let dir = tempdir()?;
  let fj = WeDb::new(Fjall::open(dir.path())?).ns(0)?.db(0)?;
  let fjall_log = script(&fj)?;

  let (obj, _store) = object_db("compat/parity");
  let object_log = script(&obj)?;

  assert_eq!(
    fjall_log, object_log,
    "ObjectLsm diverged from Fjall reference"
  );
  // dbsize is engine-approximate (Fjall counts physical history, ObjectLsm
  // counts live keys); only sanity-check that both see a populated catalog.
  assert!(fj.wedb().dbsize()? >= 1);
  assert!(obj.wedb().dbsize()? >= 1);
  Ok(())
}

fn write_batch(db: &Db<ObjectLsm>) -> wedb_embed::Result<()> {
  db.set("p_str", "persisted", [])?;
  db.rpush("p_list", &["a", "b", "c"])?;
  db.hset("p_hash", &[("f1", "v1"), ("f2", "v2")])?;
  db.zadd(
    "p_zset",
    &[(1.0, b"m1".as_slice()), (2.0, b"m2".as_slice())],
    [],
  )?;
  db.xadd("p_stream", Some(StreamId::new(1, 0)), &[("k", "v")])?;
  Ok(())
}

#[test]
fn objectlsm_reopen_persistence() -> wedb_embed::Result<()> {
  let (db, store) = object_db("compat/reopen");
  write_batch(&db)?;
  drop(db);

  let engine = ObjectLsm::open(
    Arc::new(store),
    Config::new("compat/reopen")
      .max_memtable_bytes(1024)
      .block_size(256)
      .max_segments_before_compact(6),
  )
  .expect("reopen");
  let db2 = WeDb::new(engine).ns(0)?.db(0)?;

  assert_eq!(db2.get("p_str")?, Some(b"persisted".to_vec()));
  assert_eq!(
    db2.lrange("p_list", (0, -1))?,
    vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
  );
  assert_eq!(
    db2.hgetall("p_hash")?,
    vec![
      (b"f1".to_vec(), b"v1".to_vec()),
      (b"f2".to_vec(), b"v2".to_vec())
    ]
  );
  assert_eq!(
    db2.zrange("p_zset", b"0", b"-1", [])?,
    vec![(b"m1".to_vec(), 1.0), (b"m2".to_vec(), 2.0)]
  );
  assert_eq!(db2.xlen("p_stream")?, 1);
  Ok(())
}
