# wedb_object_lsm

Object-storage-backed LSM storage engine implementing the
[`wedb_embed_engine`](../engine) `Engine` / `Partition` / `Batch` traits.

Designed as a drop-in backend for `wedb_embed` Redis-style APIs: replace
`WeDb::new(Fjall::open(dir)?)` with an engine whose data lives on
S3-compatible object storage (AWS S3 / Cloudflare R2 / MinIO).

## Architecture (M1 vertical slice)

```text
write:  Batch::commit -> immutable journal group object (atomic PUT)
                        -> apply to per-partition memtable
                        -> spill to immutable sorted segment objects
                        -> publish manifest (segment list + watermark)
read:   memtable hit (µs) -> block-indexed Range GET scans through block cache
crash:  current -> manifest -> replay journal groups newer than watermark

write-adjacent maintenance:
  - merge compaction folds a partition's segments into one (upload -> publish
    manifest -> delete old objects) once max_segments_before_compact is hit
  - applied journal groups (seq <= min partition watermark) are deleted
  - opening GCs segments not referenced by the current manifest and superseded
    manifest snapshots
```

- every committed group is a separate immutable journal object, so a PUT is the
  atomic durability point — no torn log tail;
- memtable flush advances the partition watermark; journal groups are replayed
  only when newer than the watermark;
- tombstones shadow older segments across flush / reopen;
- `clear` / `rm_partition` fold prior state into the watermark and mark dropped,
  so replayed journal groups can never resurrect deleted data.

## Milestones

- [x] M0 scaffold: crate + `Store` abstraction + in-memory store + codec
- [x] M1 vertical slice: memtable/journal/segment/manifest, recovery, Engine impl
- [x] M2 ordered streaming iteration, block-indexed segments + block cache
- [x] M3 compaction + orphan GC + journal GC
- [x] M4 compatibility harness vs `wedb_embed` tests + benchmarks
- [x] R2/S3 remote `Store` backend (feature `r2`, `object_store` sync bridge)

## Test

```sh
cargo test -p wedb_object_lsm              # engine tests (21)
cargo test -p wedb_object_lsm --features wedb   # + wedb_embed parity harness (23)
cargo bench -p wedb_object_lsm             # point read / scan / insert benches
```

The `wedb` feature adds an optional `wedb_embed` backend bridge plus the M4
compatibility harness: identical Redis-style scripts (string/hash/list/set/
zset/bitmap/stream + cross-type errors + namespaces) run against both `Fjall`
(reference) and `ObjectLsm`, asserting byte-identical result logs, and a
reopen-persistence test for `ObjectLsm`.

## Cloudflare R2 backend

`R2Store` (feature `r2`) implements the [`Store`] trait over Cloudflare R2 /
any S3-compatible endpoint via `object_store`, bridging async I/O with an
internal tokio runtime. Byte-range reads map to S3 Range GETs.

```sh
# credentials come from the environment (never commit them):
#   R2_BUCKET R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY
#   R2_ENDPOINT (or R2_ACCOUNT_ID to derive it)
cargo test -p wedb_object_lsm --features r2 --test r2   # live roundtrip; skips w/o env
```





## Backend comparison benchmark (R2 vs fjall local)

```sh
cargo bench -p wedb_object_lsm --features "r2 wedb" --bench bench_remote
# R2_* env required for the R2 rows; without them only local backends run
# BENCH_N overrides the key count (default 200)
```

Observed (Cloudflare R2 public internet from CN, n=120, per-op):

| backend | insert (commit) | point read (warm) | scan (warm) |
| --- | --- | --- | --- |
| fjall (local disk) | ~1 µs | ~0.3 µs | ~0.3 µs |
| objectlsm (memory) | ~1 µs | ~0.5 µs | ~0.2 µs |
| objectlsm (R2) | ~0.3 s (per-commit PUT) | ~9 ms (Range GET) | µs (block-cache warm) |

The R2 write cost is dominated by the per-commit journal object PUT. The next
engineering lever is group-commit journal batching (amortize many commits into
one PUT) plus larger segments, which is where ObjectLsm's S3 story gets its
throughput back on real object stores.

## Multi-instance shared bucket: lease + sharding

`ObjectLsm::open_leased(store, cfg, LeaseOptions)` makes an instance the
exclusive writer of `cfg.prefix`:

- acquisition is atomic create-if-absent on `<prefix>/lease`
  (`Store::create`; R2 uses `PutMode::Create`, MemoryStore is atomic);
- the owner renews via a background heartbeat (TTL/3); losing renewal marks
  the lease lost, so a crashed writer is taken over once its lease expires;
- the lease is released when the last engine handle drops.

Parallel writers share a bucket by sharding into disjoint prefixes:

```rust
let cfg0 = Config::for_shard("myapp/db", 0); // .../shard-0
let cfg1 = Config::for_shard("myapp/db", 1); // .../shard-1
ObjectLsm::open_leased(store.clone(), cfg0, lease("w0"))?; // independent writer
ObjectLsm::open_leased(store.clone(), cfg1, lease("w1"))?;
```

Notes: leases are cooperative (best-effort fencing); each shard is a separate
engine instance — cross-shard queries/cluster routing stay an application
concern (as in Redis Cluster).
