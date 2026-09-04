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
cargo test -p wedb_object_lsm              # engine tests (58)
cargo test -p wedb_object_lsm --features wedb   # + wedb_embed parity harness (2)
cargo bench -p wedb_object_lsm             # point read / scan / insert benches
```

The `wedb` feature adds an optional `wedb_embed` backend bridge plus the M4
compatibility harness: identical Redis-style scripts (string/hash/list/set/
zset/bitmap/stream/geo/json/hll/bloom/cuckoo/tdigest/sortedint/timeseries/
key-expiry + cross-type errors + namespaces) run against both `Fjall`
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

Observed (Cloudflare R2 public internet from CN, re-measured 2026-09-03; local
rows N=200, R2 strict N=40, R2 grouped N=200):

| backend | insert (commit) | point read (warm) | scan (warm) |
| --- | --- | --- | --- |
| fjall (local disk) | ~0.9 µs | ~0.3 µs | ~0.2 µs |
| objectlsm (memory) | ~1.6 µs | ~0.7 µs | ~0.2 µs |
| objectlsm (file, grouped 20 ms) | ~0.8 µs | ~1.7 µs | ~0.1 µs |
| objectlsm (R2, strict) | ~0.26 s /op (per-commit PUT) | ~4 ms (Range GET) | µs (cache warm) |
| objectlsm (R2, grouped 25 ms) | ~0.9 µs ack + flush | ~8.9 ms (Range GET) | µs (cache warm) |

Group-commit (`Config::journal_window_ms(Some(ms))`) turns queued commits into
one journal object per window. For 40 durable commits the measured insert
wall time dropped from ~10.2 s (strict, one PUT per commit) to µs-level acks
in grouped mode; the flush/compact step was ~10.6 s strict vs ~1.5 s grouped
(N=200). Ack in grouped mode means "buffered"; durability
is reached at the next flush (`persist()` forces one, so call it before
shutdown or whenever you need a sync point), matching an AOF-every-N-ms
trade-off. Warm cached reads/scans stay µs-level on R2.

Run: `OBJLSM_WINDOW_MS=25 BENCH_N=200 cargo bench -p wedb_object_lsm --features "r2 wedb" --bench bench_remote`

### R2 scale check (grouped 25 ms, 2026-09-04)

| keys | insert wall | insert/op | compact wall | warm read/op | object state after compact | reopen |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| 1,000 | 0.85 ms ack | 0.84 µs | 1.8 s | 3.39 ms | 1 segment, 0 journal, disk 95.5 KB | all keys recovered in 0.69 s |
| 10,000 | 38.1 s | 3.81 ms | 45.5 s | 3.33 ms | 1 segment, 0 journal, disk 951 KB | all keys recovered in 1.21 s |

The 10k run is a **soak-scale check**, not a claim of maximum throughput: under
this configuration the journal flusher periodically blocks memtable flushes on
remote R2 PUTs, making wall time non-linear. The important reliability results
are stable: journal GC reaches zero objects, compaction folds the dataset to a
single segment, cold reads stay low single-digit ms, and every key is recovered
after reopen. Reopen time grows sub-linearly with key count here (manifest /
segment metadata, not a full key replay).

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

Notes: stale-lease takeover and renewal use an atomic compare-and-swap
(`Store::put_if_matches`): the lease is replaced only if its current payload is
unchanged. `MemoryStore` compares bytes directly; `R2Store` maps it to S3
`If-Match` using the single-part object ETag (MD5). Each acquisition carries a
monotonic **fencing epoch** embedded in every journal group and in the
manifest; manifest publishing performs a conditional update of the `current`
pointer (CAS), so only one epoch's state is ever visible, and recovery ignores
journal groups from a different epoch. On writer handoff, unflushed
old-epoch journal groups are fenced off — call `compact`/`persist` before
releasing the lease to make state durable across a takeover. Lost-lease
detection remains heartbeat-based. Each shard is a separate engine instance —
cross-shard queries/cluster routing stay an application concern (as in Redis
Cluster).


## fjall-alignment notes

- `Partition::approximate_len` is O(#segments + memtable) — never a full
  partition scan — so `WeDb::dbsize`-style calls stay cheap; duplicates across
  segments intentionally overcount (approximate, like fjall's count).
- recovery skips whole journal objects whose end-seq is at/below every
  partition watermark, instead of decoding the entire history.
- the segment-index cache is FIFO-bounded (4096) so metadata memory cannot
  grow unboundedly with churn.
- dropping the engine (windowed group-commit) flushes the pending journal
  before stopping the background flusher, so a clean shutdown does not lose
  acknowledged writes.




## Local filesystem backend + process-level crash injection

`FileStore` (no feature gate) implements [`Store`] over a local directory:
objects are files under `root/key`, with byte-range reads, create-if-absent,
compare-and-swap and prefix listing. It is a test/reference backend (object
writes are not guaranteed to be atomic at the filesystem level).

`tests/crash.rs` spawns the test binary itself, performs a scenario, then calls
`std::process::abort()` at a precise durability step and reopens the same
directory in a fresh process to assert crash consistency:
journal-after-commit, flush-after-compact, segment-upload-before-manifest,
clear-publish-before-delete, compact-publish-before-delete.

