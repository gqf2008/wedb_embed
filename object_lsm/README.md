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




