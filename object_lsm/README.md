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
- [ ] M4 compatibility harness vs `wedb_embed` tests + benchmarks
- [ ] R2/S3 remote `Store` backend (feature-gated, needs credentials)

## Test

```sh
cargo test -p wedb_object_lsm
```


