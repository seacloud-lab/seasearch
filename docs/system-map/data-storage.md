# Data and Storage

> **Purpose:** Map persistent data models, physical storage, caches, WAL, and ownership boundaries.
> **Last verified:** 2026-10-12
> **Verified against commit:** `f823d568efdf005582209d51df919f037ad26539`
> **Relevant source paths:** `pkg/meta/`, `pkg/metadata/`, `pkg/core/`, `pkg/bluge/directory/`, `pkg/lru_cache/`, `pkg/wal/`

## State Ownership

| State | Owner/location | Evidence |
|---|---|---|
| Index/user/role/template/alias metadata | Bolt/Badger standalone; etcd cluster | `pkg/metadata/storage.go` `InitMetaStorage` |
| Cluster nodes/heartbeats/assignments | etcd through external `clusterkit` | `pkg/cluster/cluster.go`; `cmd/cluster-manager/manager.go` |
| Text segments | Disk, S3, or OSS | `pkg/core/newindex.go` `getOpenConfig`; `pkg/bluge/directory/` |
| Vector stored records and ANN index files | Same backend class under `vec_index` | `pkg/core/vector/vec_storage.go` `GetVectorStorage`, `ObjStore` |
| Runtime indexes/readers/writers | Compute-process memory | `pkg/core/indexlist.go` `IndexList`; `pkg/core/searcher.go` |
| Runtime vector indexes | Independent compute-process `VecIndexManager.cache`, keyed by index/field | `pkg/core/vec_index_manager.go` `GetVectorIndex`, `CloseVectorIndex`, `execGC` |
| Remote-object cache | Per-compute-node local disk | `pkg/lru_cache/cache_manager.go` `Init`, `LruCache` |
| WAL | Disk by default, optional MySQL/PostgreSQL | `pkg/wal/wal.go` `Open`; `pkg/config/config.go` WAL constants |

[Architecture](architecture.md) owns deployment/storage prerequisites, [Processes](processes.md) owns initialization, and [Operations](operations.md) owns assignment/failover. A process-local cache is not a cluster coherence or ownership mechanism.

## Metadata

`pkg/metadata/storage/storage.go` `Storager` provides individual CRUD and prefix listing. It has no transaction/CAS API. Typed repositories live in `pkg/metadata/index.go`, `user.go`, `role.go`, `template.go`, `alias.go`, and `kv.go`.

Logical keys are:

| Data | Relative key |
|---|---|
| Index | `/index/<name>` |
| User | `/user/<id>` |
| Role | `/role/<id>` |
| Template | `/template/<id>` |
| Generic KV/version | `/kv/<key>`; version at `/kv/version` |
| All aliases | `/aliases/alias` singleton |

Key constructors are `index.key`, `user.key`, `role.key`, `template.key`, and `kv.key` in the corresponding `pkg/metadata/*.go` files; the alias singleton is written by `pkg/metadata/alias.go` `alias.Set`.

In cluster mode, these sit under `${SS_ETCD_PREFIX}/metadata` (`pkg/metadata/storage.go` `InitMetaStorage`). Standalone files are `${SS_DATA_PATH}/_metadata.bolt` or `_metadata.db` (`pkg/metadata/storage/bolt/`; `pkg/metadata/storage/badger/`).

The persisted index model is `pkg/meta/index.go` `Index`: settings, mappings, first/second shards, stats, version, storage type, and vector indexes. `metadata.Index.List` has a v0.2.6 conversion fallback while `metadata.Index.Get` does not; old metadata behavior requires source verification (`pkg/metadata/index.go`; `pkg/upgrade/v026t027.go`).

## Text Index Layout

New indexes set `StoreWithHash=true`. `core.Index.GetStoreName` hashes an index name to fixed-length base62 and splits after two characters; legacy `StoreWithHash=false` retains the plain index name. Physical shard paths append the first-shard ID and a six-digit hexadecimal second-shard ID (`pkg/core/newindex.go` `NewIndex`; `pkg/core/index.go` `GetStoreName`; `pkg/core/index_shards.go` `IndexShard.openWriter`, `IndexShard.openReader`).

```text
<index-store>/<first-shard-id>/<second-shard-id:06x>/Bluge objects
```

Disk prefixes this with `SS_DATA_PATH`. S3/OSS use the logical path as an object prefix and stage/cache files beneath `SS_DATA_PATH` (`pkg/bluge/directory/disk.go` `GetDiskConfig`; `pkg/bluge/directory/s3.go`; `pkg/bluge/directory/oss.go`).

First-layer shards route documents via rendezvous hashing and must remain stable. Second-layer shards are appended when the latest exceeds `ZINC_SHARD_MAX_SIZE`; new inserts go to the latest shard, while updates/deletes can still modify an older shard containing that document (`pkg/core/index_shards.go` `Index.GetShardByDocID`, `IndexShard.CheckShards`, `IndexShard.NewShard`; `pkg/core/index_document.go` `CreateDocument`, `DeleteDocument`; `pkg/core/index_shards_wal.go` `walMergeDocs.WriteToShard`).

An index records its backend. `getOpenConfig` rejects opening it when persisted and process-wide storage types differ (`pkg/core/newindex.go` `getOpenConfig`).

## Vector Storage

Vector mappings create `meta.VecIndex` records with type `flat`, `ivf_pq`, or `hnsw`, dimensions, tuning values, layout version, and segment metadata (`pkg/core/index.go` `SetMappings`; `pkg/meta/index.go` `VecIndex`). Vector field names are hashed for physical paths when vector `StoreWithHash=true`; legacy false retains the field name (`pkg/core/vec_index.go` `MakeVecIndex`).

All vector data is rooted under `vec_index/<index-store>/<field-store>/`:

| Data | Shape | Evidence |
|---|---|---|
| Stored vectors | `<segment:04x>/stored_vec/<Bluge object>` | `pkg/core/ivfpq_segment.go`; `pkg/core/hnsw_segment.go` |
| IVFPQ FAISS index | `<segment:04x>/faiss/index.index` | `pkg/core/ivfpq_segment.go`; `pkg/core/vector/vec_storage.go` `ObjStore` |
| HNSW snapshot | `0000/hnsw/index.index` | `pkg/core/hnsw_segment.go` |

Flat exhaustively searches stored vectors. IVFPQ writes stored vectors to a growing segment, rolls at `SS_VECTOR_IVFPQ_THRESHOLD`, and seals old segments into FAISS indexes in background (`pkg/core/vec_index.go` `IvfPqIndex.Batch`, `IvfPqIndex.checkNewSeg`; `pkg/core/vec_index_manager.go` `backgroundSealCheck`, `SealIndex`, `backgroundSeal`). Flat/IVFPQ stored records can use float16 when `StoreWithFloat16` is enabled; decoding back to float32 does not recover original precision (`pkg/core/ivfpq_segment.go` `addVectors`, `getVectors`; `pkg/zutils/vector_bytes.go` `VectorToFloat16Bytes`, `Float16BytesToVector`).

HNSW stores vector change logs and materialized document vectors in Bluge. Crossing the unapplied-log threshold queues a USearch snapshot rebuild; search merges pending exact vectors with HNSW candidates and recomputes exact distances (`pkg/core/vec_index.go` `HNSWIndex.Batch`; `pkg/core/hnsw_segment.go` `HNSWSegment.NeedRebuildHNSW`, `HNSWSegment.Search`; `pkg/core/vec_index_manager.go` `BuildHNSWIndex`, `backgroundBuildHNSWIndex`).

Here, "exact" means distance computation against stored records, not guaranteed ANN recall or original-vector precision. Sealed IVFPQ and HNSW rerank only retrieved ANN candidates, so missed neighbors remain missed; HNSW log/document records use float32 serialization (`pkg/core/ivfpq_segment.go` `searchIvfPq`; `pkg/core/hnsw_segment.go` `Batch`, `searchHNSW`; `pkg/zutils/vector_bytes.go` `VectorToBytes`). See [Vector Search](request-flows.md#vector-search) for filter/projection differences across owner and distributed paths.

`VecIndexManager` caches runtime vector objects independently of text `IndexList` and the disk object cache. A cache hit increments references without rereading metadata; `CloseVectorIndex(..., false)` only releases a reference, while `wait=true` evicts/frees once references reach zero. `execGC` frees unreferenced objects idle over an hour. Text unassignment/close alone does not evict these objects (`pkg/core/vec_index_manager.go` `GetVectorIndex`, `getVectorIndex`, `CloseVectorIndex`, `execGC`; `pkg/core/indexlist.go` `updateIndexList`).

## Local Object Cache

`pkg/lru_cache/cache_manager.go` `Init` enables a process-global cache only for non-disk storage. It tracks Bluge segment/snapshot and `.index` files, removes `.temp` files at startup, and evicts unreferenced oldest files toward 70% capacity when the recorded size reaches `SS_MAX_OBJ_CACHE_SIZE`.

Capacity is a soft bound: the check occurs before adding a file and referenced files cannot be evicted (`pkg/lru_cache/cache_manager.go` `LruCache.makeRoomForCacheFile`). Separately, `pkg/bluge/directory/keycache.go` caches object listings without TTL or cross-node invalidation. There **is** local invalidation: `KeyCacheClient.Write`, `Remove`, and `Copy` invalidate matching cached prefixes (including in-flight listings), and S3/OSS Bluge backends install this wrapper. That does not refresh another node's listings or same-path cached file contents; reassignment and overwritten vector snapshots require runtime verification (`pkg/bluge/directory/s3.go` `createS3Client`; `pkg/bluge/directory/oss.go` `createOssClient`; `pkg/bluge/directory/keycache.go` `Invalidate`, `KeyCacheClient`).

## WAL and Redo

With WAL disabled, validated operations commit directly and metadata updates are queued asynchronously (`pkg/core/index_document.go` `CommitOperations`, `InitAsyncMetaDataUpdate`, `updateMetadataProcess`, `CloseAsyncMetaDataUpdate`). With WAL enabled, each first shard lazily opens a WAL; request success acknowledges append (`Log.Write`), not indexing completion or per-request `Sync`. Disk `Open` sets `NoSync: true`; SQL implementations execute an INSERT and depend on database durability configuration (`pkg/wal/wal.go` `Open`, `Log.Write`; `pkg/wal/mysql/wal.go` `Wal.Write`; `pkg/wal/pgsql/wal.go` `Wal.Write`).

`ConsumeWAL` calls `Sync` (logs failures but continues), reads/merges entries, records read redo ranges, applies text/vector batches, records completed write ranges, truncates, and updates shard metadata. After shard consumption, the WAL-list scheduler attempts to persist index metadata, then updates WAL-size stats in memory (`pkg/core/index_shards_wallist.go` `IndexShardWALList.ConsumeWAL`; `pkg/core/index.go` `UpdateMetadata`, `UpdateWALSize`). This is not a transaction across WAL, redo, text, vectors, and metadata (`pkg/core/index_shards_wal.go` `ConsumeWAL`, `WriteToShard`). `Rollback` handles mismatched redo ranges with limited cleanup: `WriteToShardRollback` deletes insert-first text records for nonnegative shard IDs, skips updates/deletes and negative shard IDs, and never undoes vectors. It is not restoration of prior document state.

Direct commit success means repository batch calls completed, not a universal snapshot-publication/fsync guarantee. Read-after-write timing and crash recovery require verification of the pinned Bluge/WAL/native dependencies, object-store behavior, and caches in the actual deployment (`pkg/core/index_shards.go` `openReader`, `CloseWriter`; `pkg/core/newindex.go` `OpenIndexWriter`; [Request Flows](request-flows.md#document-write)). [Configuration](configuration.md) owns WAL/backend settings.

Disk WAL paths use the plain index/shard name, not the hashed text store:

```text
${SS_DATA_PATH}/<plain-index-name>/<first-shard-id>/wal/
${SS_DATA_PATH}/<plain-index-name>/<first-shard-id>/redo
```

MySQL/PostgreSQL schemas are in `pkg/wal/mysql/create.sql` and `pkg/wal/pgsql/create.sql`. `rkv` passes configuration validation but its `pkg/wal/wal.go` `Open` branch is TODO and should be treated as unimplemented.

## Consistency and Deletion

- `core.DeleteIndex` deletes vector data, text data, runtime state, then metadata; there is no transaction spanning these resources (`pkg/core/deleteindex.go` `DeleteIndex`).
- Hashed-index deletion does not explicitly remove the separate plain-name disk WAL/redo tree, nor SQL WAL rows. Exception: legacy disk indexes with `StoreWithHash=false` delete `${SS_DATA_PATH}/<plain-index-name>` recursively, which includes their WAL/redo paths (`pkg/core/deleteindex.go` `delIndex`; `pkg/core/index.go` `GetStoreName`; `pkg/wal/wal.go` `Open`).
- Object/disk directory `Lock`/`Unlock` methods are no-ops and do not fence stale writers; ownership is an operational invariant (`pkg/bluge/directory/disk.go` `fileSystemDirectory.Lock`; `s3.go` `S3Backend.Lock`; `oss.go` `OssBackend.Lock`).
- Local disk WAL is not failover-shared. SQL WAL is shared but has no visible ownership epoch/fencing in this repository.
- `formatIndex` is common runtime reconstruction for normal owner loading, proxy metadata views, and internal query nodes, not just parallel search. It explicitly copies selected index/shard fields and rebuilds wrappers, hashing, and analyzers; verify field preservation for all callers when changing metadata schema (`pkg/core/loadindexes.go` `formatIndex`, `LoadZincIndexFromMetadata`, `GetZincIndexesFromMetadata`, `GetZincIndexFromMetadata`).
