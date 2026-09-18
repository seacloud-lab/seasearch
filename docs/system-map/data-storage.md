# Data and Storage

> **Purpose:** Map persistent data models, physical storage, caches, WAL, and ownership boundaries.
> **Last verified:** 2026-09-18
> **Verified against commit:** `c914b4cc84f87197c03009bafefc06f3f513700b`
> **Relevant source paths:** `pkg/meta/`, `pkg/metadata/`, `pkg/core/`, `pkg/bluge/directory/`, `pkg/lru_cache/`, `pkg/wal/`

## State Ownership

| State | Owner/location | Evidence |
|---|---|---|
| Index/user/role/template/alias metadata | Bolt/Badger standalone; etcd cluster | `pkg/metadata/storage.go` `InitMetaStorage` |
| Cluster nodes/heartbeats/assignments | etcd through external `clusterkit` | `pkg/cluster/cluster.go`; `cmd/cluster-manager/manager.go` |
| Text segments | Disk, S3, or OSS | `pkg/core/newindex.go` `getOpenConfig`; `pkg/bluge/directory/` |
| Vector exact records and index files | Same backend class under `vec_index` | `pkg/core/vector/vec_storage.go` `GetVectorStorage`, `ObjStore` |
| Runtime indexes/readers/writers | Compute-process memory | `pkg/core/indexlist.go` `IndexList`; `pkg/core/searcher.go` |
| Remote-object cache | Per-compute-node local disk | `pkg/lru_cache/cache_manager.go` `Init`, `LruCache` |
| WAL | Disk by default, optional MySQL/PostgreSQL | `pkg/wal/wal.go` `Open`; `pkg/config/config.go` WAL constants |

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

In cluster mode, these sit under `${SS_ETCD_PREFIX}/metadata` (`pkg/metadata/storage.go` `InitMetaStorage`). Standalone files are `${SS_DATA_PATH}/_metadata.bolt` or `_metadata.db` (`pkg/metadata/storage/bolt/`; `pkg/metadata/storage/badger/`).

The persisted index model is `pkg/meta/index.go` `Index`: settings, mappings, first/second shards, stats, version, storage type, and vector indexes. `metadata.Index.List` has a v0.2.6 conversion fallback while `metadata.Index.Get` does not; old metadata behavior requires source verification (`pkg/metadata/index.go`; `pkg/upgrade/v026t027.go`).

## Text Index Layout

New indexes set `StoreWithHash=true`. `core.Index.GetStoreName` hashes an index name to fixed-length base62 and splits after two characters. Physical shard paths append the first-shard ID and a six-digit hexadecimal second-shard ID (`pkg/core/index.go` `GetStoreName`; `pkg/core/index_shards.go` `IndexShard.openWriter`, `IndexShard.openReader`).

```text
<index-store>/<first-shard-id>/<second-shard-id:06x>/Bluge objects
```

Disk prefixes this with `SS_DATA_PATH`. S3/OSS use the logical path as an object prefix and stage/cache files beneath `SS_DATA_PATH` (`pkg/bluge/directory/disk.go` `GetDiskConfig`; `pkg/bluge/directory/s3.go`; `pkg/bluge/directory/oss.go`).

First-layer shards route documents via rendezvous hashing and must remain stable. Second-layer shards are appended when the latest exceeds `ZINC_SHARD_MAX_SIZE`; new inserts go to the latest shard, while updates/deletes can still modify an older shard containing that document (`pkg/core/index_shards.go` `Index.GetShardByDocID`, `IndexShard.CheckShards`, `IndexShard.NewShard`; `pkg/core/index_document.go` `CreateDocument`, `DeleteDocument`; `pkg/core/index_shards_wal.go` `walMergeDocs.WriteToShard`).

An index records its backend. `getOpenConfig` rejects opening it when persisted and process-wide storage types differ (`pkg/core/newindex.go` `getOpenConfig`).

## Vector Storage

Vector mappings create `meta.VecIndex` records with type `flat`, `ivf_pq`, or `hnsw`, dimensions, tuning values, layout version, and segment metadata (`pkg/core/index.go` `SetMappings`; `pkg/meta/index.go` `VecIndex`). Vector field names are also hashed for physical paths (`pkg/core/vec_index.go` `MakeVecIndex`).

All vector data is rooted under `vec_index/<index-store>/<field-store>/`:

| Data | Shape | Evidence |
|---|---|---|
| Exact/stored vectors | `<segment:04x>/stored_vec/<Bluge object>` | `pkg/core/ivfpq_segment.go`; `pkg/core/hnsw_segment.go` |
| IVFPQ FAISS index | `<segment:04x>/faiss/index.index` | `pkg/core/ivfpq_segment.go`; `pkg/core/vector/vec_storage.go` `ObjStore` |
| HNSW snapshot | `0000/hnsw/index.index` | `pkg/core/hnsw_segment.go` |

Flat uses exact stored vectors. IVFPQ writes exact vectors to a growing segment, rolls at `SS_VECTOR_IVFPQ_THRESHOLD`, and seals old segments into FAISS indexes in background (`pkg/core/vec_index.go` `IvfPqIndex.Batch`, `IvfPqIndex.checkNewSeg`; `pkg/core/vec_index_manager.go` `backgroundSealCheck`, `SealIndex`, `backgroundSeal`).

HNSW stores vector change logs and materialized document vectors in Bluge. Crossing the unapplied-log threshold queues a USearch snapshot rebuild; search merges pending exact vectors with HNSW candidates and recomputes exact distances (`pkg/core/vec_index.go` `HNSWIndex.Batch`; `pkg/core/hnsw_segment.go` `HNSWSegment.NeedRebuildHNSW`, `HNSWSegment.Search`; `pkg/core/vec_index_manager.go` `BuildHNSWIndex`, `backgroundBuildHNSWIndex`).

## Local Object Cache

`pkg/lru_cache/cache_manager.go` `Init` enables a process-global cache only for non-disk storage. It tracks Bluge segment/snapshot and `.index` files, removes `.temp` files at startup, and evicts unreferenced oldest files toward 70% capacity when the recorded size reaches `SS_MAX_OBJ_CACHE_SIZE`.

Capacity is a soft bound: the check occurs before adding a file and referenced files cannot be evicted (`pkg/lru_cache/cache_manager.go` `LruCache.makeRoomForCacheFile`). `pkg/bluge/directory/keycache.go` also caches object listings in process memory without TTL or cross-node invalidation; reassignment and overwrite-style vector files deserve verification.

## WAL and Redo

With WAL disabled, validated operations commit directly and metadata updates are queued asynchronously (`pkg/core/index_document.go` `CommitOperations`, `InitAsyncMetaDataUpdate`, `updateMetadataProcess`, `CloseAsyncMetaDataUpdate`). With WAL enabled, each first shard lazily opens a WAL, appends operations, and returns; `IndexShard.ConsumeWAL` syncs, reads, merges, writes Bluge/vector batches, records redo ranges, truncates, and updates shard metadata (`pkg/core/index_shards_wal.go` `OpenWAL`, `Rollback`, `ConsumeWAL`).

Disk WAL paths use the plain index/shard name, not the hashed text store:

```text
${SS_DATA_PATH}/<plain-index-name>/<first-shard-id>/wal/
${SS_DATA_PATH}/<plain-index-name>/<first-shard-id>/redo
```

MySQL/PostgreSQL schemas are in `pkg/wal/mysql/create.sql` and `pkg/wal/pgsql/create.sql`. `rkv` passes configuration validation but its `pkg/wal/wal.go` `Open` branch is TODO and should be treated as unimplemented.

## Consistency and Deletion

- `core.DeleteIndex` deletes vector data, text data, runtime state, then metadata; there is no transaction spanning these resources (`pkg/core/deleteindex.go` `DeleteIndex`).
- Index deletion does not visibly remove plain-name disk WAL paths or SQL WAL rows (`pkg/core/deleteindex.go`; `pkg/wal/wal.go`).
- Object/disk directory locks do not fence stale writers; ownership is an operational invariant (`pkg/bluge/directory/`).
- Local disk WAL is not failover-shared. SQL WAL is shared but has no visible ownership epoch/fencing in this repository.
- `formatIndex` reconstructs selected metadata for parallel search; verify field preservation before changing metadata schema (`pkg/core/loadindexes.go` `formatIndex`).
