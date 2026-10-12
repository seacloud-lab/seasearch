# Operations

> **Purpose:** Describe startup operations, background jobs, clustering, observability, failure behavior, and runtime constraints.
> **Last verified:** 2026-10-12
> **Verified against commit:** `f823d568efdf005582209d51df919f037ad26539`
> **Relevant source paths:** `cmd/`, `pkg/core/`, `pkg/cluster/`, `pkg/routes/`, `pkg/lru_cache/`, `pkg/wal/`, `pkg/zutils/logger/`, `pkg/bluge/analysis/lang/chs/`

## Background Work

| Work | Cadence/trigger | Evidence |
|---|---|---|
| Compute heartbeat | Continuous keepalive; retry after 15s | `pkg/cluster/cluster.go` `keepHeartbeat` |
| Assignment/user/role watches | etcd watch; retry after 15s | `pkg/cluster/cluster.go` `syncAssigns`, `syncUsers`, `syncRoles` |
| Proxy node/assignment watches | Initial list + watch; retry after 15s | `cmd/zinc-proxy/proxy.go` `syncClusterNodes`, `syncAssigns` |
| Manager rebalance | 15s wait before each update, including the first | `cmd/cluster-manager/manager.go` `watchCluster` |
| Runtime index resource GC | Every 10m; closes after 24h inactivity | `pkg/core/indexlist.go` `StartGC`, `GC` |
| Lazy text writer close | Every minute; 5m idle | `pkg/core/indexlist.go` `LazyCloseSecondIndexShardWriters` |
| Async metadata flush | When WAL is off, drains queued updates on a 1s ticker | `pkg/core/index_document.go` `InitAsyncMetaDataUpdate`, `updateMetadataProcess` |
| WAL consumption | `ZINC_WAL_SYNC_INTERVAL` | `pkg/core/index_shards_wallist.go` `ConsumeWAL` |
| Vector cache GC | Starts after 1h; frees one zero-ref index idle >1h, retries in 1m when one is found, otherwise 1h | `pkg/core/vec_index_manager.go` `backgroundGC`, `execGC` |
| Lazy vector writer close | Every minute; tries segment writers idle >5m with no references | `pkg/core/vec_index_manager.go` `lazyCloseSegmentWriter`; `pkg/core/ivfpq_segment.go` `TryCloseIdleWriter` |
| IVFPQ sealing | Automatic threshold scan once at ~1h; explicit `SealIndex` also queues tasks | `pkg/core/vec_index_manager.go` `backgroundSealCheck`, `SealIndex`, `backgroundSeal` |
| HNSW build | Batch queues a task when unapplied logs exceed `HNSWMaxLogs` | `pkg/core/vec_index.go` `HNSWIndex.Batch`; `pkg/core/hnsw_segment.go` `NeedRebuildHNSW`; `pkg/core/vec_index_manager.go` `BuildHNSWIndex`, `backgroundBuildHNSWIndex` |
| Telemetry | Event worker and 30m heartbeat when enabled | `pkg/core/telemetry.go` `Cron` |
| Log reopen | `SIGUSR1`; external tooling must rotate/rename files | `pkg/zutils/logger/logger.go` `HandleRotateSignal`, `RotatingWriter.reopen` |

`backgroundSealCheck` uses `time.NewTimer(time.Hour)` without resetting it, so the automatic scan is not hourly recurring. Both it and `SealIndex` queue index/field tasks when a growing IVFPQ segment reaches `IvfPqThreshold`; compute `/api/:target/:field/_seal` calls `SealVectorIndex` -> `SealIndex`. HNSW rebuilding is threshold/task-driven, not a periodic scan (`pkg/routes/routes.go` `SetRoutes`; `pkg/handlers/index/vector_index.go` `SealVectorIndex`; `pkg/core/vec_index_manager.go`).

## Shutdown Limitations

- The 60s timeout bounds HTTP draining, not subsequent cleanup; a second termination signal exits immediately, and HTTP shutdown failure calls `log.Fatal` before closers run (all three process `main`/`shutdown` functions).
- Text `LazyCloseSecondIndexShardWriters`, vector `lazyCloseSegmentWriter`, and `IndexShardWALList.ConsumeWAL` use ticker loops without a stop/wait path. Telemetry's cron/event worker and logger's signal loop are also not stopped by the main cleanup callback (`pkg/core/indexlist.go`; `pkg/core/vec_index_manager.go`; `pkg/core/index_shards_wallist.go`; `pkg/core/telemetry.go` `Cron`, `runEvents`; `pkg/zutils/logger/logger.go` `HandleRotateSignal`).
- Vector manager closer accounts for GC, seal-check, seal-worker, and HNSW-worker only; `CloseVecIndexManager` signals/waits but does not iterate/free cached vector indexes. The fifth worker, lazy-close, is unaccounted for (`pkg/core/vec_index_manager.go` `InitVecIndexManager`, `CloseVecIndexManager`). `lru_cache.LruCache.Close` is a no-op (`pkg/lru_cache/cache_manager.go`).
- Async metadata shutdown stops the worker without explicitly draining its queue; WAL-mode index metadata updates instead occur during WAL consumption. Do not equate shutdown completion with a final metadata flush (`pkg/core/index_document.go` `CloseAsyncMetaDataUpdate`, `updateMetadataProcess`; `pkg/core/index_shards_wallist.go` `ConsumeWAL`).

## Cluster Assignment and Failover

The manager intersects configured nodes with live heartbeat nodes, preserves valid assignments where possible, balances all 256 prefixes, and writes changed assignments (`cmd/cluster-manager/manager.go` `updateAssigns`, `distribute`). Exact heartbeat lease duration is controlled by external `clusterkit`; the pinned implementation must be checked for timing guarantees.

Compute/proxy watches are asynchronous with no initial-sync readiness barrier. Compute index loading filters against the current assignment map; later assignment notifications load/unload runtime indexes. Proxy HTTP can start with empty maps (`pkg/cluster/cluster.go` `Init`, `updateAssigns`; `pkg/core/loadindexes.go` `LoadZincIndexesFromMetadata`; `pkg/core/indexlist.go` `watchIndexUpdate`, `updateIndexList`; `cmd/zinc-proxy/proxy.go` `StartProxy`; `cmd/zinc-proxy/main.go` `main`).

Whenever `watchAssigns` returns (not only on error), compute and proxy `syncAssigns` clear their assignment maps before the 15s retry. Compute `AssignCheck` then rejects ordinary owner-checked requests (406 through `ClusterMiddleware`); proxy `GetAddrByIndex` fails owner lookup. Clearing the compute map does not itself notify `UnassignChan` to unload cached indexes. Proxy node-watch return, by contrast, retains its last node map (`pkg/cluster/cluster.go` `syncAssigns`, `AssignCheck`; `pkg/routes/middleware.go` `ClusterMiddleware`; `cmd/zinc-proxy/proxy.go` `syncAssigns`, `syncClusterNodes`, `GetAddrByIndex`).

Failover is delayed ownership reassignment, not in-flight request retry or replica promotion. No generation fencing or old-owner shutdown acknowledgment is visible in the repository's handoff path; assignment checking is a request-time gate, not cancellation of already-running writes. Directory locks here are no-ops; external storage/dependency fencing must not be assumed (`cmd/cluster-manager/manager.go` `updateAssigns`; `pkg/core/indexlist.go` `updateIndexList`; `pkg/routes/middleware.go` `ClusterMiddleware`; `pkg/bluge/directory/disk.go` `fileSystemDirectory.Lock`; `s3.go` `S3Backend.Lock`; `oss.go` `OssBackend.Lock`). Local WAL files are not transferred by this path (`pkg/core/index_shards_wal.go` `OpenWAL`; `pkg/wal/wal.go` `Open`).

Ordinary reverse-proxy transport errors return 502 except `context.Canceled`; owner lookup failures occur before forwarding (500 in `directForwarding`). Fanout `fetchHTTP` returns errors for transport failures, 4xx, non-200 statuses, and response decode failures; it uses `http.DefaultTransport` without an explicit overall timeout or the incoming request's context (`cmd/zinc-proxy/http.go` `directForwarding`; `cmd/zinc-proxy/utils.go` `fetchHTTP`, `ProxyPool.Get`).

Fanout errors fail bulk/unified-search requests, but successful writes to other nodes are not rolled back. HTTP 200 bulk item errors are different: `EsBulk` appends returned `Items` without aggregating downstream top-level `Error`/`Errors`, so inspect item results rather than treating 200 as all-item success (`cmd/zinc-proxy/bulk.go` `Bulk`, `EsBulk`; `pkg/handlers/document/bulk.go` `ESBulk`, `BulkResponse`; `cmd/zinc-proxy/parallel_query.go` `processUnifiedSearch`).

Parallel query node selection uses configured nodes, not a separately heartbeat-filtered list in the proxy. A registered dead secondary node can therefore fail distributed search (`cmd/zinc-proxy/proxy.go` `updateNodeMp`, `GetNodeList`; `cmd/zinc-proxy/parallel_query.go` `calcNodeIndexMap`, `getQueryNodes`).

Object-list caching invalidates only on this process's writes/removes/copies; stable vector-file cache hits have no cross-node freshness check (`pkg/bluge/directory/keycache.go` `objectKeyCache.List`, `KeyCacheClient`; `pkg/lru_cache/cache_manager.go` `GetCacheFile`; `pkg/core/vector/vec_storage_s3.go` `s3Storage.LoadFile`; `pkg/core/vector/vec_storage_oss.go` `OssStorage.LoadFile`).

## Observability

| Facility | Behavior | Evidence |
|---|---|---|
| Logs | Zerolog `ConsoleWriter` formatted text, not JSON; files or stdout with component prefix | `pkg/zutils/logger/logger.go` `SetupMainLog`, `SetupAccessLog`, `RotatingWriter.Write`; process `main` functions |
| Access log | Method, URI, status, elapsed; compute and proxy | `pkg/routes/accesslog.go` `AccessLog`; proxy `SetupHttp` |
| pprof | Registered unconditionally on compute | `pkg/routes/pprof.go` `SetPProf` |
| Prometheus | Optional Gin metrics and index gauge metrics | `pkg/routes/prometheus.go`; `pkg/core/metrics.go` |
| Sentry | Optional initialization | `cmd/zincsearch/main.go` `sentries` |
| Pyroscope | Optional CPU/allocation/in-use profiles | `cmd/zincsearch/main.go` `profiling` |
| Product telemetry | Optional server/search/heartbeat events | `pkg/core/telemetry.go` |
| Health/version | Public `/healthz`, `/version` on compute only; not registered on proxy/manager | `pkg/routes/routes.go` `SetRoutes`; both proxy/manager `SetupHttp` functions |

`/healthz` reports a constant OK and does not probe metadata, object storage, WAL, or cluster connectivity (`pkg/meta/healthz.go` `GetHealthz`).

All three entry points start `HandleRotateSignal`; `SIGUSR1` reopens configured log files (and redirects stderr for file-backed main logs), rather than implementing size/time rotation (`pkg/zutils/logger/logger.go` `HandleRotateSignal`, `RotatingWriter.reopen`).

## Operational Invariants

- `SS_DATA_PATH` must be writable even with object storage; it holds cache/temp files and may hold WAL (`pkg/config/config.go` `initConfig`).
- Every node performing distributed reads must access the same metadata and physical object backend (`pkg/core/loadindexes.go` `GetZincIndexFromMetadata`, `formatIndex`; `pkg/handlers/search/search_v2.go` `InternalUnifiedSearch`).
- Single-writer ownership is required; see [Cluster Assignment and Failover](#cluster-assignment-and-failover) for the no-op directory locks and handoff limitations.
- First-layer shard count must not change after documents exist because it controls document routing (`pkg/core/index_shards.go` `GetShardByDocID`).
- Object cache size is soft, and mmap/file-descriptor capacity matters; see [Local Object Cache](data-storage.md#local-object-cache).
- Vector document IDs must be canonical base62 integers (`pkg/core/index_document.go` `CreateDocument`).
- Built-in GSE standard/search analyzers now lowercase tokens across scripts before the stop filter, not just GSE's ASCII normalization. Previously indexed mixed-case non-ASCII terms are not rewritten on restart; reindex affected data for consistent matching. Custom tokenizer/filter chains do not automatically inherit the built-in filters (`pkg/bluge/analysis/lang/chs/analyzer/standard.go` `NewStandardAnalyzer`; `search.go` `NewSearchAnalyzer`; `pkg/bluge/analysis/lang/chs/gse.go` analyzer versus tokenizer constructors).

## Known Boundaries to Reverify

- Manager leader election is not visible in `cmd/cluster-manager/manager.go`; API security is documented in [APIs](apis.md#manager-api-boundary).
- Proxy/manager entry points configure neither TLS nor server read/write/idle timeouts; deployment protection is required. Proxy search routes omit compute `IndexAliasMiddleware`; verify aliases through the deployed proxy (`cmd/zinc-proxy/main.go`, `cmd/cluster-manager/main.go` `main`; `cmd/zinc-proxy/http.go` `SetupHttp`; `pkg/routes/middleware.go` `IndexAliasMiddleware`).
- Manager cleanup calls `clusterkit.Delete` for `/cluster/nodes` and `/cluster/assign/`; verify these exact paths against the pinned dependency before using `/api/cluster/clean` (`cmd/cluster-manager/http.go` `cleanClusterData`).
- When there are no live registered nodes, existing assignments are retained by manager logic (`cmd/cluster-manager/manager.go` `updateAssigns`).
- Lease expiry, watch recovery, assignment-write atomicity, and object/snapshot publication guarantees require inspection of pinned `clusterkit`, replaced Bluge/Ice modules, object clients, and deployment behavior; repository call sites alone do not establish them (`go.mod`; `pkg/cluster/cluster.go`; `cmd/cluster-manager/manager.go` `updateAssigns`).
- CLI `zincsearch version` is hard-coded to `0.9.0`, while HTTP uses `pkg/meta/version.go` build variables (`cmd/zincsearch/main.go` `main`; `pkg/meta/version.go`).
