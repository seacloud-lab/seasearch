# Operations

> **Purpose:** Describe startup operations, background jobs, clustering, observability, failure behavior, and runtime constraints.
> **Last verified:** 2026-09-18
> **Verified against commit:** `c914b4cc84f87197c03009bafefc06f3f513700b`
> **Relevant source paths:** `cmd/`, `pkg/core/`, `pkg/cluster/`, `pkg/routes/`, `pkg/lru_cache/`, `pkg/wal/`

## Background Work

| Work | Cadence/trigger | Evidence |
|---|---|---|
| Compute heartbeat | Continuous keepalive; retry after 15s | `pkg/cluster/cluster.go` `keepHeartbeat` |
| Assignment/user/role watches | etcd watch; retry after 15s | `pkg/cluster/cluster.go` `syncAssigns`, `syncUsers`, `syncRoles` |
| Manager rebalance | Every 15s | `cmd/cluster-manager/manager.go` `watchCluster` |
| Runtime index resource GC | Every 10m; closes after 24h inactivity | `pkg/core/indexlist.go` `StartGC`, `GC` |
| Lazy text writer close | Every minute; 5m idle | `pkg/core/indexlist.go` `LazyCloseSecondIndexShardWriters` |
| Async metadata flush | Queue processing when WAL is off | `pkg/core/index_document.go` `InitAsyncMetaDataUpdate`, `updateMetadataProcess`, `CloseAsyncMetaDataUpdate` |
| WAL consumption | `ZINC_WAL_SYNC_INTERVAL` | `pkg/core/index_shards_wallist.go` `ConsumeWAL` |
| Vector cache GC | Hourly, then faster while expired entries exist | `pkg/core/vec_index_manager.go` `backgroundGC` |
| IVFPQ sealing/HNSW build | Periodic/task-driven workers | `pkg/core/vec_index_manager.go` `backgroundSealCheck`, `backgroundSeal`, `backgroundBuildHNSWIndex` |
| Telemetry | Event worker and 30m heartbeat when enabled | `pkg/core/telemetry.go` `Cron` |
| Log rotation | Signal-driven | `pkg/zutils/logger/` `HandleRotateSignal` |

Some long-lived loops do not have an obvious individually awaited shutdown path; process exit may terminate them after primary closers finish. Recheck closer accounting before changing graceful shutdown.

## Cluster Assignment and Failover

The manager intersects configured nodes with live heartbeat nodes, preserves valid assignments where possible, balances all 256 prefixes, and writes changed assignments (`cmd/cluster-manager/manager.go` `updateAssigns`, `distribute`). Exact heartbeat lease duration is controlled by external `clusterkit`; the pinned implementation must be checked for timing guarantees.

Compute nodes watch assignments and load/unload runtime indexes. Proxy watches assignments and configured node URLs. Failover means delayed ownership reassignment; it does not retry an in-flight request or promote a data replica (`pkg/cluster/cluster.go` `updateAssigns`; `pkg/core/indexlist.go` `updateIndexList`; `cmd/zinc-proxy/proxy.go` `syncAssigns`).

Ordinary reverse-proxy transport failures return 502. Fanout operations fail when a participant fails, and `fetchHTTP` has no explicit client timeout (`cmd/zinc-proxy/utils.go` `fetchHTTP`, `ProxyPool.Get`).

Parallel query node selection uses configured nodes, not a separately heartbeat-filtered list in the proxy. A registered dead secondary node can therefore fail distributed search (`cmd/zinc-proxy/proxy.go` node map updates; `cmd/zinc-proxy/parallel_query.go` `getQueryNodes`).

## Observability

| Facility | Behavior | Evidence |
|---|---|---|
| Structured logs | Zerolog component main/access logs; optional stdout | `pkg/zutils/logger/`; process `main` functions |
| Access log | Method, URI, status, elapsed; compute and proxy | `pkg/routes/accesslog.go` `AccessLog`; proxy `SetupHttp` |
| pprof | Registered unconditionally on compute | `pkg/routes/pprof.go` `SetPProf` |
| Prometheus | Optional Gin metrics and index gauge metrics | `pkg/routes/prometheus.go`; `pkg/core/metrics.go` |
| Sentry | Optional initialization | `cmd/zincsearch/main.go` `sentries` |
| Pyroscope | Optional CPU/allocation/in-use profiles | `cmd/zincsearch/main.go` `profiling` |
| Product telemetry | Optional server/search/heartbeat events | `pkg/core/telemetry.go` |
| Health/version | Public `/healthz`, `/version` | `pkg/meta/healthz.go`, `pkg/meta/version.go` |

`/healthz` reports a constant OK and does not probe metadata, object storage, WAL, or cluster connectivity (`pkg/meta/healthz.go` `GetHealthz`).

## Operational Invariants

- `SS_DATA_PATH` must be writable even with object storage; it holds cache/temp files and may hold WAL (`pkg/config/config.go` `initConfig`).
- Every node performing distributed reads must access the same metadata and physical object backend (`pkg/core/loadindexes.go` `GetZincIndexFromMetadata`, `formatIndex`; `pkg/handlers/search/search_v2.go` `InternalUnifiedSearch`).
- Single-writer ownership is required because storage directory locks are disabled/no-op (`pkg/bluge/directory/`).
- First-layer shard count must not change after documents exist because it controls document routing (`pkg/core/index_shards.go`).
- Object cache size is soft, and mmap/file-descriptor capacity matters (`pkg/lru_cache/cache_manager.go`).
- Vector document IDs must be canonical base62 integers (`pkg/core/index_document.go` `CreateDocument`).
- Local disk WAL is not transferred during failover; verify durability expectations before using it in cluster mode (`pkg/wal/wal.go`; `pkg/core/index_shards_wal.go`).

## Known Boundaries to Reverify

- Manager leader election is not visible in `cmd/cluster-manager/manager.go`; API security is documented in [APIs](apis.md#manager-api-boundary).
- Manager cleanup deletes a path that appears inconsistent with `clusterkit` assignment storage; inspect the external dependency before using `/api/cluster/clean` (`cmd/cluster-manager/http.go` `cleanClusterData`).
- When there are no live registered nodes, existing assignments are retained by manager logic (`cmd/cluster-manager/manager.go` `updateAssigns`).
- Assignment handoff has no generation fencing or acknowledgment. Shared storage itself does not reject stale writers.
- Object-list and stable vector-file caches lack cross-node invalidation (`pkg/bluge/directory/keycache.go`; `pkg/lru_cache/cache_manager.go`).
- CLI `zincsearch version` is hard-coded to `0.9.0`, while HTTP uses `pkg/meta/version.go` build variables (`cmd/zincsearch/main.go` `main`; `pkg/meta/version.go`).
