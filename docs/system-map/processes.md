# Processes

> **Purpose:** Map executable entry points, process responsibilities, lifecycle, and interactions.
> **Last verified:** 2026-10-12
> **Verified against commit:** `f823d568efdf005582209d51df919f037ad26539`
> **Relevant source paths:** `cmd/zincsearch/main.go`, `cmd/zinc-proxy/`, `cmd/cluster-manager/`, `pkg/cluster/`, `pkg/routes/`

## Executables

| Process | Entry point | Default listen address | Responsibility |
|---|---|---|---|
| SeaSearch compute/API | `cmd/zincsearch/main.go` `main` | `:4080` | Owns index runtime, HTTP APIs, text/vector reads and writes, metadata, optional WAL |
| Cluster proxy | `cmd/zinc-proxy/main.go` `main` | `0.0.0.0:4082` | Routes/fans out compute work, handles user/role/template metadata CRUD locally, forwards node administration to manager |
| Cluster manager | `cmd/cluster-manager/main.go` `main` | `0.0.0.0:4081` | Stores node definitions and balances 256 ownership partitions among live nodes |

`AGENTS.md` names these process boundaries. The root `main.go` is not a fourth process.

## Compute Lifecycle

`cmd/zincsearch/main.go` `main` initializes in dependency order:

1. `config.InitConfig`, logging, `ider.InitIder`, `metadata.InitMetaStorage`, and `auth.InitFirstUser`.
2. GSE analysis, telemetry, Sentry, and profiling.
3. `cluster.Init` and `lru_cache.Init`.
4. `core.InitIndexList`, `core.InitVecIndexManager`, and `auth.Init`.
5. Async metadata updates; if enabled, `core.InitWalList` and `wal.Init`.
6. `routes.Setup` and the HTTP server, optionally with TLS 1.2 or newer.

The ordering is significant: metadata precedes users/indexes; cluster initialization and cache precede index loading; vector/WAL workers start before HTTP. `cluster.Init` launches heartbeat and assignment/user/role watches asynchronously, without waiting for initial synchronization; `LoadZincIndexesFromMetadata` filters against the current assignment map (`cmd/zincsearch/main.go` `main`; `pkg/cluster/cluster.go` `Init`; `pkg/core/loadindexes.go` `LoadZincIndexesFromMetadata`).

On first `SIGINT`/`SIGTERM`, HTTP gets up to 60 seconds to drain; `SIGQUIT` closes it without graceful drain. The cleanup callback invokes index, metadata-updater, index-watcher, vector-manager, auth, cluster, cache, metadata, and optional WAL closers in that order. A second signal exits directly; an HTTP shutdown error calls `log.Fatal` before later cleanup. These calls are not a guarantee that all background work/resources are drained; see [Operations](operations.md#shutdown-limitations) (`cmd/zincsearch/main.go` `main`, `shutdown`).

## Proxy Lifecycle

`cmd/zinc-proxy/main.go` `main` loads proxy config, initializes logs, opens shared etcd metadata, initializes IDs, then calls `InitProxy` and `StartProxy`. `StartProxy` opens `clusterkit` and launches node/assignment watches asynchronously; HTTP starts without a populated-routing-map readiness barrier (`cmd/zinc-proxy/proxy.go` `StartProxy`, `syncClusterNodes`, `syncAssigns`).

`SetupHttp` registers forwarded compute APIs, specialized fanout handlers, local user/role/template handlers, and `/api/cluster/nodes` forwarding to the manager (`cmd/zinc-proxy/http.go` `SetupHttp`, `forwardToManager`; `pkg/handlers/auth/`; `pkg/handlers/index/template.go`). Shutdown drains/closes HTTP, signals and waits for the two proxy watchers, closes `clusterkit`, then closes metadata (`cmd/zinc-proxy/main.go` `main`, `shutdown`; `cmd/zinc-proxy/proxy.go` `ShutDownProxy`).

## Manager Lifecycle

`cmd/cluster-manager/main.go` `main` loads manager config, initializes logging/routes, and calls `InitClusterManger`. The manager opens `clusterkit`; `watchCluster` runs assignment updates on a 15-second timer (`cmd/cluster-manager/manager.go` `InitClusterManger`, `watchCluster`). The first rebalance is therefore timer-delayed rather than immediate.

Shutdown drains HTTP and signals the manager watcher before closing `clusterkit` (`cmd/cluster-manager/main.go` `shutdown`; `cmd/cluster-manager/manager.go` `ShutDownClusterManager`).

## Cluster Interaction

```mermaid
sequenceDiagram
    participant N as zincsearch
    participant T as etcd topology/clusterkit
    participant A as etcd application metadata
    participant M as cluster-manager
    participant P as zinc-proxy
    P->>M: /api/cluster/nodes administration
    M->>T: read/write configured nodes; read heartbeats
    N->>T: heartbeat(node ID)
    M->>T: write changed prefix assignments
    P->>T: watch nodes and assignments (async)
    N->>T: watch assignments (async)
    N->>A: read/write metadata; watch users/roles (async)
    P->>A: user/role/template CRUD; read index metadata
    P->>N: route owner request or partial query
```

An index maps to the first two hex characters of its MD5 digest. The manager distributes all `00` through `ff` prefixes; the proxy resolves prefix to node URL; the compute node independently rejects non-owned ordinary index requests (`cmd/cluster-manager/manager.go` `distribute`; `cmd/zinc-proxy/proxy.go` `getAssignNodeByIndex`; `pkg/cluster/cluster.go` `AssignCheck`; `pkg/routes/middleware.go` `ClusterMiddleware`).

Eligible large searches can execute read-only work on non-owner nodes via internal APIs. These nodes reconstruct indexes from shared metadata/storage rather than taking write ownership (`cmd/zinc-proxy/parallel_query.go` `calcNodeIndexMap`; `pkg/core/loadindexes.go` `GetZincIndexFromMetadata`, `formatIndex`; `pkg/handlers/search/search_v2.go` `InternalUnifiedSearch`).

## Process Boundaries

- Local proxy metadata CRUD writes shared etcd, not a proxy-local database (`pkg/metadata/storage.go` `InitMetaStorageForProxy`; `cmd/zinc-proxy/http.go` `SetupHttp`).
- The manager does not initialize application metadata or indexes (`cmd/cluster-manager/main.go` `main`).
- Detailed routing/failure, handoff, shutdown, and deployment caveats are centralized in [Operations](operations.md).
