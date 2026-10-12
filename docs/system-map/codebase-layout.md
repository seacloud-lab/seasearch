# Codebase Layout

> **Purpose:** Identify important directories and package ownership without cataloging every file.
> **Last verified:** 2026-10-12
> **Verified against commit:** `f823d568efdf005582209d51df919f037ad26539`
> **Relevant source paths:** `cmd/`, `pkg/`, `faiss_wrapper/`, `docs/`, `test/`, `.github/workflows/`, `ci/`, `go.mod`

## Top Level

| Path | Role |
|---|---|
| `cmd/zincsearch/` | Main compute/API executable |
| `cmd/zinc-proxy/` | Cluster gateway, routing, bulk splitting, distributed query merge |
| `cmd/cluster-manager/` | Node administration, heartbeat filtering, partition balancing |
| `pkg/` | Shared application packages |
| `faiss_wrapper/` | CGO/C++ FAISS distance wrapper used by IVFPQ code |
| `docs/` | Generated Swagger artifacts plus this hand-maintained system map |
| `test/` | API tests, benchmark, test utilities, and older test documentation |
| `examples/` | Python examples and sample documents |
| `ci/` | Native-dependency CI image definition |
| `.github/workflows/` | Go test/lint and CI-image workflows |

`main.go` at repository root is a package placeholder, not an entry point.

## Package Ownership

| Package | Responsibility and starting symbols |
|---|---|
| `pkg/core/` | Main domain/runtime: `Index`, `IndexList`, shards, commits, search, vectors, telemetry |
| `pkg/core/vector/` | Vector storage abstraction and disk/S3/OSS adapters: `ObjStore`, `GetVectorStorage`, `ListVecSegments`; used by `InitVecIndexManager` and vector segment loading |
| `pkg/handlers/` | Gin request adapters grouped by `auth`, `document`, `index`, `search` |
| `pkg/routes/` | Router/middleware assembly: `Setup`, `SetRoutes`, `AuthMiddleware`, `ClusterMiddleware` |
| `pkg/uquery/` | Query normalization/translation: `NormalizeQuery`, `ParseQueryDSL`, aggregations, sorting, highlighting |
| `pkg/uquery/analysis/` | Analyzer selection/custom assembly and tokenizer/filter dispatch: `RequestAnalyzer`, `QueryAnalyzer`, `QueryAnalyzerForField`; called by index settings/loading, analyze handlers, and query translation |
| `pkg/bluge/` | Bluge integration, local analysis implementations, search merging, and disk/S3/OSS directories |
| `pkg/bluge/analysis/lang/chs/` | GSE implementation and dictionaries: `NewGseStandardAnalyzer`, `NewGseSearchAnalyzer`, analyzer/tokenizer/filter subpackages; selection lives in `pkg/uquery/analysis/` |
| `pkg/meta/` | Persisted and API data structures, health/version, Elasticsearch response models |
| `pkg/metadata/` | Typed index/user/role/template/alias/KV repositories and backend selection |
| `pkg/metadata/storage/` | `Storager` interface plus Bolt, Badger, and etcd implementations |
| `pkg/config/` | Compute-process environment schema and startup validation |
| `pkg/cluster/` | Compute heartbeat, assignment and auth metadata watches |
| `pkg/auth/` | Bootstrap admin, password verification, users/roles/permissions, local caches |
| `pkg/wal/` | Optional disk/MySQL/PostgreSQL WAL and redo abstraction |
| `pkg/lru_cache/` | Node-local rotating cache for remote Bluge and vector files |
| `pkg/ider/` | Snowflake IDs encoded as base62 |
| `pkg/upgrade/` | Legacy persisted index-metadata conversion |
| `pkg/errors/` | Application error types and HTTP rendering |
| `pkg/zutils/` | Config reflection, JSON, flattening, logging, hashing, filesystem and conversion helpers |

## Core Files by Task

| Task | Start here |
|---|---|
| Index creation/loading/deletion | `pkg/core/newindex.go`, `loadindexes.go`, `deleteindex.go` |
| Sharding and physical paths | `pkg/core/index_shards.go`, `pkg/core/index.go` |
| Document writes/WAL commit | `pkg/core/index_document.go`, `index_shards_document.go`, `index_shards_wal.go` |
| Text search | `pkg/core/search.go`, `multi_search.go`, `searcher.go`, `pkg/bluge/search/search.go` |
| Analyzer selection versus GSE implementation | `pkg/uquery/analysis/analyzer.go`, then `pkg/bluge/analysis/lang/chs/`; see [Operations](operations.md#operational-invariants) for GSE upgrade compatibility |
| Vector lifecycle/search | `pkg/core/vec_index.go`, `vec_index_manager.go`, `vector_search.go`, `ivfpq_segment.go`, `hnsw_segment.go` |
| Vector file storage | `pkg/core/vector/vec_storage.go`, `vec_storage_s3.go`, `vec_storage_oss.go`; persistence context in [Data and Storage](data-storage.md#vector-storage) |
| HTTP surface | `pkg/routes/routes.go`, then the referenced `pkg/handlers/` package |
| Cluster proxy | `cmd/zinc-proxy/http.go`, `proxy.go`, `parallel_query.go`, `vector.go` |
| Cluster balancing | `cmd/cluster-manager/manager.go`, `http.go` |

## Non-Obvious Conventions

- The Go module name remains `github.com/zincsearch/zincsearch`; imports should follow that path (`go.mod`).
- Five modules resolve to `seacloud-lab` forks: Bluge, its segment API, Ice, go-gin-prometheus, and WAL (`go.mod` `replace` block). Imports retain their original module paths; replacements do not require import renames. Check `go.mod` for current versions rather than maintaining a duplicate dependency catalog here.
- Shard-count configuration does not control current index creation; see [Configuration](configuration.md#compute-configuration).
- UI routes are commented out and no `web/` directory exists (`pkg/routes/routes.go` `SetRoutes`).
- GSE dictionary asset ownership is documented in [Development](development.md#generated-and-data-files).

Generated-file ownership and regeneration are documented in [Development](development.md#generated-and-data-files).
