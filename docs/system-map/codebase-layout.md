# Codebase Layout

> **Purpose:** Identify important directories and package ownership without cataloging every file.
> **Last verified:** 2026-09-18
> **Verified against commit:** `c914b4cc84f87197c03009bafefc06f3f513700b`
> **Relevant source paths:** `cmd/`, `pkg/`, `faiss_wrapper/`, `docs/`, `test/`, `.github/workflows/`, `ci/`

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
| `pkg/handlers/` | Gin request adapters grouped by `auth`, `document`, `index`, `search` |
| `pkg/routes/` | Router/middleware assembly: `Setup`, `SetRoutes`, `AuthMiddleware`, `ClusterMiddleware` |
| `pkg/uquery/` | Query normalization/translation: `NormalizeQuery`, `ParseQueryDSL`, aggregations, sorting, highlighting |
| `pkg/bluge/` | Bluge integration, analyzers, search merging, and disk/S3/OSS directories |
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
| Vector lifecycle/search | `pkg/core/vec_index.go`, `vec_index_manager.go`, `vector_search.go`, `ivfpq_segment.go`, `hnsw_segment.go` |
| HTTP surface | `pkg/routes/routes.go`, then the referenced `pkg/handlers/` package |
| Cluster proxy | `cmd/zinc-proxy/http.go`, `proxy.go`, `parallel_query.go`, `vector.go` |
| Cluster balancing | `cmd/cluster-manager/manager.go`, `http.go` |

## Non-Obvious Conventions

- The Go module name remains `github.com/zincsearch/zincsearch`; imports should follow that path (`go.mod`).
- Project forks replace three Bluge modules (`go.mod` `replace` block).
- Current index creation hard-codes one first-layer shard despite the `ZINC_SHARD_NUM` config field (`pkg/core/newindex.go` `NewIndex`; `pkg/config/config.go` `shard.Num`).
- UI routes are commented out and no `web/` directory exists (`pkg/routes/routes.go` `SetRoutes`).
- `pkg/bluge/analysis/lang/chs/gse_dict.go` and `gse_stop.go` are large embedded dictionary data; treat them as data assets, not normal hand-edited logic (`pkg/bluge/analysis/lang/chs/README.md`).

Generated-file ownership and regeneration are documented in [Development](development.md#generated-and-data-files).
