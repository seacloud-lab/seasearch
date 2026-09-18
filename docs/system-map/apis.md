# APIs

> **Purpose:** Map API groups, route ownership, middleware, authentication, and compatibility boundaries.
> **Last verified:** 2026-09-18
> **Verified against commit:** `c914b4cc84f87197c03009bafefc06f3f513700b`
> **Relevant source paths:** `pkg/routes/`, `pkg/handlers/`, `cmd/zinc-proxy/http.go`, `cmd/cluster-manager/http.go`, `docs/`

## Source of Truth

Compute routes are registered in `pkg/routes/routes.go` `SetRoutes`; setup middleware is in `pkg/routes/setup.go` `Setup`. Proxy and manager have independent routers in `cmd/zinc-proxy/http.go` and `cmd/cluster-manager/http.go` `SetupHttp`.

Generated Swagger is an incomplete annotation-derived view, not route truth. See [Development](development.md#generated-and-data-files) for regeneration and edit constraints.

## Middleware and Authentication

The compute router installs recovery, access logging, pprof, optional Prometheus, and permissive CORS (`pkg/routes/setup.go` `Setup`; `pkg/routes/routes.go` `SetRoutes`).

`pkg/routes/middleware.go` `AuthMiddleware` requires HTTP Basic credentials and an exact route permission string. `admin` bypasses permission lookup (`pkg/auth/auth.go` `VerifyCredentials`, `VerifyRoleHasPermission`). There are no sessions/JWTs; `/api/login` validates credentials and returns user information (`pkg/handlers/auth/login.go` `Login`).

`ClusterMiddleware` verifies that the current compute process owns every named target and returns HTTP 406 on mismatch. `IndexAliasMiddleware` expands ES aliases before search (`pkg/routes/middleware.go`).

Public compute surfaces include `/version`, `/healthz`, `/api/login`, ES discovery/license/xpack responses, optional Swagger, pprof, and optional metrics (`pkg/routes/routes.go`; `pkg/routes/pprof.go` `SetPProf`; `pkg/routes/prometheus.go` `SetPrometheus`). Health is a shallow constant response, not a dependency check (`pkg/meta/healthz.go` `GetHealthz`).

## Compute Route Groups

| Group | Principal paths | Handlers |
|---|---|---|
| Operational | `GET /version`, `/healthz`, `/swagger/*any`, pprof, metrics | `pkg/meta/`, `pkg/routes/` |
| Internal distributed search | `POST /api/internal/get_stats`, `/api/internal/unified_search`, `/api/internal/vector_search` | `pkg/handlers/search/` internal handlers |
| Unified search | `POST /api/unified_search` | `search.UnifiedSearch` |
| Auth/admin | `/api/login`, `/api/user`, `/api/role`, `/api/permissions` | `pkg/handlers/auth/` |
| Native index | `/api/index`, `/api/index/:target`, mapping/settings/refresh/analyze | `pkg/handlers/index/` |
| Native documents | `/api/_bulk`, `/:target/_bulk`, `/_bulkv2`, `/_doc`, `/_update` | `pkg/handlers/document/` |
| Native text search | `POST /api/:target/_search` | `search.SearchV1` |
| Vector | `POST /api/:target/_search/vector`, `/:target/:field/_seal`, `/:target/_recall` | `pkg/handlers/search/search_vector.go`; `pkg/handlers/index/vector_index.go` |

Consult `pkg/routes/routes.go` `SetRoutes` for exact methods and permission strings before changing a route.

## Elasticsearch-Compatible Surface

The `/es` routes mostly reuse native handlers and storage rather than a separate engine (`pkg/routes/routes.go`):

| Group | Paths/behavior |
|---|---|
| Discovery | `/es/`, `/_license`, `/_xpack`; fabricated compatibility responses in `pkg/meta/elastic/` |
| Search | `/_search`, `/:target/_search`, `/_msearch`, `/:target/_msearch`, delete by query |
| Index lifecycle | `PUT /:target`, mappings, settings, refresh, analyze |
| Templates/aliases | `/_index_template...`, `/_aliases`, alias reads |
| Documents | `/_bulk`, `/:target/_bulk`, `_doc`, `_create`, `_update`, delete |
| Data streams | `/_data_stream/:target`; compatibility responses only, not a persisted stream lifecycle |

`ESMiddleware` sets compatibility headers. `pkg/meta/elastic/elasticsearch.go` `NewESInfo` can imitate a client version or use `ZINC_PLUGIN_ES_VERSION`. Data-stream handlers return compatibility shapes without creating storage (`pkg/meta/elastic/data_stream.go` `PutDataStream`, `GetDataStream`).

## Proxy API Boundary

`cmd/zinc-proxy/http.go` `SetupHttp` mirrors much of the compute API but uses three strategies:

| Strategy | Examples | Evidence |
|---|---|---|
| Handle locally from shared metadata | Users/roles, index listing/existence, templates | proxy `SetupHttp`; `AuthMiddlewareNoCache` |
| Reverse proxy to owner | Ordinary index/document/search operations | `directForwarding`; `ProxyPool.Get` in `utils.go` |
| Parse and fan out | Index delete, bulk, msearch, eligible unified/vector search | `bulk.go`, `search.go`, `parallel_query.go`, `vector.go` |

Locally authenticated proxy routes use `cmd/zinc-proxy/middleware.go` `AuthMiddlewareNoCache`, which reads users/roles from shared metadata. Other forwarded routes generally preserve `Authorization` for compute-node enforcement (`cmd/zinc-proxy/utils.go` `fetchHTTP`).

Proxy `/api/login` is registered locally with shared `pkg/handlers/auth/login.go` `Login`, but that handler uses the in-memory credential cache. Proxy startup does not call `auth.Init`, so login behavior through the proxy is not verified and appears inconsistent with its direct-metadata middleware (`cmd/zinc-proxy/main.go` `main`; `pkg/auth/auth.go` `VerifyCredentials`).

Known route drift: proxy registers `POST /api/:target/:field/_rebuild`, while compute registers `POST /api/:target/:field/_seal`. Verify and align source routes before relying on `_rebuild` (`cmd/zinc-proxy/http.go` `SetupHttp`; `pkg/routes/routes.go` `SetRoutes`).

## Manager API Boundary

`cmd/cluster-manager/http.go` `SetupHttp` registers:

- `GET/POST/PUT/DELETE /api/cluster/nodes`
- `POST /api/cluster/clean`

These handlers manage configured nodes and cluster keys (`listClusterNodes`, `createClusterNodes`, `updateClusterNodes`, `deleleClusterNodes`, `cleanClusterData`). No authentication middleware is installed. The proxy forwards cluster-node CRUD to `SS_CLUSTER_MANAGER_URL`, but not the clean endpoint (`cmd/zinc-proxy/http.go` `forwardToManager`).

## Verify Before Changing

- Permission strings are data consumed by roles; route renames can be authorization migrations (`pkg/auth/permission.go` `AddPermission`).
- Internal distributed requests also require permissions. Verify non-admin role contents when changing fanout APIs.
- Swagger annotations and runtime routes have drift; update annotations and regenerate only after checking `SetRoutes`.
- Manager API exposure and authentication are deployment-sensitive and not secured in this repository.
