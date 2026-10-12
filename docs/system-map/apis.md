# APIs

> **Purpose:** Map API groups, route ownership, middleware, authentication, and compatibility boundaries.
> **Last verified:** 2026-10-12
> **Verified against commit:** `f823d568efdf005582209d51df919f037ad26539`
> **Relevant source paths:** `pkg/routes/`, `pkg/handlers/`, `cmd/zinc-proxy/http.go`, `cmd/cluster-manager/http.go`, `docs/`

## Source of Truth

Compute routes are registered in `pkg/routes/routes.go` `SetRoutes`; setup middleware is in `pkg/routes/setup.go` `Setup`. Proxy and manager have independent routers in `cmd/zinc-proxy/http.go` and `cmd/cluster-manager/http.go` `SetupHttp`.

Generated Swagger is an incomplete annotation-derived view, not route truth. See [Development](development.md#generated-and-data-files) for regeneration and edit constraints.

## Middleware and Authentication

The compute router installs recovery, access logging, pprof, optional Prometheus, and permissive CORS (`pkg/routes/setup.go` `Setup`; `pkg/routes/routes.go` `SetRoutes`).

`pkg/routes/middleware.go` `AuthMiddleware` requires HTTP Basic credentials and an exact route permission string. `admin` bypasses permission lookup (`pkg/auth/auth.go` `VerifyCredentials`, `VerifyRoleHasPermission`). There are no sessions/JWTs; `/api/login` validates credentials and returns user information (`pkg/handlers/auth/login.go` `Login`).

`ClusterMiddleware` splits the path `target` on commas and calls `cluster.AssignCheck` on each literal name, returning HTTP 406 on mismatch. It is attached only to selected routes; absence does not imply the handler has no ownership check. Internal search routes deliberately have authentication without this middleware so query nodes can read assigned subsets from metadata (`pkg/routes/routes.go` `SetRoutes`; `pkg/routes/middleware.go` `ClusterMiddleware`; `pkg/cluster/cluster.go` `AssignCheck`; `pkg/core/loadindexes.go` `GetZincIndexFromMetadata`).

Public compute surfaces include `/version`, `/healthz`, `/api/login`, ES discovery/license/xpack responses, optional Swagger, pprof, and optional metrics (`pkg/routes/routes.go`; `pkg/routes/pprof.go` `SetPProf`; `pkg/routes/prometheus.go` `SetPrometheus`). Health is a shallow constant response, not a dependency check (`pkg/meta/healthz.go` `GetHealthz`).

## Compute Route Groups

| Group | Principal paths | Handlers |
|---|---|---|
| Operational | `GET /version`, `/healthz`, `/swagger/*any`, pprof, metrics | `pkg/meta/`, `pkg/routes/` |
| Internal distributed search | `POST /api/internal/get_stats`, `/api/internal/unified_search`, `/api/internal/vector_search` | `pkg/handlers/search/` internal handlers |
| Unified search | `POST /api/unified_search` | `search.UnifiedSearch` |
| Auth/admin | `/api/login`, `/api/user`, `/api/role`, `/api/permissions` | `pkg/handlers/auth/` |
| Native index | `/api/index`, `/api/index/:target`, mapping/settings/refresh/analyze | `pkg/handlers/index/` |
| Native documents | `/api/_bulk`, `/api/:target/_bulk`, `_multi`, `_bulkv2`, `_doc`, `_update` | `pkg/handlers/document/` |
| Native text search | `POST /api/:target/_search` | `search.SearchV1` |
| Vector | `POST /api/:target/_search/vector`, `/:target/:field/_seal`, `/:target/_recall` | `pkg/handlers/search/search_vector.go`; `pkg/handlers/index/vector_index.go` |

Consult `pkg/routes/routes.go` `SetRoutes` for exact methods and permission strings before changing a route.

### Compute/Proxy Matrix

This is a routing boundary summary, not an endpoint inventory. Evidence: compute `pkg/routes/routes.go` `SetRoutes`; proxy `cmd/zinc-proxy/http.go` `SetupHttp`.

| Family | Compute middleware/ownership | Proxy behavior |
|---|---|---|
| Native target search/docs, mapping/settings, refresh, `_multi`, vector/recall/seal | Basic permission + `ClusterMiddleware` | Usually owner-forwarded; vector may distribute; `_seal` absent (see drift below) |
| Native create without path target; index existence | Basic permission, no cluster middleware; `CreateIndexWorker` checks ownership | Create resolves body/path owner; existence reads metadata locally |
| Target `_bulk`/`_bulkv2` vs targetless forms | Target forms add cluster middleware; all require Basic permission; workers resolve indexes | NDJSON bulk groups by owner; JSON bulkv2 forwards one owner |
| Target ES `_search`/`_msearch` | Basic + ES header + alias + cluster middleware | Search always internal unified pipeline; msearch owner-grouped ordinary requests |
| Targetless ES `_search`/`_msearch` | Basic + ES header + alias middleware, no cluster middleware | Search lists metadata names; msearch uses header/default names, not implicit all-index fanout |
| ES create, existence, mapping/settings/analyze | Basic + ES header, no cluster middleware; inspect handler checks | Create/existence special handlers, others owner-forwarded or local targetless analyze |
| Internal stats/unified/vector; public unified | Basic permission, no cluster middleware; stats/partial paths reconstruct metadata | Internal routes not exposed; public unified calls internal compute endpoints |

For operation formats and semantics see [Document Write](request-flows.md#document-write) and [Bulk Write](request-flows.md#bulk-write): `_doc`/ES `_create` are replacement/upsert, not exclusive creation; `_update` is a separate raw-body replacement path with optional `insert=true`, not ES patch merging.

## Target Resolution

- Compute `IndexAliasMiddleware` only looks up the **entire exact path target** as one alias, replacing it with comma-joined indexes. It is registered on ES search/msearch/delete-by-query, not generic document/index routes. It does not expand individual comma tokens, wildcard aliases, or msearch body headers (`pkg/routes/middleware.go` `IndexAliasMiddleware`; `pkg/routes/routes.go` `SetRoutes`).
- Compute `searchIndex` dispatches empty targets, multiple names, or a first name starting/ending in `*` to `core.MultiSearch`. `GetMatchedIndexes` matches loaded local indexes using exact names, empty/all, leading-`*` suffix matching, or trailing-`*` prefix matching, not general glob syntax. Cluster middleware on target routes runs **before** this expansion and hashes literal tokens, so wildcard cluster behavior is not equivalent to checking each expanded index (`pkg/handlers/search/search_v2.go` `searchIndex`; `pkg/core/multi_search.go` `GetMatchedIndexes`, `isMatchIndex`; `pkg/cluster/cluster.go` `AssignCheck`).
- Proxy installs no `IndexAliasMiddleware`. ES `SearchDSL` splits explicit targets on commas, then `calcNodeIndexMap` performs exact metadata `Get`; wildcard/alias names are not expanded there and missing keys are skipped. Targetless ES search lists all metadata names. Unified and vector requests likewise use exact names; msearch groups literal header/path names by owner. `directForwarding` hashes the entire path target, without comma/wildcard/alias expansion (`cmd/zinc-proxy/search.go` `SearchDSL`, `MultiSearch`; `parallel_query.go` `calcNodeIndexMap`; `vector.go` `SearchVector`; `http.go` `directForwarding`; `proxy.go` `GetAddrByIndex`).

See [Architecture](architecture.md) and [Operations](operations.md) for prefix ownership; [Request Flows](request-flows.md#distributed-text-search) owns merge/scoring limitations. Route/target syntax alone does not promise Elasticsearch-equivalent behavior.

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
| Reverse proxy to owner | Native search, documents, mapping/settings, JSON bulkv2 | `http.go` `directForwarding`; `bulk.go` `BulkV2`; `utils.go` `ProxyPool.Get` |
| Parse and fan out/internal calls | Index delete, NDJSON bulk, msearch, all ES/unified search, eligible IVFPQ | `bulk.go`, `search.go`, `parallel_query.go`, `vector.go` |

Locally authenticated proxy routes use `cmd/zinc-proxy/middleware.go` `AuthMiddlewareNoCache`, which reads users/roles from shared metadata. Forwarded routes preserve `Authorization`; parsed fanout uses `fetchHTTP` to copy it. ES/unified fanout is therefore authorized by **internal** compute permissions, not the original ES/public unified permission. Multi-node execution additionally requires `search.internal.GetStats`; the unified internal permission is spelled `search.internal.UnifySearch` (`cmd/zinc-proxy/utils.go` `fetchHTTP`, `ProxyPool.Get`; `pkg/routes/routes.go` `SetRoutes`).

Proxy `/api/login` uses shared `pkg/handlers/auth/login.go` `Login` -> cached `auth.VerifyCredentials`, not proxy direct-metadata verification. Initial user/role cache population belongs to `auth.InitFirstUser` (`firststart.go` `isFirstStart`, `initPermissionCache`); `auth.Init` only starts cluster update consumers (`cached_users.go` `Init`). Compute startup calls both, proxy startup neither. Thus a freshly started proxy does not load existing users into its login cache and valid metadata credentials can yield HTTP 200 with `validated:false`; local user create/update can later populate entries, so this is not a claim that proxy login can never work. [Processes](processes.md) owns startup/watch sequencing (`cmd/zincsearch/main.go` `main`; `cmd/zinc-proxy/main.go` `main`; `pkg/auth/auth.go` `VerifyCredentials`; `pkg/auth/createuser.go` `CreateUser`).

Known route drift: proxy registers `POST /api/:target/:field/_rebuild`, while compute registers `POST /api/:target/:field/_seal`. Verify and align source routes before relying on `_rebuild` (`cmd/zinc-proxy/http.go` `SetupHttp`; `pkg/routes/routes.go` `SetRoutes`).

There is no generic catch-all forwarding. Proxy does not register compute `/version`, `/healthz`, pprof/metrics/Swagger, internal search APIs, ES discovery/license/xpack, aliases, data streams, or `_seal`; those compute surfaces are not automatically available through the proxy. Proxy ES routes also do not globally install `ESMiddleware` (targetless ES analyze explicitly does), so locally handled responses need not carry compute compatibility headers (`cmd/zinc-proxy/http.go` `SetupHttp`).

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
