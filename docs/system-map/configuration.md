# Configuration

> **Purpose:** Explain configuration sources, precedence, important environment groups, defaults, and validation.
> **Last verified:** 2026-09-18
> **Verified against commit:** `c914b4cc84f87197c03009bafefc06f3f513700b`
> **Relevant source paths:** `pkg/config/config.go`, `pkg/zutils/load_conf.go`, `cmd/zinc-proxy/config.go`, `cmd/cluster-manager/config.go`, `pkg/auth/firststart.go`

## Loading and Precedence

Each process calls `godotenv.Load()` with no path, ignores load errors, then recursively fills tagged structs through `pkg/zutils/load_conf.go` `LoadConfig`/`setField` (`pkg/config/config.go` `InitConfig`; proxy/manager config initializers).

Effective precedence is:

1. Non-empty process environment.
2. `.env` value loaded from the working directory, where the environment did not already define it.
3. `default=...` in the struct tag.
4. Go zero value.

An explicitly empty value is treated as absent and cannot override a non-empty default (`setField`). There are no command-line config flags or YAML/TOML loaders.

Signed integer fields accept human-size syntax through `docker/go-units`; durations use `time.ParseDuration`; unsigned integers are decimal only; slices are comma-split without whitespace trimming. Invalid values terminate through `log.Fatal` (`pkg/zutils/load_conf.go` `setField`).

## Compute Configuration

The complete schema and defaults are in `pkg/config/config.go` type `config` and nested structs.

| Area | Important variables and defaults |
|---|---|
| Server | `ZINC_SERVER_ADDRESS` empty, `ZINC_SERVER_PORT=4080`, TLS certificate/key empty, `GIN_MODE=release` |
| Paths/storage | `SS_DATA_PATH=./data`, `SS_STORAGE_TYPE=disk`, `ZINC_METADATA_STORAGE=bolt` |
| Search/indexing | `ZINC_BATCH_SIZE=1024`, `ZINC_MAX_RESULTS=10000`, `ZINC_MAX_DOCUMENT_SIZE=1m`, `ZINC_SHARD_GOROUTINE_NUM=3`, `ZINC_SHARD_MAX_SIZE=1073741824` |
| Text engine | `ZINC_ICE_COMPRESSOR=zstd`, `ZINC_ENABLE_TEXT_KEYWORD_MAPPING=false` |
| Cluster/etcd | `SS_CLUSTER_ENABLE=false`, `SS_CLUSTER_ID=1`, `SS_ETCD_PREFIX=/seasearch`, endpoints/credentials empty |
| Cache | `SS_MAX_OBJ_CACHE_SIZE=10GB` |
| WAL | `ZINC_WAL_ENABLE=false`, `ZINC_WAL_STORAGE_TYPE=disk`, `ZINC_WAL_SYNC_INTERVAL=1s`, SQL/RKV settings empty |
| Vectors | `SS_VECTOR_IVFPQ_THRESHOLD=100000`, `SS_VECTOR_HNSW_MAX_LOGS=10000` |
| API/observability | `ZINC_SWAGGER_ENABLE=true`, Prometheus/telemetry/Sentry/profiler disabled |
| Plugins | ES compatibility version; GSE enable/stop/HMM/dictionary settings |

`InitConfig` verifies `SS_DATA_PATH` is writable, applies the Ice compressor, and validates selected object/WAL settings (`pkg/config/config.go` `initConfig`, `checkS3`, `checkOss`, `checkWalConfig`). S3/OSS selection requires credentials, bucket, and endpoint. S3 part size is a plain unsigned MiB value multiplied by 1,048,576 during validation.

When cluster mode is enabled, etcd metadata is selected regardless of `ZINC_METADATA_STORAGE` (`pkg/metadata/storage.go` `InitMetaStorage`). Index metadata records storage type, and nodes cannot open an index using a different configured backend (`pkg/core/newindex.go` `getOpenConfig`).

`ZINC_SHARD_NUM` exists but current `core.NewIndex` hard-codes one first-layer shard. Do not infer that changing the variable changes new index layout (`pkg/config/config.go` `shard.Num`; `pkg/core/newindex.go` `NewIndex`).

## Object Storage

S3 variables are `SS_S3_ACCESS_ID`, `SS_S3_ACCESS_SECRET`, `SS_S3_BUCKET`, `SS_S3_ENDPOINT`, signature/HTTPS/path-style/region/SSE-C settings, and `SS_S3_PART_SIZE`. OSS variables are corresponding `SS_OSS_*` credentials, bucket, and endpoint (`pkg/config/config.go` types `s3`, `oss`).

Storage type matching is case-sensitive in validation and runtime switches. Supported runtime values are `disk`, `s3`, and `oss` (`pkg/config/config.go`; `pkg/core/newindex.go` `getOpenConfig`).

## WAL

Accepted constants are `disk`, `rkv`, `postgresql`, and `mysql` (`pkg/config/config.go`). SQL selection requires `ZINC_WAL_SQL_HOST`, `PORT`, `DB`, `USER`, and `PWD`; RKV requires `ZINC_WAL_RKV_ENDPOINT`. RKV implementation remains TODO despite passing validation (`pkg/wal/wal.go` `Open`).

## Bootstrap Credentials

`ZINC_FIRST_ADMIN_USER` and `ZINC_FIRST_ADMIN_PASSWORD` are read directly by `pkg/auth/firststart.go` `InitFirstUser`. They are required only when metadata contains no users; otherwise they are ignored.

## Proxy Configuration

`cmd/zinc-proxy/config.go` `ProxyConfig` defines:

- Listener: `SS_CLUSTER_PROXY_HOST=0.0.0.0`, `SS_CLUSTER_PROXY_PORT=4082`
- Logs: `SS_CLUSTER_PROXY_LOG_DIR=./log`, `SS_CLUSTER_PROXY_LOG_LEVEL=INFO`, `SS_LOG_TO_STDOUT`
- Query fanout: `SS_PARALLEL_QUERY_THRESHOLD=3`, `SS_PARALLEL_QUERY_NODE_LIMIT=3`
- Body limit: `ZINC_MAX_DOCUMENT_SIZE=1m`
- Manager: `SS_CLUSTER_MANAGER_URL`
- etcd: endpoints, prefix, username, password

The proxy uses credentials for application metadata etcd access. Its `clusterkit.Open` topology connection receives endpoints/prefix only (`cmd/zinc-proxy/proxy.go` `StartProxy`).

## Manager Configuration

`cmd/cluster-manager/config.go` `ClusterManagerConfig` defines listener `0.0.0.0:4081`, manager log settings, etcd endpoints, and prefix. It has no etcd username/password fields; `clusterkit.Open` receives endpoints/prefix (`cmd/cluster-manager/manager.go` `InitClusterManger`). Verify etcd authentication expectations in deployment and the external dependency.

## Sensitive Defaults and Verification

The compute config source contains non-empty default Sentry/profiler service values, including a profiler API key, although both features default off (`pkg/config/config.go` type `config`). Treat activation and secret policy as deployment/security review items.

Before changing defaults, check persistence compatibility, cluster-wide consistency, and existing environment manifests outside this repository; no deployment manifests were verified here.
