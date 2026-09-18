# SeaSearch System Map

> **Purpose:** Entry point and maintenance contract for the repository system map.
> **Last verified:** 2026-09-18
> **Verified against commit:** `c914b4cc84f87197c03009bafefc06f3f513700b`
> **Relevant source paths:** `AGENTS.md`, `README.md`, `cmd/`, `pkg/`, `go.mod`

This map is a navigation aid for future agents. It summarizes verified relationships and points to durable source evidence, but it is not a replacement for checking the relevant implementation before making a change. Source code and configuration are authoritative when they disagree with these documents.

## Documents

| Document | Covers |
|---|---|
| [Architecture](architecture.md) | System boundaries, deployment modes, component ownership, and dependencies |
| [Processes](processes.md) | Executables, startup/shutdown order, and process interactions |
| [Codebase layout](codebase-layout.md) | Important directories and package responsibilities |
| [Request flows](request-flows.md) | End-to-end indexing, text search, vector search, and cluster routing flows |
| [Data and storage](data-storage.md) | Metadata, text/vector persistence, WAL, cache, and ownership conventions |
| [APIs](apis.md) | Route groups, middleware, authentication, compatibility surface, and handler locations |
| [Configuration](configuration.md) | Configuration loading, precedence, important variables, and validation |
| [Operations](operations.md) | Background work, clustering, observability, failover, and operational constraints |
| [Development](development.md) | Build/test constraints, CI, generation, native dependencies, and generated files |

## Reading Guide

| Task | Read first |
|---|---|
| Understand the system or choose a package | [Architecture](architecture.md), then [Codebase layout](codebase-layout.md) |
| Change startup, shutdown, or a binary | [Processes](processes.md), then [Operations](operations.md) |
| Change an HTTP endpoint or authorization | [APIs](apis.md), then [Request flows](request-flows.md) |
| Change indexing, search, or vectors | [Request flows](request-flows.md), then [Data and storage](data-storage.md) |
| Change metadata, object storage, WAL, or cache | [Data and storage](data-storage.md), then [Configuration](configuration.md) |
| Change cluster routing or failover | [Architecture](architecture.md), [Processes](processes.md), and [Operations](operations.md) |
| Build, test, lint, or regenerate Swagger | [Development](development.md) |

## Maintenance

Update this map in the same change whenever architecture, process responsibilities, major flows, APIs, storage, or configuration change.

1. Edit only the affected focused documents; avoid repeating details already owned elsewhere.
2. Verify statements against source and cite paths plus symbols. Do not rely on generated Swagger as the route source of truth.
3. Update `Last verified` and `Verified against commit` in every document actually rechecked. Use the commit being documented; if documentation is authored in an uncommitted worktree, record the current `git rev-parse HEAD` and verify the diff as well.
4. Check relative links, referenced paths, and symbol names before finishing.
5. Mark dependency- or deployment-dependent behavior as requiring source/runtime verification.

If implementation and map conflict, correct the map. Do not preserve obsolete map behavior for compatibility.
