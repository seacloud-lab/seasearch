# SeaSearch — Multi-Tenant Search Engine with Full-Text and Vector Indexing

**SeaSearch** is a multi-tenant search engine built on top of the open-source search engine [ZincSearch](https://zincsearch-docs.zinc.dev/) and implemented in Go. Our goal is to provide a lightweight search engine that can support an unlimited number of indexes.

In a multi-tenant environment, such as a SaaS application, this allows each tenant's data to be indexed independently. With traditional search engines such as Elasticsearch, when all tenants' data is stored in a single index, the index may eventually become too large and require manual sharding. With SeaSearch, each tenant can have its own index, making it easier to manage and scale large numbers of tenants.

## SeaSearch vs. Elasticsearch

* **Lightweight**: SeaSearch is implemented in Go and has a smaller runtime footprint than Elasticsearch, which is built on the JVM.
* **No Practical Limit on the Number of Indexes**: SeaSearch is designed to support a large number of indexes. This makes it possible to create a separate index for each tenant, project, or other logical unit in an application. Queries can then be restricted to the relevant index, reducing the amount of data that needs to be searched. With Elasticsearch, applications often store data from many tenants or projects in the same index, which can become less efficient as the data volume grows.
* **Elasticsearch API Compatibility**: SeaSearch provides an API compatible with Elasticsearch, making it easier to integrate with existing applications.
* **S3-Compatible Storage**: SeaSearch can use S3-compatible object storage as its storage backend.
* **Shared-Storage Cluster Architecture**: Elasticsearch clusters replicate data across nodes, which can make cluster management and scaling relatively complex. SeaSearch uses a shared-storage architecture in which cluster nodes share the same storage backend, typically S3-compatible object storage. This simplifies cluster management and makes it easier to provide high availability. Query performance can also be scaled horizontally by adding more query nodes.
* **Vector Search**: SeaSearch provides a lightweight vector search implementation with support for Flat, HNSW, and IVFPQ vector indexes.

## Architecture

SeaSearch uses a shared-storage architecture.

<img width="2160" height="818" alt="image" src="https://github.com/user-attachments/assets/cafead47-ec61-4c29-86f3-5617ee1f57b0" />

A SeaSearch cluster consists of the following types of nodes:

* **SeaSearch compute node**: Handles index read and write requests. All compute nodes share the same S3-compatible storage backend, where the index data is stored.
* **SeaSearch proxy (or gateway)**: Distributes client requests among SeaSearch compute nodes.
* **Etcd**: Stores cluster metadata, including index metadata and the data distribution map.
* **SeaSearch cluster manager**: Monitors the health of SeaSearch compute nodes and redistributes index ownership among the nodes when necessary.

In a single-node deployment, SeaSearch uses a local KV database (bbolt) to store index metadata and the local file system to store index data.

### Data Distribution and Failover

Because all compute nodes share the same storage backend, SeaSearch only needs to distribute **index ownership** among nodes rather than moving or replicating the actual index data.

* All indexes are grouped into a fixed number of partitions based on a hash of their names.
* The cluster manager maintains a map that determines which node currently owns each index partition. This map is stored in Etcd.
* The SeaSearch proxy routes requests for an index to the compute node that owns the corresponding partition.
* Whenever a compute node fails or a new node is added, the cluster manager recomputes the ownership map and transfers partition ownership between nodes as needed.

Because updating the cluster configuration does not require transferring large amounts of data, SeaSearch can efficiently manage a large number of indexes.

### Local Cache

When handling requests, compute nodes may need to retrieve index data from S3 storage, which can introduce additional latency. To reduce this latency, SeaSearch compute nodes cache index data on their local disks.

This caching strategy is feasible because index data is organized into immutable segments. Once created, an index segment can only be read or deleted; its contents are never modified.

To support queries against indexes that are larger than the available local disk space, compute nodes use a rotating cache. When a new segment needs to be cached and the cache has reached its size limit, older segments are evicted to make room.

With this design, clients typically experience higher latency only for the first request after an index segment has been evicted or when a node starts up. Once the cache is warmed up, subsequent requests can be served at speeds comparable to those of local storage.

In our experience, the warm-up latency can be further reduced by taking advantage of the high network bandwidth available in modern data centers. During the warm-up stage, multiple index segments can be retrieved from S3 in parallel, significantly accelerating index loading.
