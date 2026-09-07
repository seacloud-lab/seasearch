# SeaSearch - Multi-tenant Search Engine with Full-text Index and Vector Index

**SeaSearch** is a multi-tenant search engine built on open source search engine ([ZincSearch](https://zincsearch-docs.zinc.dev/)), implemented in Go language. Our goal is to implement a lightweight search engine that can support unlimited number of indexes. In a multi-tenant setting (such as SaaS), this allows indexing each tenant's data independently. With existing search engines, such as ElasticSearch, you have to manually shard your index when it gets very large if all the tenants' data are added into a single index.

## SeaSearch vs. ElasticSearch:

- **Lightweight**: SeaSearch is implemented in Go language. So it's more lightweight than ElasticSearch, which is implemented in Java.
- **No Limit on the Number of Indexes**: SeaSearch can support large number of indexes in the system. With this feature, we can create one index for each tenant or project (or other small unit for your application). This limits the amount of data that needs to be searched for queries. With ElasticSearch, we have to save data from all tenants or projects into one index, which is not good for performance when you have large amount of data.
- **Good Compatibility**: API compatible with ElasticSearch
- **Support S3 Storage**: SeaSearch can use S3 as storage
- **Shared Storage Architecture for Cluster**: ElasticSearch's cluster architecture is based on replication of data among nodes. It's complex to maintain and not easy to scale. SeaSearch uses a shared-storage architecture. Cluster nodes share the same storage (usually S3 compatible object storage). With this architecture, it's easier to provide HA guarantees and easier to maintain. It's also possible to scale query performance by using more query nodes.
- **Vector Search**: SeaSearch provides more lightweight vector search implementation compared to ElasticSearch. You can choose Flat, HNSW and IVFPQ vector indexes.
