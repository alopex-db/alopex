
# Alopex DB 🦊

<div align="center">

**Silent. Adaptive. Unbreakable.**

The unified database engine that scales from a single embedded file to a globally distributed cluster.  
Native SQL, Vector Search, and Graph capabilities in one Rust-based engine.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()
[![Status](https://img.shields.io/badge/status-pre--alpha-orange)]()

</div>

---

## 📖 Overview

**Alopex DB** is designed for the next generation of data-intensive applications—specifically those driving Agentic AI, RAG (Retrieval-Augmented Generation), and Edge Computing.

It solves the fragmentation problem: instead of gluing together SQLite, a Vector DB, and a Distributed SQL engine, **Alopex provides a single engine that adapts to your workload.**

### Core Philosophy: The "Alopex" (Arctic Fox) Traits
* **Silent (Swift & Light):** written in Rust with a zero-overhead embedded mode.
* **Adaptive (Flexible):** Seamlessly transitions from a local library to a multi-node cluster.
* **Unbreakable (Resilient):** Built on Raft consensus for extreme fault tolerance in distributed mode.

---

## 🌟 Key Features

### 1. Three Modes, One Engine
Start small, scale infinitely without changing your data model.

| Mode | Use Case | Architecture |
| :--- | :--- | :--- |
| **Embedded** | Mobile Apps, Local RAG, Edge Devices | Single Binary / Library (like SQLite) |
| **Single-Node** | Microservices, Dev/Test Envs | Standalone Server (Postgres-compatible*) |
| **Distributed** | High-Availability Production | Shared-nothing Cluster (Range Sharding + Raft) |

### 2. Native Vector & Graph Support
Vectors are not an afterthought. Alopex treats `vector<float, N>` as a first-class citizen within ACID transactions.
* **Hybrid Search:** Filter by SQL metadata and sort by vector similarity in a single query.
* **Graph-Ready:** Optimized specifically for Knowledge Graph storage (nodes/edges) alongside embeddings.

### 3. Lake-Link Architecture (Parquet Integration)
Bridge the gap between your Data Lake (S3) and your AI Application.
* **Zero-ETL Import:** Directly ingest Parquet files and transform them into indexed Vectors/Graphs on the fly.
* **Cold/Hot Tiering:** Offload older graph data to Parquet and re-hydrate instantly when needed.

---

## 🚀 Quick Look (Conceptual)

### SQL + Vector
Alopex extends standard SQL with vector operations.

```sql
-- Create a table with mixed data types (Structured + Vector)
CREATE TABLE knowledge_chunks (
    id UUID PRIMARY KEY,
    content TEXT,
    embedding VECTOR(1536), -- OpenAI compatible
    created_at TIMESTAMP
);

-- Hybrid Search: SQL Filter + ANN Search
SELECT content, cosine_similarity(embedding, [0.1, 0.5, ...]) as score
FROM knowledge_chunks
WHERE created_at > '2024-01-01'
ORDER BY score DESC
LIMIT 5;
````

### The "Lake-Link" Import

Turn raw Parquet data into a queryable Knowledge Graph.

```sql
COPY FROM 's3://datalake/wiki_dump.parquet'
INTO GRAPH wiki_graph
MAP COLUMNS (
    id => node_id,
    vector_col => embedding,
    links => edges -- Auto-generate graph edges from adjacency lists
);
```

-----

## 🛠 Architecture

Alopex DB is built in **Rust** for safety and performance.

  * **Storage Engine:** Custom LSM-Tree tailored for high-throughput vector writes.
  * **Consensus:** Raft (based on `raft-rs`) ensures consistency across distributed nodes.
  * **Vector Index:** Pluggable indexing (HNSW / IVF) that is sharded alongside data ranges.

-----

## 🎬 Demos

Embedded KV demo (basic CRUD + flush/WAL replay):

```bash
./examples/embedded-kv/demo.sh
```

This runs a minimal flow for the embedded key-value API to show transaction semantics and durability.

Vector demo:

```bash
./examples/embedded-vector/demo_vector.sh
```

What it does:
- Runs a flat search benchmark (`search_flat`) on 10k×128 vectors (cosine / L2) to show baseline performance.
- Executes embedded API E2E tests: vector upsert/search with filters and checksum corruption detection.

-----

## 🛣 Roadmap

  * **v0.1 (Current):** Embedded Key-Value store with basic Vector support.
  * **v0.2:** Single-Node Server with SQL parser and Parquet reader.
  * **v0.3:** Distributed Cluster MVP (Raft replication).
  * **v1.0:** Multi-region locality & Production readiness.

-----

## 🤝 Contributing

Alopex DB is an open-source project under the **Apache 2.0 License**.
We welcome contributions from engineers interested in Rust, Distributed Systems, and Vector Search.

See [CONTRIBUTING.md](https://www.google.com/search?q=CONTRIBUTING.md) for details.

-----

<div align="center">
<sub>Built with 🦀 and ❤️ by the Alopex DB Team.</sub>
</div>
# Discord hook test
# Discord hook test 2
# hook test 3
# hook test 4
# hook test 5
