
# Alopex DB 🦊

<div align="center">

**Silent. Adaptive. Unbreakable.**

The unified database engine that scales from a single embedded file to a globally distributed cluster.  
Native SQL and Vector Search today, with a Property Graph track planned for v1.x.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Latest release](https://img.shields.io/github/v/release/alopex-db/alopex?sort=semver&label=latest)](https://github.com/alopex-db/alopex/releases/latest)
[![crates.io](https://img.shields.io/crates/v/alopex-embedded.svg)](https://crates.io/crates/alopex-embedded)
[![PyPI](https://img.shields.io/pypi/v/alopex.svg)](https://pypi.org/project/alopex/)
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

### 2. Native Vector Support and a Planned Graph Layer
Vectors are not an afterthought. Alopex treats `vector<float, N>` as a first-class citizen within ACID transactions.
* **Hybrid Search:** Filter by SQL metadata and sort by vector similarity in a single query.
* **Property Graph (planned for v1.x):** Keep relational tables as the source of truth, then add a graph catalog, transactional adjacency index, SQL/PGQ-style queries, and GraphRAG traversal.

### 3. Lake-Link Architecture (planned for v1.x)
Bridge the gap between your Data Lake (S3) and your AI Application.
* **Graph ETL:** Map CSV, Parquet, Arrow, DataFrame, and later S3 datasets into versioned graph generations.
* **Cold/Hot Tiering:** Export and rehydrate graph generations through the storage-backend contract.

> **Implementation status:** SQL, DataFrame operations, transactional local `COPY`, and HNSW vector search are implemented. Property Graph DDL/query, graph ETL, GraphRAG, S3 Lake-Link, and IVF are roadmap items rather than current capabilities.

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
CREATE INDEX knowledge_chunks_embedding_hnsw ON knowledge_chunks (embedding)
USING HNSW WITH (ef_search = 128);

SELECT content, vector_similarity(embedding, [0.1, 0.5, ...], 'cosine') AS score
FROM knowledge_chunks
WHERE created_at > '2024-01-01'
ORDER BY score DESC
LIMIT 5;
```

`vector_similarity` always ranks nearest vectors with `DESC`; `vector_distance`
always ranks nearest vectors with `ASC`. HNSW's optional `ef_search` index
setting trades query latency for recall.

### Property Graph and Lake-Link Direction (planned)

The v1.x design aligns the SQL surface with SQL/PGQ concepts instead of reviving the former `COPY ... INTO GRAPH ... MAP COLUMNS` sketch verbatim.

```sql
CREATE PROPERTY GRAPH wiki_graph
  VERTEX TABLES (pages KEY (id))
  EDGE TABLES (links SOURCE KEY (source_id) REFERENCES pages (id)
                     DESTINATION KEY (target_id) REFERENCES pages (id));

SELECT *
FROM GRAPH_TABLE(
  wiki_graph
  MATCH (source)-[link]->(target)
  COLUMNS (source.id AS source_id, target.id AS target_id)
);
```

Rust is the core typed API; SQL, Python, HTTP/gRPC, and CLI call the same catalog, planner, executor, and job lifecycle. The detailed scope is tracked in [issue #596](https://github.com/alopex-db/alopex/issues/596), the [v1.x roadmap](docs-internal/roadmap.md), and the [graph database comparison report](docs/graph-database-feature-performance-research.md).

-----

## 🛠 Architecture

Alopex DB is built in **Rust** for safety and performance.

  * **Storage Engine:** Custom LSM-Tree tailored for high-throughput vector writes.
  * **Consensus:** Raft (based on `raft-rs`) ensures consistency across distributed nodes.
  * **Vector Index:** HNSW is implemented. IVF remains an independent future index option and is not a prerequisite for the graph layer.

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

The [v0.8.11 HNSW diagnostic contract](docs/hnsw-v0811-diagnostics.md) defines
the reproducible recall, latency, hybrid-search, and scale evidence. Public
notebook timings are API demonstrations unless that document links them as a
release artifact.

The [v0.8.11 SQL mutation contract](docs/sql-mutations-v0.8.11.md) documents
constraints, joined DML, COPY, sequences, their introspection surfaces, and
their explicit single-node boundaries.

## 🧰 CLI (cargo)

Run the CLI from source (local/embedded):

```bash
cd alopex
cargo run -p alopex-cli -- --data-dir ./data sql "SELECT 1"
```

Use a server profile:

```bash
cd alopex
cargo run -p alopex-cli -- --profile prod sql "SELECT * FROM users"
```

> **⚠️ 1 データディレクトリ = 1 プロセス**
> `--data-dir` が指すディレクトリは、同時にただ 1 つのプロセスからしか開けない。
> 稼働中の `alopex-server` の `data_dir` を CLI や組み込みから直接開こうとすると、
> `already open by another process` エラーで失敗する（v0.8.7 までは黙って通り、
> WAL と SSTable が壊れた）。共有したいときはサーバーを 1 つだけ立て、
> `--profile` / HTTP / gRPC 経由で接続すること。
> 詳細: [docs/single-process-lock.md](docs/single-process-lock.md)

### crates.io から Embedded を使う

最新公開版を追加するには `cargo add` を使います。Alopex兄弟crateは同じpatch版へ固定されます。

```bash
cargo add alopex-embedded
```

通常は兄弟crateを個別にpinする必要はありません。既存のlockfileを最新公開版へ更新する場合は次を実行します。

```bash
cargo update -p alopex-embedded
cargo generate-lockfile
```

Rust向けparserは`alopex-sql`に静的同梱されます。実行時に`libalopex_sql_parser.so`などを検索する設定や、利用者crateの`build.rs`でのrpath追加は不要です。

-----

## 🛣 Roadmap

The current release badge at the top of this README updates automatically from
GitHub Releases. See the [published roadmap](https://alopex-db.github.io/roadmap/)
for the current and planned capability boundaries.

The v1.x Graph / Lake-Link work is divided into six acceptance-driven phases:
contract and API, graph catalog and adjacency index, SQL/PGQ query MVP, local
graph ETL, GraphRAG and incremental maintenance, then S3/tiering integration.
The implementation is intentionally completed in v1.x; v2.0 is the integration
and release-acceptance gate.

-----

## 🤝 Contributing

Alopex DB is an open-source project under the **Apache 2.0 License**.
We welcome contributions from engineers interested in Rust, Distributed Systems, and Vector Search.

Compatibility-sensitive development must follow the
[reference compatibility development policy](docs/reference-compatibility-development-policy.md).

-----

<div align="center">
<sub>Built with 🦀 and ❤️ by the Alopex DB Team.</sub>
</div>
# Discord hook test
# Discord hook test 2
# hook test 3
# hook test 4
# hook test 5
# hook test 6
# hook test 7
# hook test 8
# hook test 8
# hook test 9
# hook test 10
