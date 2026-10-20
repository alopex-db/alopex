<p align="center">
  <a href="https://alopex-db.github.io">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-horizontal-dark-800.png">
      <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-horizontal-light-800.png">
      <img alt="Alopex DB" src="https://raw.githubusercontent.com/alopex-db/alopex-db.github.io/main/docs/assets/images/brand/lockup-horizontal-light-800.png" width="420">
    </picture>
  </a>
</p>

<h3 align="center">Silent. Adaptive. Unbreakable.</h3>

<p align="center">
  <b>One Rust engine: SQL + vector search (HNSW) + columnar storage.</b><br>
  From a single embedded file to a distributed cluster — same data model, same API.
</p>

<p align="center">
  <a href="https://alopex-db.github.io">Documentation</a> &bull;
  <a href="https://alopex-db.github.io/getting-started/quickstart/">Quick Start</a> &bull;
  <a href="https://alopex-db.github.io/roadmap/">Roadmap</a> &bull;
  <a href="https://github.com/alopex-db/alopex/releases">Releases</a> &bull;
  <a href="https://github.com/alopex-db/alopex/issues">Issues</a> &bull;
  <a href="https://github.com/sponsors/asopitech">Sponsor</a>
</p>

<p align="center">
  <a href="https://github.com/alopex-db/alopex/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/alopex-db/alopex?sort=semver&label=release&color=1E3A5F"></a>
  <a href="https://crates.io/crates/alopex-embedded"><img alt="crates.io" src="https://img.shields.io/crates/v/alopex-embedded.svg?label=crates.io&color=5FB4C9"></a>
  <a href="https://pypi.org/project/alopex/"><img alt="PyPI" src="https://img.shields.io/pypi/v/alopex.svg?label=PyPI&color=5FB4C9"></a>
  <a href="https://github.com/alopex-db/alopex/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/alopex-db/alopex/ci.yml?branch=main&label=CI"></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/github/license/alopex-db/alopex?color=blue"></a>
  <a href="https://github.com/sponsors/asopitech"><img alt="Sponsor" src="https://img.shields.io/badge/sponsor-%E2%9D%A4-db61a2"></a>
  <a href="https://github.com/alopex-db/alopex/stargazers"><img alt="Stars" src="https://img.shields.io/github/stars/alopex-db/alopex?style=flat&color=7EC8E3"></a>
</p>

---

## Install

```bash
# Rust — embedded, like SQLite
cargo add alopex-embedded alopex-sql

# Python
pip install alopex

# Command line (TUI + batch)
cargo install alopex-cli
```

Prebuilt CLI binaries for Linux, macOS, and Windows are attached to
[every release](https://github.com/alopex-db/alopex/releases/latest). Server and
cluster builds are the `alopex-server` and `alopex-cluster` crates.

## Hello, hybrid search

Vectors are a column type, not a bolt-on. Filter with SQL and rank by similarity
in one statement, inside one transaction.

```sql
CREATE TABLE knowledge_chunks (
    id         INTEGER PRIMARY KEY,
    content    TEXT,
    category   TEXT,
    created_at TIMESTAMP,
    embedding  VECTOR(1536)
);

CREATE INDEX knowledge_hnsw ON knowledge_chunks (embedding)
USING HNSW WITH (m = 16, ef_construction = 200);

SELECT content, vector_similarity(embedding, ?) AS score
FROM knowledge_chunks
WHERE category = 'research' AND created_at > DATE '2026-01-01'
ORDER BY score DESC
LIMIT 5;
```

```python
import alopex

db = alopex.Database.open("./my_data")
db.execute_sql("SELECT content FROM knowledge_chunks WHERE category = ?", ["research"])
hits = db.search_hnsw("knowledge_chunks", query_embedding, k=10)
```

`vector_similarity` ranks nearest-first with `DESC`; `vector_distance` ranks
nearest-first with `ASC`. HNSW's `ef_search` trades latency for recall and can be
set per query.

→ [SQL + Vector guide](https://alopex-db.github.io/guides/sql-vector/) ·
[Python guide](https://alopex-db.github.io/guides/python/) ·
[Quick start](https://alopex-db.github.io/getting-started/quickstart/)

## Why Alopex DB

A retrieval stack usually means SQLite for local state, a vector database for
embeddings, and something distributed for when it grows — three systems to sync,
three operational stories, three sets of semantics. Alopex DB is one engine that
changes shape instead.

| | Typical stack | Alopex DB |
|:--|:--|:--|
| **Systems to run** | Embedded DB + vector DB + distributed SQL | One engine |
| **Vector search** | A separate service, eventually consistent with your rows | `VECTOR(N)` column inside the same ACID transaction |
| **Growing up** | Rewrite the data access layer | Same data model from embedded file to cluster |
| **Language** | Mixed | Pure Rust, memory-safe, no C toolchain required |

## What it does today

- **SQL frontend** — DDL/DML, JOIN and subqueries, GROUP BY and aggregates,
  window functions and CTEs, `EXPLAIN` / `EXPLAIN ANALYZE`, transactions,
  constraints, sequences, and `COPY`.
- **Types beyond scalars** — exact `DECIMAL`/`NUMERIC`, native
  `DATE`/`TIME`/`INTERVAL`, `JSON`/`JSONB` with path operators, nested
  `ARRAY`/`MAP`/`STRUCT`, and deterministic full-text search.
- **Vector search** — `VECTOR(N)` as a first-class type, HNSW indexing with
  query-local `ef_search`, hybrid SQL + similarity queries, ACID throughout.
- **Storage engine** — custom LSM-tree with WAL, MVCC snapshot isolation, and
  columnar segments with compression, statistics, and predicate pushdown.
- **Surfaces** — embedded library, HTTP/gRPC server, cluster-aware deployment,
  DataFrame API, Python wheels, and a CLI that opens a TUI on a terminal.

→ [Architecture](https://alopex-db.github.io/concepts/architecture/) ·
[Vector search](https://alopex-db.github.io/concepts/vector-search/)

## Any scale, one engine

| Mode | Use case | Shape |
|:--|:--|:--|
| **Embedded** | Mobile, local RAG, edge devices | Single library or binary, like SQLite |
| **Single node** | Microservices, dev and test | Standalone server over HTTP/gRPC |
| **Replicated** | High availability, read-heavy | Primary–replica with failover |
| **Distributed** | Large-scale production | Multi-Raft cluster, range sharding |

→ [Deployment modes](https://alopex-db.github.io/concepts/modes/)

## Project status and limits

Alopex DB is **pre-1.0 and under active development**. We publish often and we
are explicit about the edges:

- The public crates and the Python package move as one aligned version; the
  badge above is the current one.
- Public APIs can still change between minor versions. Pin exact versions in
  production and read the [CHANGELOG](CHANGELOG.md) before upgrading — releases
  state their compatibility and migration notes.
- Some SQL administration surfaces (`COPY`, constraints, sequences, and parts of
  advanced DML) are **single-node only** today. Their boundaries are written
  down in [docs/sql-mutations-v0.8.11.md](docs/sql-mutations-v0.8.11.md).
- Distributed capability parity (v0.9) and the **v1.x Graph / Lake-Link track**
  (Property Graph catalog, SQL/PGQ, Graph ETL, GraphRAG, S3 tiering) are
  **planned, not shipped**. Each phase states its acceptance boundary on the
  [roadmap](https://alopex-db.github.io/roadmap/#graph-lake-link); nothing in
  this README describes them as available today.
- HNSW recall, latency, and hybrid-search claims are only those backed by the
  [diagnostic contract](docs/hnsw-v0811-diagnostics.md); notebook timings are
  demonstrations, not benchmarks.

> [!IMPORTANT]
> **One data directory, one process.** A directory passed to `--data-dir` can be
> opened by exactly one process at a time. Pointing the CLI or an embedded
> program at a running server's `data_dir` fails with `already open by another
> process` — and before v0.8.7 it silently corrupted the WAL and SSTables. Run
> one server and connect through `--profile`, HTTP, or gRPC.
> See [docs/single-process-lock.md](docs/single-process-lock.md).

## Try it locally

```bash
# Embedded key-value: CRUD, flush, WAL replay
./examples/embedded-kv/demo.sh

# Vector: flat-search benchmark on 10k x 128, then upsert/search with filters
./examples/embedded-vector/demo_vector.sh

# CLI against a local data directory
cargo run -p alopex-cli -- --data-dir ./data sql "SELECT 1"
```

## The Alopex family

| Project | What it is | Latest |
|:--|:--|:--|
| [**alopex**](https://github.com/alopex-db/alopex) | This repo — the unified engine, CLI, server, and Python package | [![release](https://img.shields.io/github/v/release/alopex-db/alopex?sort=semver&label=%20)](https://github.com/alopex-db/alopex/releases/latest) |
| [**alopex-skulk**](https://github.com/alopex-db/alopex-skulk) | Time-series database on the same core — Parquet columnar, TTL and downsampling, embedded PromQL and SQL-TS | [![crates.io](https://img.shields.io/crates/v/alopex-skulk.svg?label=%20)](https://crates.io/crates/alopex-skulk) |
| [**alopex-chirps**](https://github.com/alopex-db/alopex-chirps) | Gossip and messaging mesh over UDP/QUIC — the cluster control plane | [![crates.io](https://img.shields.io/crates/v/alopex-chirps.svg?label=%20)](https://crates.io/crates/alopex-chirps) |
| [**alopex-ui**](https://github.com/alopex-db/alopex-ui) | Alopex Data UI — one DataSource contract for forms, grids, charts, graphs, and maps | In development |

Two more designs are published and open for comment: **Trail** (logs and events,
schema binds on read) and **Alopex OTel** (storage and dashboards in one
product). Read [Trail](https://alopex-db.github.io/concepts/trail/) and
[OTel](https://alopex-db.github.io/concepts/otel/) — the APIs are not frozen, so
what you need can still change them.

## Alopex Enterprise

A commercial feature package built on this OSS foundation — security suite,
unified query model with enterprise search, SLO-based observability, and
platform infrastructure services. It is **in architecture design**, not
generally available, and it is not a support tier.
[Discuss a use case](https://asopi.tech/en/services/alopex-enterprise).

## Contributing

Apache-2.0, and contributions are welcome — especially from engineers who enjoy
Rust, storage engines, distributed systems, and vector search.

- Good first stops: the [issues](https://github.com/alopex-db/alopex/issues) and
  the [contributing guide](https://alopex-db.github.io/contributing/).
- Compatibility-sensitive work must follow the
  [reference compatibility development policy](docs/reference-compatibility-development-policy.md).
- Questions and bug reports belong in Issues; see
  [SUPPORT](https://github.com/alopex-db/.github/blob/main/SUPPORT.md) for where
  each project is tracked.

## Sponsor this work

Alopex DB is built in the open by [@asopitech](https://github.com/asopitech).
GitHub Sponsors funds continued development across Alopex DB, jv-lang, GraphRAG,
and related tools — and higher tiers let you follow the work before it is public.

| Tier | What you get |
|:--|:--|
| **$3 / month** | OSS Support — back ongoing development, updates, and releases |
| **$12 / month** | Early Access — experiments, design drafts, internal notes, and prototypes long before they are public |
| **$99 / month** | Enterprise-focused Early Access — pre-release capabilities aimed at security and deployment across the asopitech ecosystem |

Sponsorship is not an Alopex Enterprise license, SLA, or commercial support
contract; current benefits are defined on the Sponsors page.

<p align="center">
  <a href="https://github.com/sponsors/asopitech?metadata_source=alopex_readme&metadata_campaign=sponsor_early_access&metadata_content=readme_sponsor_section"><img alt="Sponsor @asopitech on GitHub Sponsors" src="https://img.shields.io/badge/GitHub%20Sponsors-%E2%9D%A4%20Sponsor%20%40asopitech-db61a2?logo=githubsponsors&logoColor=white&style=for-the-badge"></a>
  <a href="https://buymeacoffee.com/asopitechia"><img alt="Buy Me a Coffee" src="https://img.shields.io/badge/Buy%20Me%20a%20Coffee-%E2%98%95-FFDD00?style=for-the-badge&logo=buymeacoffee&logoColor=black"></a>
</p>

If Alopex DB is useful to you, a ⭐ on the repository genuinely helps other
people find it.

## License

[Apache License 2.0](LICENSE).

<p align="center">
  <sub>Built with 🦀 Rust and ❤️ by the Alopex DB team · <a href="https://alopex-db.github.io">alopex-db.github.io</a></sub>
</p>
