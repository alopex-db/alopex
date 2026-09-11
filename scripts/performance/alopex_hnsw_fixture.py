"""Create or verify a persisted AlopexDB HNSW search fixture."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from hnsw_v0811_contract import GLOVE_SHA256


def _size_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def prepare(dataset: Path, output: Path, size: int, commit: str) -> dict[str, object]:
    import alopex
    import h5py
    import numpy as np

    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"fixture directory is not empty: {output}")
    digest = hashlib.sha256(dataset.read_bytes()).hexdigest()
    if digest != GLOVE_SHA256:
        raise ValueError(f"unexpected GloVe checksum: {digest}")
    output.mkdir(parents=True, exist_ok=True)
    with h5py.File(dataset) as source:
        if size > len(source["train"]):
            raise ValueError(f"requested N={size} exceeds dataset rows")
        vectors = np.asarray(source["train"][:size], dtype=np.float32)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    db = alopex.Database.open(str(output))
    name = "glove_hnsw"
    db.create_hnsw_index(
        name,
        alopex.HnswConfig(
            vectors.shape[1],
            m=16,
            ef_construction=200,
            metric=alopex.Metric.COSINE,
        ),
    )
    try:
        with db.begin(alopex.TxnMode.READ_WRITE) as transaction:
            for index, vector in enumerate(vectors):
                transaction.upsert_to_hnsw(name, str(index).encode(), vector, None)
            transaction.commit()
        db.flush()
        stats = db.get_hnsw_stats(name)
    finally:
        db.close()
    manifest = {
        "schema": "alopex.hnsw-search-fixture/v1",
        "dataset": "glove-100-angular",
        "dataset_sha256": digest,
        "alopex_commit": commit,
        "file_format_version": None,
        "file_format_version_reason": "not exposed by the Python API",
        "hnsw": {"name": name, "dimension": int(vectors.shape[1]), "m": 16, "ef_construction": 200, "metric": "cosine"},
        "node_count": int(stats.node_count),
        "fixture_bytes": _size_bytes(output),
        "search_only": True,
    }
    (output / "fixture-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def verify(fixture: Path) -> dict[str, object]:
    import alopex

    manifest = json.loads((fixture / "fixture-manifest.json").read_text(encoding="utf-8"))
    db = alopex.Database.open(str(fixture))
    try:
        stats = db.get_hnsw_stats(str(manifest["hnsw"]["name"]))
    finally:
        db.close()
    observed = {"node_count": int(stats.node_count), "fixture_bytes": _size_bytes(fixture)}
    if observed["node_count"] != manifest["node_count"]:
        raise ValueError(f"fixture node count mismatch: {observed['node_count']} != {manifest['node_count']}")
    return {"schema": manifest["schema"], "valid": True, **observed}


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--dataset", type=Path, required=True)
    prepare_parser.add_argument("--output", type=Path, required=True)
    prepare_parser.add_argument("--size", type=int, required=True)
    prepare_parser.add_argument("--commit", required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args.dataset, args.output, args.size, args.commit) if args.command == "prepare" else verify(args.fixture)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
