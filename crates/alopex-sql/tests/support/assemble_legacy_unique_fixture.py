"""Assemble seven authenticated old-producer outputs without modifying KV bytes.

Identity input must contain public-safe, normalized provenance, never raw logs.
"""
import argparse
import hashlib
import json
from pathlib import Path

CASES = ("column", "named", "composite", "nullable", "duplicate",
         "named_duplicate", "composite_duplicate")
COMMIT = "d3917e1fa21d41097742a971e14094c0d6ae397e"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases-dir", type=Path, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    identity = json.loads(args.identity.read_text())
    assert identity["source_commit"] == COMMIT
    assert identity["parser_contract"] == "0.25.0"
    for field in ("source_manifest_sha256", "producer_source_sha256", "producer_binary_sha256",
                  "dependencies_sha256", "parser_record_sha256", "compiler", "commands"):
        assert identity[field], f"missing provenance: {field}"
    # This rejects personal absolute paths in public provenance. The operator must
    # additionally inspect command hostnames and other environment-specific text.
    public_text = json.dumps(identity)
    assert "/Users/" not in public_text and "/home/" not in public_text
    cases, records = [], []
    for name in CASES:
        path = args.cases_dir / f"{name}.json"
        raw = path.read_bytes()
        case = json.loads(raw)
        assert case["schema"] == "alopex-sql-kv-fixture/v1"
        assert case["source_commit"] == COMMIT and case["parser_contract"] == "0.25.0"
        assert case["case"] == name
        keys = [bytes.fromhex(entry[0]) for entry in case["entries"]]
        assert keys == sorted(set(keys))
        for _, value in case["entries"]:
            bytes.fromhex(value)
        cases.append(case)
        records.append({"case": name, "sha256": hashlib.sha256(raw).hexdigest(),
                        "sql_sha256": hashlib.sha256(case["sql"].encode()).hexdigest(),
                        "entries": len(keys)})
    raw = (json.dumps({"schema": "alopex-sql-kv-fixtures/v1", "cases": cases},
                      indent=2, sort_keys=True) + "\n").encode()
    manifest = {"schema": "alopex-sql-kv-fixture-provenance/v1", "identity": identity,
                "cases": records, "fixture_sha256": hashlib.sha256(raw).hexdigest()}
    assert not args.output.exists() and not args.manifest.exists()
    with args.output.open("xb") as stream:
        stream.write(raw)
    with args.manifest.open("x") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    main()
