#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
RUNNER="${REPO_ROOT}/scripts/release/verify-release/run.sh"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/alopex-release-join.XXXXXX")"
trap 'rm -rf "${TEMP_ROOT}"' EXIT
VERSION="$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["workspace"]["package"]["version"])')"

python3 - "${TEMP_ROOT}/candidate.json" "${VERSION}" <<'PY'
import json
import sys

core_sha = "a" * 40
python_sha = "c" * 40
digest = "b" * 64
targets = [
    "x86_64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
]
version = sys.argv[2]
candidate = {
    "version": version,
    "core_tag": {"name": f"v{version}", "peeled_sha": core_sha},
    "python_tag": {"name": f"alopex-py-v{version}", "peeled_sha": python_sha},
    "core": {
        "status": "success",
        "published": True,
        "run_id": "1001",
        "head_sha": core_sha,
        "source_sha": core_sha,
        "peeled_sha": core_sha,
        "registry": "crates.io",
        "crates": [{"name": "alopex-core", "status": "published"}],
    },
    "python": {
        "status": "success",
        "published": True,
        "run_id": "1002",
        "head_sha": python_sha,
        "peeled_sha": python_sha,
        "registry": "pypi",
        "distributions": [{"name": name, "status": "published", "sha256": digest} for name in (
            f"alopex-{version}-cp38-abi3-manylinux_2_17_x86_64.whl",
            f"alopex-{version}-cp38-abi3-win_amd64.whl",
            f"alopex-{version}-cp38-abi3-macosx_11_0_arm64.whl",
            f"alopex-{version}.tar.gz",
        )],
    },
    "parser": {
        "contract": "0.28.0",
        "manifest_sha256": digest,
        "envelope_sha256": digest,
        "assets": [
            {
                "target": target,
                "archive_sha256": digest,
                "library_sha256": digest,
                "native_smoke": True,
            }
            for target in targets
        ],
    },
    "publication_order": {"core_before_python": True},
    "provenance": {"python_descends_from_core": True},
}
with open(sys.argv[1], "w", encoding="utf-8") as stream:
    json.dump(candidate, stream, sort_keys=True)
PY

assert_success() {
    if ! "$@" >"${TEMP_ROOT}/out" 2>&1; then
        echo "expected success: $*" >&2
        cat "${TEMP_ROOT}/out" >&2
        exit 1
    fi
}

assert_fail() {
    if "$@" >"${TEMP_ROOT}/out" 2>&1; then
        echo "expected failure: $*" >&2
        cat "${TEMP_ROOT}/out" >&2
        exit 1
    fi
}

assert_success bash "${RUNNER}" --verify-join "${TEMP_ROOT}/candidate.json"

python3 - "${TEMP_ROOT}" <<'PYFIX'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
data = json.loads((root / "candidate.json").read_text())
data["parser"]["assets"] = [a for a in data["parser"]["assets"] if a["target"] != "x86_64-apple-darwin"]
(root / "primary-only.json").write_text(json.dumps(data))
for index, target in enumerate(a["target"] for a in data["parser"]["assets"]):
    altered = json.loads(json.dumps(data))
    altered["parser"]["assets"] = [a for a in altered["parser"]["assets"] if a["target"] != target]
    (root / f"missing-primary-{index}.json").write_text(json.dumps(altered))
PYFIX
assert_success bash "${RUNNER}" --verify-join "${TEMP_ROOT}/primary-only.json"
for candidate in "${TEMP_ROOT}"/missing-primary-*.json; do
    assert_fail bash "${RUNNER}" --verify-join "$candidate"
done

python3 - "${TEMP_ROOT}/candidate.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, encoding="utf-8") as stream:
    data = json.load(stream)
data["core"]["head_sha"] = "d" * 40
with open(path, "w", encoding="utf-8") as stream:
    json.dump(data, stream, sort_keys=True)
PY
assert_success bash "${RUNNER}" --verify-join "${TEMP_ROOT}/candidate.json"

python3 - "${TEMP_ROOT}/candidate.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, encoding="utf-8") as stream:
    data = json.load(stream)
data["core"]["source_sha"] = "d" * 40
with open(path, "w", encoding="utf-8") as stream:
    json.dump(data, stream, sort_keys=True)
PY
assert_fail bash "${RUNNER}" --verify-join "${TEMP_ROOT}/candidate.json"

python3 - "${TEMP_ROOT}/candidate.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, encoding="utf-8") as stream:
    data = json.load(stream)
data["core"]["peeled_sha"] = "d" * 40
with open(path, "w", encoding="utf-8") as stream:
    json.dump(data, stream, sort_keys=True)
PY
assert_fail bash "${RUNNER}" --verify-join "${TEMP_ROOT}/candidate.json"

python3 - "${TEMP_ROOT}/candidate.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, encoding="utf-8") as stream:
    data = json.load(stream)
data["core"]["peeled_sha"] = data["core_tag"]["peeled_sha"]
data["parser"]["assets"].pop()
with open(path, "w", encoding="utf-8") as stream:
    json.dump(data, stream, sort_keys=True)
PY
assert_fail bash "${RUNNER}" --verify-join "${TEMP_ROOT}/candidate.json"

python3 - "${TEMP_ROOT}/candidate.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, encoding="utf-8") as stream:
    data = json.load(stream)
data["parser"]["assets"].append({
    "target": "x86_64-pc-windows-msvc",
    "archive_sha256": "b" * 64,
    "library_sha256": "not-a-digest",
    "native_smoke": True,
})
with open(path, "w", encoding="utf-8") as stream:
    json.dump(data, stream, sort_keys=True)
PY
assert_fail bash "${RUNNER}" --verify-join "${TEMP_ROOT}/candidate.json"

python3 - "${TEMP_ROOT}/candidate.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, encoding="utf-8") as stream:
    data = json.load(stream)
data["parser"]["assets"] = [
    {
        "target": target,
        "archive_sha256": "b" * 64,
        "library_sha256": "b" * 64,
        "native_smoke": True,
    }
    for target in (
        "x86_64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
        "x86_64-pc-windows-msvc",
    )
]
data["core"]["source_sha"] = data["core_tag"]["peeled_sha"]
data["provenance"] = {"python_descends_from_core": False}
with open(path, "w", encoding="utf-8") as stream:
    json.dump(data, stream, sort_keys=True)
PY
assert_fail bash "${RUNNER}" --verify-join "${TEMP_ROOT}/candidate.json"

echo "release-join checks passed"
