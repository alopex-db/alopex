#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
RUNNER="${REPO_ROOT}/scripts/release/verify-release/run.sh"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/alopex-release-join.XXXXXX")"
trap 'rm -rf "${TEMP_ROOT}"' EXIT

python3 - "${TEMP_ROOT}/candidate.json" <<'PY'
import json
import sys

sha = "a" * 40
digest = "b" * 64
targets = [
    "x86_64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
]
candidate = {
    "version": "0.8.8",
    "reviewed_main_sha": sha,
    "tag": {"name": "v0.8.8", "peeled_sha": sha},
    "core": {
        "status": "success",
        "published": True,
        "run_id": "1001",
        "head_sha": sha,
        "source_sha": sha,
        "peeled_sha": sha,
        "registry": "crates.io",
        "crates": [{"name": "alopex-core", "status": "published"}],
    },
    "python": {
        "status": "success",
        "published": True,
        "run_id": "1002",
        "head_sha": sha,
        "peeled_sha": sha,
        "registry": "pypi",
        "distributions": [{"name": "alopex", "status": "published", "sha256": digest}],
    },
    "parser": {
        "contract": "0.15.0",
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
    "repair_forward": {"complete": True},
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

python3 - "${TEMP_ROOT}/candidate.json" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path, encoding="utf-8") as stream:
    data = json.load(stream)
data["core"]["head_sha"] = "c" * 40
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
data["core"]["peeled_sha"] = "c" * 40
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
data["core"]["peeled_sha"] = data["reviewed_main_sha"]
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

echo "release-join checks passed"
