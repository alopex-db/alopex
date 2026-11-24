"""Normalize local Cargo references in a public native wheel's SBOM."""
from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
from zipfile import ZipFile


COMMIT = re.compile(r"[0-9a-f]{40}\Z")
SBOM = re.compile(r"[^/]+\.dist-info/sboms/alopex-py\.cyclonedx\.json\Z")
RECORD = re.compile(r"[^/]+\.dist-info/RECORD\Z")
SOURCE_URL = "https://github.com/alopex-db/alopex/tree/{commit}/"


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("WHEEL_SBOM_INVALID")


def _record_hash(data: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    return "sha256=" + digest.decode("ascii")


def normalize_wheel_sbom(wheel: Path, source_root: Path, source_commit: str) -> dict[str, int]:
    """Replace known local references atomically; return path-free proof counts.

    A failed validation leaves the input wheel untouched. Other ZIP member bytes
    and metadata are copied, while the SBOM and RECORD are regenerated.
    """
    wheel = Path(wheel)
    root = str(Path(source_root))
    _require(Path(root).is_absolute() and root != "/" and
             type(source_commit) is str and COMMIT.fullmatch(source_commit) is not None)
    old_prefix = "path+file://" + root.rstrip("/") + "/"
    new_prefix = SOURCE_URL.format(commit=source_commit)
    temporary = None
    try:
        with ZipFile(wheel) as archive:
            members = archive.infolist()
            names = [member.filename for member in members]
            _require(len(names) == len(set(names)) and
                     all(not name.startswith("/") and ".." not in name.split("/")
                         for name in names))
            sboms = [name for name in names if "/sboms/" in name]
            records = [name for name in names if name.endswith(".dist-info/RECORD")]
            _require(len(sboms) == len(records) == 1 and
                     SBOM.fullmatch(sboms[0]) is not None and
                     RECORD.fullmatch(records[0]) is not None and
                     sboms[0].split(".dist-info/")[0] ==
                     records[0].split(".dist-info/")[0])
            sbom_name, record_name = sboms[0], records[0]
            original_sbom = archive.read(sbom_name)
            document = json.loads(original_sbom)
            _require(type(document) is dict and document.get("bomFormat") == "CycloneDX" and
                     document.get("specVersion") == "1.5" and
                     type(document.get("components")) is list and
                     type(document.get("dependencies")) is list)

            counts = {"bom-ref": 0, "ref": 0, "dependsOn": 0}

            def transform(value, key=None):
                if type(value) is dict:
                    return {name: transform(item, name) for name, item in value.items()}
                if type(value) is list:
                    return [transform(item, key) for item in value]
                if type(value) is str and value.startswith("path+file://"):
                    _require(key in counts and value.startswith(old_prefix))
                    suffix = value[len(old_prefix):]
                    _require(suffix.startswith("crates/") and
                             len(suffix.split("/")) == 2 and
                             "#" in suffix and not any(c in suffix for c in "\\\n\r"))
                    counts[key] += 1
                    return new_prefix + suffix
                return value

            converted = transform(document)
            _require(sum(counts.values()) > 0)
            old_components = [item.get("bom-ref") for item in document["components"]
                              if type(item) is dict]
            new_components = [item.get("bom-ref") for item in converted["components"]
                              if type(item) is dict]
            _require(len(old_components) == len(document["components"]) and
                     len(new_components) == len(set(new_components)) and
                     all(type(ref) is str for ref in new_components))
            for dependency in converted["dependencies"]:
                _require(type(dependency) is dict and type(dependency.get("ref")) is str and
                         ("dependsOn" not in dependency or
                          type(dependency["dependsOn"]) is list and
                          all(type(ref) is str for ref in dependency["dependsOn"])))
            # The reference graph must retain exactly the old relationships.
            reverse = lambda ref: old_prefix + ref[len(new_prefix):] if ref.startswith(new_prefix) else ref
            _require([reverse(ref) for ref in new_components] == old_components)
            for old, new in zip(document["dependencies"], converted["dependencies"]):
                _require(type(old) is dict and
                         reverse(new["ref"]) == old["ref"] and
                         [reverse(ref) for ref in new.get("dependsOn", [])] ==
                         old.get("dependsOn", []))

            new_sbom = (json.dumps(converted, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
            rows = list(csv.reader(io.StringIO(archive.read(record_name).decode("utf-8"))))
            _require(all(len(row) == 3 for row in rows) and
                     len(rows) == len(names) and
                     {row[0] for row in rows} == set(names) and
                     len({row[0] for row in rows}) == len(rows))
            for row in rows:
                if row[0] == sbom_name:
                    row[1], row[2] = _record_hash(new_sbom), str(len(new_sbom))
                elif row[0] == record_name:
                    row[1], row[2] = "", ""
                else:
                    data = archive.read(row[0])
                    _require(row[1] == _record_hash(data) and row[2] == str(len(data)))
            output = io.StringIO(newline="")
            writer = csv.writer(output, lineterminator="\n")
            writer.writerows(rows)
            new_record = output.getvalue().encode("utf-8")

            with tempfile.NamedTemporaryFile(prefix=".wheel-sbom-", suffix=".whl",
                                             dir=wheel.parent, delete=False) as stream:
                temporary = Path(stream.name)
            with ZipFile(temporary, "w") as target:
                target.comment = archive.comment
                for member in members:
                    data = (new_sbom if member.filename == sbom_name else
                            new_record if member.filename == record_name else
                            archive.read(member.filename))
                    target.writestr(member, data)
        os.replace(temporary, wheel)
        temporary = None
        return {"replacements": sum(counts.values()), "bom_refs": counts["bom-ref"],
                "dependency_refs": counts["ref"], "dependency_edges": counts["dependsOn"],
                "members": len(names)}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
