#!/usr/bin/env python3
"""Static workflow process-contract verifier for Alopex release and measurement lanes.

Checks:
  - NO_PRE_PREFLIGHT_SERVICES: forbids GitHub Actions `services:` blocks in
    measurement/verification workflows (which pull/start containers before any
    in-job storage or security preflight runs).
  - PINNED_CONTAINER_IMAGE_DIGEST: requires `@sha256:<64-hex>` image digests
    for container images declared in measurement/verification workflows.
  - NO_PUBLIC_CMS_OR_RAW_STRESS_UPLOAD: forbids uploading `.cms` encrypted
    diagnostic bundles or unvalidated raw `target/stress-reports` /
    `target/stress-artifacts` directories through `actions/upload-artifact`.
  - NO_ADHOC_PRIVILEGE_RELAXATION: forbids `--privileged`,
    `seccomp=unconfined`, `apparmor=unconfined`, and broad `--cap-add`.
  - LONG_JOB_EARLY_STOP_REQUIRED: requires jobs with `timeout-minutes > 60` in
    measurement/verification workflows to reference an explicit budget,
    checkpoint, or early-stop control.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
import sys


MEASUREMENT_WORKFLOWS = {
    "parity-performance.yml",
    "alopex-performance.yml",
    "stress-tests.yml",
    "public-release-verification.yml",
}

DIGEST_RE = re.compile(r"@sha256:[0-9a-f]{64}\b")
TIMEOUT_RE = re.compile(r"^\s*timeout-minutes\s*:\s*(\d+)\b")
SERVICES_RE = re.compile(r"^\s{2,6}services\s*:\s*$")
IMAGE_RE = re.compile(r"^\s*image\s*:\s*([^\s#]+)")
PRIV_RE = re.compile(
    r"(--privileged\b|seccomp\s*=\s*unconfined|apparmor\s*=\s*unconfined|--cap-add(?:=|\s+)(?:ALL|SYS_ADMIN)\b)",
    re.IGNORECASE,
)
EARLY_STOP_RE = re.compile(
    r"(early[-_ ]?stop|checkpoint|stage[-_ ]?budget|time[-_ ]?budget|storage[-_ ]?guard)",
    re.IGNORECASE,
)
RAW_STRESS_UPLOAD_RE = re.compile(
    r"target/stress-(?:reports|artifacts)\b"
)
CMS_UPLOAD_RE = re.compile(r"\.cms\b")


@dataclass(frozen=True)
class Finding:
    rule_id: str
    workflow: str
    line: int
    detail: str


def audit_workflow_text(rel_path: str, text: str) -> list[Finding]:
    findings: list[Finding] = []
    name = Path(rel_path).name
    is_measurement = name in MEASUREMENT_WORKFLOWS
    lines = text.splitlines()

    has_early_stop_control = bool(EARLY_STOP_RE.search(text))
    in_upload_block = False
    upload_indent = 0

    for lineno, line in enumerate(lines, start=1):
        stripped = line.lstrip(" ")
        indent = len(line) - len(stripped)

        if PRIV_RE.search(line):
            findings.append(
                Finding(
                    rule_id="NO_ADHOC_PRIVILEGE_RELAXATION",
                    workflow=rel_path,
                    line=lineno,
                    detail="privilege or LSM relaxation flag detected",
                )
            )

        if is_measurement:
            if SERVICES_RE.match(line):
                findings.append(
                    Finding(
                        rule_id="NO_PRE_PREFLIGHT_SERVICES",
                        workflow=rel_path,
                        line=lineno,
                        detail="Actions services block runs before in-job preflight",
                    )
                )

            img_m = IMAGE_RE.match(line)
            if img_m and not DIGEST_RE.search(img_m.group(1)):
                findings.append(
                    Finding(
                        rule_id="PINNED_CONTAINER_IMAGE_DIGEST",
                        workflow=rel_path,
                        line=lineno,
                        detail=f"unpinned container image reference: {img_m.group(1)}",
                    )
                )

            to_m = TIMEOUT_RE.match(line)
            if to_m and int(to_m.group(1)) > 60 and not has_early_stop_control:
                findings.append(
                    Finding(
                        rule_id="LONG_JOB_EARLY_STOP_REQUIRED",
                        workflow=rel_path,
                        line=lineno,
                        detail=f"timeout-minutes={to_m.group(1)} without checkpoint/early-stop control",
                    )
                )

        # Track actions/upload-artifact step blocks
        if "actions/upload-artifact@" in line:
            in_upload_block = True
            upload_indent = indent
            continue
        if in_upload_block:
            if stripped and not stripped.startswith("#") and indent <= upload_indent and stripped.startswith("- "):
                in_upload_block = False
            elif stripped and not stripped.startswith("#") and indent < upload_indent:
                in_upload_block = False

        if in_upload_block and is_measurement:
            if CMS_UPLOAD_RE.search(line):
                findings.append(
                    Finding(
                        rule_id="NO_PUBLIC_CMS_OR_RAW_STRESS_UPLOAD",
                        workflow=rel_path,
                        line=lineno,
                        detail="encrypted CMS diagnostic bundle uploaded in measurement workflow",
                    )
                )
            if RAW_STRESS_UPLOAD_RE.search(line):
                findings.append(
                    Finding(
                        rule_id="NO_PUBLIC_CMS_OR_RAW_STRESS_UPLOAD",
                        workflow=rel_path,
                        line=lineno,
                        detail="raw stress reports/artifacts uploaded without allowlist boundary",
                    )
                )

    return findings


def audit_repository(repo_root: Path) -> list[Finding]:
    wf_dir = repo_root / ".github/workflows"
    findings: list[Finding] = []
    if not wf_dir.is_dir():
        return findings
    for path in sorted(wf_dir.glob("*.yml")):
        rel = path.relative_to(repo_root).as_posix()
        findings.extend(audit_workflow_text(rel, path.read_text(encoding="utf-8")))
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    findings = audit_repository(args.repo_root.resolve())
    if args.as_json:
        print(json.dumps([asdict(f) for f in findings], indent=2, sort_keys=True))
    else:
        print(f"findings: {len(findings)}")
        for f in findings:
            print(f"  [{f.rule_id}] {f.workflow}:{f.line} - {f.detail}")

    if args.strict and findings:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
