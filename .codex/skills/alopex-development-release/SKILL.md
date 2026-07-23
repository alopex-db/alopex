---
name: alopex-development-release
description: Repeatable Alopex version development and release workflow covering roadmap/spec-workflow approvals, phased design and implementation, all-surface verification, target-version release gates, tagging, registry publication, and post-release evidence. Use when planning, implementing, validating, or releasing any Alopex version.
---

# Alopex development and release

Use this skill for an end-to-end Alopex version. Keep requirements, design, implementation, verification, and publication as separate checkpoints. Read [references/checklist.md](references/checklist.md) for the command-level checklist.

## Non-negotiable rules

- Work from `/home/roomtv/works/alopex-db`; treat `alopex/` as the source repository and `.spec-workflow/` as the management state.
- Prefix shell commands with `rtk`. Confirm cwd, branch, worktree, remotes, and clean/dirty state before changing anything.
- Keep the 50 GiB workspace limit. Run the cache-budget check before and after Cargo, maturin, pytest, CI, stress, or other artifact-generating work; clean generated artifacts at each stage.
- Never publish, push a release tag, create a GitHub Release, or deploy without explicit user authorization. “Release-ready” means stop before publication.
- Preserve user work. Isolate release work in a dedicated worktree/branch and never reset or delete broad paths.
- Use newly created approval IDs and the actual project root when submitting spec-workflow requests. Do not infer approval from stale dashboard entries or old approval files.

## Development workflow

1. **Observe and decompose.** Inspect the roadmap, all crate/Python surfaces, existing specs, branch state, and reference-project source. Split work by broad phases, not one design/task project per feature. Keep feature requirements separate from cross-phase policy/gate requirements.
2. **Write requirements first.** Requirements state what must be true and observable; do not prescribe Rust/Python modules, SQL implementation, data structures, or algorithms. Include compatibility assumptions for functionality completed in earlier versions. Explicitly enumerate SQL statements/functions, CLI commands, distributed-read behavior, DataFrame behavior, and Python local/async interfaces when in scope.
3. **Design after approval.** Design explains how the approved goals map to the existing/reference implementation, contracts, error behavior, compatibility, and test viewpoints. Do not change approved release goals because implementation details reveal new considerations; add implementation caveats without silently changing scope.
4. **Submit and approve each artifact.** Submit requirements, design, and tasks through spec-workflow using new request IDs; verify they appear in the dashboard, obtain approval, then record implementation for every task. If rejected, revise and resubmit rather than implementing an unapproved artifact.
5. **Implement in small commits.** Keep commits scoped and push frequently. Run focused RED tests before bug fixes, then focused GREEN tests and independent broader verification. Keep wiring, contracts, and freshness/idempotency concerns distinct.
6. **Run the full surface matrix.** Before tagging, verify every affected crate plus the Python binding. For v0.8-class work, run `crates/alopex-tools/v08/verify-v08-surfaces.sh` and the candidate/release checks; cover distributed read, cluster/server, SQL, CLI, DataFrame streaming, Rust Python bindings, and Python local/async APIs. A representative test is not full coverage.

## Release workflow

1. Confirm the target version in workspace package metadata, internal dependency constraints, crate manifests, lockfile, Python package metadata, changelog, and release notes. Keep unrelated dependency-lockfile churn out of the release commit.
2. Create/use an isolated release branch/worktree, run the project safety script (`scripts/release/safe-tag.sh`), and verify the branch, clean tree, version, matching Python tag expectation, and absence/presence of the target tag.
3. **Require a target-version CI gate.** The release workflow must invoke the target version's full gate, not only a previous-version compatibility gate. If it invokes only an old gate, add the current verifier/gate and commit it before tagging. The gate must cover the complete surface matrix.
4. Merge the approved release branch to `main`, push it, and rerun safety checks. Run candidate verification, formatting, clippy, and required release checks immediately before tagging.
5. With explicit authorization, create annotated Rust `v<version>` and independent Python `alopex-py-v<version>` tags at the intended commits and push them. Do not casually move a tag after a registry or GitHub Release exists. If a tag-triggered workflow fails before publication, fix the commit and recreate only that failed tag after verifying its exact scope.
6. Monitor both release workflows to completion. Inspect failed logs, not just the overall status. Do not report success while a publish job is queued, skipped, or failed.
7. Verify publication independently: GitHub Release and asset names, exact tag-to-commit SHA, every crates.io package in dependency order, and PyPI wheels/sdist plus Python GitHub Release. Use public registry/index evidence when API polling is rate-limited.
8. Finish with worktree/branch/remote checks, generated-artifact cleanup, cache-budget check, and a Japanese handoff listing completed and uncompleted work, workflow URLs, SHAs, registry evidence, and any known follow-up (for example, a patch release required when a version was already published with a defect).

## Failure and recovery

- Treat a release-gate failure as a blocker even when another independent package workflow succeeds.
- Reproduce the failing test locally, fix the smallest correct contract, rerun focused and full checks, commit/push, and rerun the release workflow from the corrected commit.
- Never overwrite an already published registry version. If Python or Rust was published from a defective commit, use a new patch version and document the relationship.
- If an unpublished tag caused a failed workflow, delete/recreate only that exact tag after checking that no GitHub Release or registry artifact exists. Preserve successful independent tags.
- Do not hide unrelated main-branch CI failures; report them separately from release blockers.
