---
name: alopex-development-release
description: Repeatable Alopex version development and release workflow covering roadmap-first, v0.8-style phase requirements that enumerate every crate/module/API/SQL/CLI surface, spec-workflow approvals, phased design and implementation, all-surface verification, target-version release gates, tagging, registry publication, and post-release evidence. Use when planning, implementing, validating, or releasing any Alopex version.
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
- Treat phase coverage as a release-blocking completeness contract: every roadmap item and inherited public surface must belong to exactly one broad phase, have explicit requirements, and be traced through design, tasks, tests, and release evidence. Never replace missing phase coverage with a later audit.

## Development workflow

1. **Start every version from the roadmap.** Before creating any requirement document, identify the target `v.x.x.x`, read the current roadmap and its version/phase entries, snapshot the target goals and dependencies, and begin the spec for that version. Enumerate every workspace crate, product/development module, Python/Nim surface, SQL surface, and CLI surface named by the roadmap or required by prior-version compatibility. For each item, assign a requirement anchor immediately: new in this version, inherited and required to remain working, explicitly deferred to a later version, or out of scope with a reason. A later audit must not be used to discover requirements that should have been written at kickoff.
2. **Decompose from that initial inventory.** Follow the v0.8 pattern: establish a small number of broad, capability-coherent phases (for example, cluster metadata/operations, distributed SQL read, DataFrame streaming/expressions, and Python surfaces), then put individual features, API calls, SQL statements/functions, and CLI commands under those phases as requirements or later tasks. Do not create one design/task project per feature. Every inventory row must have exactly one owning phase plus any explicit cross-phase policy/gate reference; do not leave rows only in a generic “cross-phase” bucket. Compare rough implementation volume, affected surfaces, dependency order, and verification load before approval. Rebalance or split a phase that hides a disproportionately large implementation behind a short requirement, and do not use later phases as an implementation-delivery deferral. Read [references/phase-requirements.md](references/phase-requirements.md) for the phase matrix and completeness gate.
3. **Write phase-complete requirements first.** Requirements state what must be true and observable; do not prescribe Rust/Python modules, SQL implementation, data structures, or algorithms. Treat functionality completed by earlier versions as explicit compatibility/baseline requirements and include it in the initial inventory rather than silently omitting it because the current roadmap does not repeat it. At requirement kickoff, enumerate exact SQL statements, operators, functions, PRAGMAs, vector/HNSW/COPY surfaces, CLI subcommands/options, server endpoints, embedded APIs, DataFrame namespaces/operations, Python sync/async/NumPy/catalog surfaces, cluster diagnostics, and development verifiers that are affected or inherited. Each item must be classified as new, inherited, deferred, or out of scope with an evidence-based reason, and must be assigned to a phase and requirement ID. State feature outcomes separately from policies, gates, and release checks that apply across phases. Do not proceed to design until every item has phase ownership, a requirement, status, acceptance criteria, and a planned test/evidence viewpoint.
4. **Design after approval.** Design explains how the approved goals map to the existing/reference implementation, contracts, error behavior, compatibility, and test viewpoints. Do not change approved release goals because implementation details reveal new considerations; add implementation caveats without silently changing scope.
5. **Submit and approve each artifact.** Submit requirements, design, and tasks through spec-workflow using new request IDs; verify they appear in the dashboard, obtain approval, then record implementation for every task. If rejected, revise and resubmit rather than implementing an unapproved artifact.
6. **Implement in small commits.** Keep commits scoped and push frequently. Run focused RED tests before bug fixes, then focused GREEN tests and independent broader verification. Keep wiring, contracts, and freshness/idempotency concerns distinct.
7. **Run the full surface matrix.** Before tagging, verify every affected crate plus the Python binding. For v0.8-class work, run `crates/alopex-tools/v08/verify-v08-surfaces.sh` and the candidate/release checks; cover distributed read, cluster/server, SQL, CLI, DataFrame streaming, Rust Python bindings, and Python local/async APIs. A representative test is not full coverage.

## Phase-complete requirements gate

Before requesting any requirements approval, apply the v0.8-style phase matrix in
[references/phase-requirements.md](references/phase-requirements.md). The matrix must
contain concrete rows for every roadmap item and inherited public surface, not only
coarse crate labels. Assign each row exactly one broad owning phase and classify it as
new, inherited, deferred, or out of scope with evidence. Enumerate exact SQL
statements/functions/PRAGMAs, CLI commands/options/modes, server routes, embedded and
Python sync/async APIs, DataFrame operations/namespaces, cluster diagnostics, Nim/FFI
surfaces, and development verifiers. For each row record support/rejection status,
acceptance criteria, and an evidence viewpoint.

Use broad capability phases like the approved v0.8.0 sequence (cluster metadata and
operations; distributed-read SQL and CLI; DataFrame streaming and expressions; Python
local surfaces), adapting names to the target roadmap. Do not create one phase or
design/task project per feature, and do not hide unfinished work in a generic
cross-phase or later integration phase. Compare phase effort, affected-surface count,
dependencies, and verification load before approval. Require a target-version gate and
the full `requirements → design → task → test/evidence` crosswalk; an older release
gate and an “any one document” mapping are insufficient.

## spec-workflow documents and approval flow

Call `mcp__spec_workflow__spec_workflow_guide` first whenever a user requests a spec or feature workflow. Work on exactly one kebab-case spec at a time and use the absolute project root `/home/roomtv/works/alopex-db` for every spec-workflow tool call. Never use a stale approval ID or a dashboard status copied from an earlier request.

1. **Requirements.** Read steering documents when present, then check user templates before the standard requirements template. Create `.spec-workflow/specs/<spec-name>/requirements.md`. Write user stories and observable EARS-style acceptance criteria: what users and operators can do, compatibility assumptions, failure/limit behavior, SQL statements/functions, CLI commands, and Python/DataFrame/distributed-read surfaces. Keep implementation mechanisms out of requirements. Separate feature scope from policy/gate criteria, and group broad phases instead of creating a phase for every feature.
2. **Request requirements approval.** Call `mcp__spec_workflow__approvals` with `action: "request"`, `category: "spec"`, `categoryName: "<spec-name>"`, `type: "document"`, the relative `filePath` only, an accurate title, and `projectPath` set to the management root. Never send document content in the request. Poll with `action: "status"` until the dashboard reports `approved` or `needs-revision`; verbal approval is not sufficient. On `needs-revision`, apply the review comments, create a new request, and do not proceed. After `approved`, delete that exact request with `action: "delete"`; if deletion fails, stop and poll again.
3. **Design.** Only after requirements approval and successful cleanup, read the design template and inspect the existing/reference source. Create `.spec-workflow/specs/<spec-name>/design.md` mapping every phase-owned inventory row—including inherited prior-version behavior, every module/crate, and every enumerated SQL/CLI/server/Python/DataFrame surface—to contracts, existing components, data flow, error behavior, compatibility, and independent verification viewpoints. The design must include a phase-to-file/component crosswalk and identify any row that is genuinely deferred or out of scope. Design may describe implementation, but must not change the approved goal or import unapproved scope when implementation details reveal new facts; add a caveat or follow-up instead. Submit, poll, revise with a new request when needed, and delete the design approval request using the same rules.
4. **Tasks / implementation document.** Only after design approval and cleanup, read the tasks template and create `.spec-workflow/specs/<spec-name>/tasks.md`. Convert every design/inventory row into atomic implementation or compatibility-verification tasks (normally 1–3 files each), with requirement references, file locations, success criteria, constraints, and a `_Prompt` containing Role, Task, Restrictions, `_Leverage`, `_Requirements`, Success, and instructions to update status and log implementation. Treat the phase as the parent grouping and individual features/API/SQL/CLI items as tasks. Record rough effort/surface size per phase and task; flag monolithic or back-loaded tasks and rebalance them before approval. No inventory row may remain without a task or an explicit deferred/out-of-scope decision. Submit, poll, revise through a new request, and delete the tasks request before implementation.
5. **Implementation logging.** Before each task, call `mcp__spec_workflow__spec_status`, read `tasks.md`, mark the task `- [-]`, and search `.spec-workflow/specs/<spec-name>/Implementation Logs/` for existing artifacts/patterns to avoid duplication. Implement and test the task, then call `mcp__spec_workflow__log_implementation` with the absolute project root, spec name, task ID, summary, created/modified files, line statistics, and complete structured `artifacts` (functions, classes, API endpoints, components, and integrations as applicable). Mark the task `- [x]` only after the log succeeds and tests pass. Repeat until every task is `[x]`.
6. **Status and cleanup.** Use `mcp__spec_workflow__spec_status` and read `tasks.md` directly when resuming. Keep approval requests and implementation logs distinct: delete completed/rejected requests only through the approval tool, never by manually deleting pending workflow state. Preserve approved documents and logs as the audit trail; archive or remove only when the user explicitly requests it.

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
