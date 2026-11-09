# Agent context use

Start with the acceptance condition and the smallest relevant crate or file. Search paths and symbols before reading bounded excerpts; exclude worktrees, vendor and generated output from broad searches unless targeted. For verbose commands, preserve the full log and exit status, then show only the decisive lines and log path. Reuse source-bound evidence and expand tests only for an unanswered condition. Keep safety, storage and verification requirements intact.

Test affected behavior in the smallest test or crate first. Add consumer tests only for a changed boundary. Do not run or rerun workspace-wide tests after a small fix; use them only for a required integration/release gate or documented broad impact unresolved by focused checks.

For multi-phase work, keep one compact task-state note with phase acceptance checks, decisions and evidence pointers, verified results, and the next action. Update it at milestones. After a handoff or compaction, reload only the current phase's evidence; never replay whole tool catalogs, logs or transcripts.

For substantial separable work, delegate disjoint implementation, review, assessment, focused tests, evaluation, task triage, progress checks, docs, and investigation before the parent loads details. Brief each agent with acceptance checks, exact scope/file ownership, allowed side effects, stop condition, minimal history, and a compact result with evidence pointers. The parent coordinates shared state, authorization, integration, final judgment, and per-host serialized builds. Avoid duplicate or short dependent work.

At phase boundaries, check usage across project sessions with `python3 ../alopex-docs-internal/scripts/agent-usage-audit.py --workspace .. --hours 24` when local Codex records are available. If repeated calls or polls produce no new decision or verified fact, stop that loop and reassess the acceptance condition. Cached input still counts toward processed tokens.

The workspace procedure is in `../alopex-docs-internal/development/agent-context-efficiency.md`; read it for a large task or a context-use investigation. The `context-efficient-development` skill applies to these cases, not routine coding.
