---
name: context-efficient-development
description: Plan or improve context use for a complex, multi-phase development task or audit an agent workflow's token use.
---

# Context-efficient development

Use this skill for a complex task likely to span phases, turns or compactions, or when diagnosing context consumption. For ordinary coding, follow the short rules in the workspace `AGENTS.md` without loading this skill.

1. Establish a project-wide baseline across all available sessions, not only the current chat: model calls and turns, input and cached input, output, compactions, repeated tools, and hook output. Include unfinished long-running threads. Distinguish billed cost, processed tokens, and context-window pressure; local session logs may be incomplete and are not a final bill.
2. Divide the task by acceptance evidence, not by arbitrary turn count. Keep one compact task-state note with current phase, decisions, source pointers, verified results, and next action. Update at milestones; after compaction or a new turn, read the note and only the evidence needed next. The note is an index, not a replacement for source evidence.
   If a long thread keeps carrying a large history across many calls, start a fresh session at a verified phase boundary with this note and source pointers. A fork retains the history and does not solve that problem. Preserve the active result and avoid concurrent duplicate work.
   Before the parent loads details, identify substantial separable work across implementation, review, assessment, focused tests, evaluation, task triage, progress monitoring, documentation, and investigation. Delegate with a bounded brief: outcome and acceptance checks, source identity, exact scope/file ownership, allowed side effects, resource budget, stop condition, and compact return of changes, verification, evidence pointers, and risks. Use no inherited conversation history when supported. Parent coordinates dependencies, shared state, authorization, integration, final judgment, and serialized builds per host. Do not duplicate work or create agents for short dependent steps; measure total parent and child usage.
3. Identify costly repetition across sessions. If calls or polls do not produce a new decision or verified fact, stop the loop, update the task-state note, and reassess the acceptance condition. Prefer a narrow search, bounded excerpt, selected tool fields, or retained full log over large chat output. Discover tool names before requesting full schemas.
4. Make persistent changes at the proper layer: `AGENTS.md` for always-needed decisions, a skill for a distinct workflow, a reference for conditional detail, a script for deterministic filtering, and a hook only when lifecycle automation has a measured benefit. Never suppress failed commands or required evidence.
5. For code changes, map affected behavior and consumers to focused tests first. Run a full workspace suite only for a required integration or release gate, or a specific cross-workspace risk left unresolved by focused checks. Do not repeat it after each small edit.
6. Verify with a representative task. Compare before/after usage when available; otherwise report a structural change and its unmeasured limit.

For the research basis, concrete command patterns, and measurement plan, read the workspace's `alopex-docs-internal/development/agent-context-efficiency.md` when those details are needed.
