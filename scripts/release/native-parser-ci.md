# Native parser CI selection

The Release Process Contract always runs delivery/report contract tests and the
release-report state model for its selected changes. Its contract job also
compares the PR base or push-before commit with the checked-out commit.
It fetches only those two commits at depth one, not the full Git history.

Only native parser inputs select the four native parser jobs and their asset
aggregation: the Nim source/dependency tree, exact-toolchain setup action,
build/archive tooling and native tests, workspace version manifest, this
workflow, or the classifier and its regression tests. The complete input list
is in `classify_parser_changes.py`; extend it when adding a build dependency.
Both workflow event filters include `Cargo.toml` because the workspace version
is part of parser archive identity.

Report rendering, publication and scenario-client changes run their contract
tests without rebuilding unchanged native parser assets. This is an explicit
skip, not a new four-target qualification result. Public release asset
qualification is unchanged.

The comparison uses NUL-delimited paths and disables rename detection, so
renames into or out of the parser tree and deletions still select all hosts.
Failed base fetches, missing commits, invalid IDs, unreadable Git history, a failed diff, or an empty
comparison select all four hosts. A failed contract job blocks the native jobs
and remains a failed workflow; it is never treated as a passing parser check.
