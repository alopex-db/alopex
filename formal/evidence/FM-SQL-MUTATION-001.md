# FM-SQL-MUTATION-001 evidence

TLC 2.20-compatible build `2026.05.12.170007` checked
`SqlMutationLifecycle.cfg` with one worker on 2026-09-03.

- Result: no error found
- States: 4,519 generated, 846 distinct, 0 left on queue
- Complete-state-graph depth: 16
- Checked invariants: `TypeOK`, `CommittedRowsSatisfyConstraints`, `FailureDoesNotPublishRows`, `IdleHasNoStagedMutation`
- Deadlocks: disabled because stable idle and completed COPY states are intentional

The same TLC build checked `SequenceConcurrent.cfg` with two clients and a
three-value allocation domain on 2026-09-03.

- Result: no error found
- States: 709 generated, 246 distinct, 0 left on queue
- Complete-state-graph depth: 15
- Checked invariants: `TypeOK`, `CommittedValuesAreUnique`, `CommittedPrefixIsGapFree`
