# FM-SQL-MUTATION-001 inventory

- Models: `formal/tla/sql/SqlMutationLifecycle.tla`, `formal/tla/sql/SequenceConcurrent.tla`
- Configurations: `formal/tla/sql/SqlMutationLifecycle.cfg`, `formal/tla/sql/SequenceConcurrent.cfg`
- Owners: SQL constraints (#170), advanced DML (#171), COPY (#172), and sequence allocation (#173)
- Properties: invalid or conflicting mutations never publish rows; cascade depth is bounded; COPY publishes only after temporary output completes; sequence allocation commits or rolls back atomically and stays within configured cycle bounds
