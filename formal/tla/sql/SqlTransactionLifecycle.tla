------------------------- MODULE SqlTransactionLifecycle -------------------------
EXTENDS FiniteSets

States == {"Idle", "Active", "Failed"}
Writes == {"before", "after"}

VARIABLES state, staged, durable, visible, acknowledged, discarded, savepoint, alive,
          barrier, postCommitRead, commitFailed, rollbackFailed

vars == <<state, staged, durable, visible, acknowledged, discarded, savepoint, alive,
          barrier, postCommitRead, commitFailed, rollbackFailed>>

Init ==
    /\ state = "Idle"
    /\ staged = {}
    /\ durable = {}
    /\ visible = {}
    /\ acknowledged = {}
    /\ discarded = {}
    /\ savepoint = FALSE
    /\ alive = TRUE
    /\ barrier = FALSE
    /\ postCommitRead = "None"
    /\ commitFailed = FALSE
    /\ rollbackFailed = FALSE

Begin ==
    /\ alive
    /\ state = "Idle"
    /\ state' = "Active"
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ barrier' = FALSE
    /\ postCommitRead' = "None"
    /\ commitFailed' = FALSE
    /\ rollbackFailed' = FALSE
    /\ UNCHANGED <<durable, visible, acknowledged, discarded, alive>>

WriteBefore ==
    /\ alive
    /\ state = "Active"
    /\ ~savepoint
    /\ "before" \notin durable
    /\ staged' = staged \cup {"before"}
    /\ discarded' = discarded \ {"before"}
    /\ UNCHANGED <<state, durable, visible, acknowledged, savepoint, alive,
                    barrier, postCommitRead, commitFailed, rollbackFailed>>

CreateSavepoint ==
    /\ alive
    /\ state = "Active"
    /\ ~savepoint
    /\ savepoint' = TRUE
    /\ UNCHANGED <<state, staged, durable, visible, acknowledged, discarded, alive,
                    barrier, postCommitRead, commitFailed, rollbackFailed>>

WriteAfter ==
    /\ alive
    /\ state = "Active"
    /\ savepoint
    /\ "after" \notin durable
    /\ staged' = staged \cup {"after"}
    /\ discarded' = discarded \ {"after"}
    /\ UNCHANGED <<state, durable, visible, acknowledged, savepoint, alive,
                    barrier, postCommitRead, commitFailed, rollbackFailed>>

StatementFail ==
    /\ alive
    /\ state = "Active"
    /\ state' = "Failed"
    /\ UNCHANGED <<staged, durable, visible, acknowledged, discarded, savepoint, alive,
                    barrier, postCommitRead, commitFailed, rollbackFailed>>

RollbackToSavepoint ==
    /\ alive
    /\ state \in {"Active", "Failed"}
    /\ savepoint
    /\ state' = "Active"
    /\ staged' = staged \ {"after"}
    /\ discarded' = discarded \cup (staged \cap {"after"})
    /\ UNCHANGED <<durable, visible, acknowledged, savepoint, alive,
                    barrier, postCommitRead, commitFailed, rollbackFailed>>

ReleaseSavepoint ==
    /\ alive
    /\ state = "Active"
    /\ savepoint
    /\ savepoint' = FALSE
    /\ UNCHANGED <<state, staged, durable, visible, acknowledged, discarded, alive,
                    barrier, postCommitRead, commitFailed, rollbackFailed>>

Commit ==
    /\ alive
    /\ state = "Active"
    /\ state' = "Idle"
    /\ durable' = durable \cup staged
    /\ visible' = durable \cup staged
    /\ acknowledged' = acknowledged \cup staged
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ barrier' = TRUE
    /\ postCommitRead' = "None"
    /\ commitFailed' = FALSE
    /\ rollbackFailed' = FALSE
    /\ UNCHANGED <<discarded, alive>>

CommitFail ==
    /\ alive
    /\ state = "Active"
    /\ state' = "Failed"
    /\ commitFailed' = TRUE
    /\ barrier' = FALSE
    /\ postCommitRead' = "None"
    /\ UNCHANGED <<staged, durable, visible, acknowledged, discarded, savepoint,
                    alive, rollbackFailed>>

Rollback ==
    /\ alive
    /\ state \in {"Active", "Failed"}
    /\ state' = "Idle"
    /\ discarded' = discarded \cup staged
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ barrier' = FALSE
    /\ postCommitRead' = "None"
    /\ commitFailed' = FALSE
    /\ rollbackFailed' = FALSE
    /\ UNCHANGED <<durable, visible, acknowledged, alive>>

RollbackFail ==
    /\ alive
    /\ state \in {"Active", "Failed"}
    /\ state' = "Idle"
    /\ discarded' = discarded \cup staged
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ barrier' = FALSE
    /\ postCommitRead' = "None"
    /\ commitFailed' = FALSE
    /\ rollbackFailed' = TRUE
    /\ UNCHANGED <<durable, visible, acknowledged, alive>>

PostCommitReadSuccess ==
    /\ alive
    /\ state = "Idle"
    /\ barrier
    /\ postCommitRead = "None"
    /\ postCommitRead' = "Success"
    /\ UNCHANGED <<state, staged, durable, visible, acknowledged, discarded, savepoint,
                    alive, barrier, commitFailed, rollbackFailed>>

PostCommitReadFail ==
    /\ alive
    /\ state = "Idle"
    /\ barrier
    /\ postCommitRead = "None"
    /\ postCommitRead' = "Failed"
    /\ UNCHANGED <<state, staged, durable, visible, acknowledged, discarded, savepoint,
                    alive, barrier, commitFailed, rollbackFailed>>

DisconnectOrCrash ==
    /\ alive
    /\ state \in {"Active", "Failed"}
    /\ alive' = FALSE
    /\ state' = "Idle"
    /\ discarded' = discarded \cup staged
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ barrier' = FALSE
    /\ postCommitRead' = "None"
    /\ commitFailed' = FALSE
    /\ rollbackFailed' = FALSE
    /\ UNCHANGED <<durable, visible, acknowledged>>

CrashAfterTerminal ==
    /\ alive
    /\ state = "Idle"
    /\ alive' = FALSE
    /\ UNCHANGED <<state, staged, durable, visible, acknowledged, discarded, savepoint,
                    barrier, postCommitRead, commitFailed, rollbackFailed>>

Restart ==
    /\ ~alive
    /\ alive' = TRUE
    /\ state' = "Idle"
    /\ staged' = {}
    /\ visible' = durable
    /\ savepoint' = FALSE
    /\ barrier' = FALSE
    /\ postCommitRead' = "None"
    /\ commitFailed' = FALSE
    /\ rollbackFailed' = FALSE
    /\ UNCHANGED <<durable, acknowledged, discarded>>

Next ==
    \/ Begin
    \/ WriteBefore
    \/ CreateSavepoint
    \/ WriteAfter
    \/ StatementFail
    \/ RollbackToSavepoint
    \/ ReleaseSavepoint
    \/ Commit
    \/ CommitFail
    \/ Rollback
    \/ RollbackFail
    \/ PostCommitReadSuccess
    \/ PostCommitReadFail
    \/ DisconnectOrCrash
    \/ CrashAfterTerminal
    \/ Restart

TypeOK ==
    /\ state \in States
    /\ staged \subseteq Writes
    /\ durable \subseteq Writes
    /\ visible \subseteq Writes
    /\ acknowledged \subseteq Writes
    /\ discarded \subseteq Writes
    /\ savepoint \in BOOLEAN
    /\ alive \in BOOLEAN
    /\ barrier \in BOOLEAN
    /\ postCommitRead \in {"None", "Success", "Failed"}
    /\ commitFailed \in BOOLEAN
    /\ rollbackFailed \in BOOLEAN

OnlyDurableWritesAreVisible == visible = durable
AcknowledgedCommitsAreDurable == acknowledged \subseteq durable
DiscardedWritesNeverBecomeDurable == (discarded \cap durable) = {}
IdleHasNoStagedWrites == state = "Idle" => staged = {}
DeadProcessHasNoActiveTransaction == ~alive => state = "Idle" /\ staged = {}
PostCommitReadRequiresBarrier == postCommitRead # "None" => barrier
CommitFailureDoesNotPublish == commitFailed => (staged \cap durable) = {}
CommitBarrierPublishesDurably == barrier => visible = durable

Spec == Init /\ [][Next]_vars

=============================================================================
