------------------------- MODULE SqlTransactionLifecycle -------------------------
EXTENDS FiniteSets

States == {"Idle", "Active", "Failed"}
Writes == {"before", "after"}

VARIABLES state, staged, durable, visible, acknowledged, discarded, savepoint, alive

vars == <<state, staged, durable, visible, acknowledged, discarded, savepoint, alive>>

Init ==
    /\ state = "Idle"
    /\ staged = {}
    /\ durable = {}
    /\ visible = {}
    /\ acknowledged = {}
    /\ discarded = {}
    /\ savepoint = FALSE
    /\ alive = TRUE

Begin ==
    /\ alive
    /\ state = "Idle"
    /\ state' = "Active"
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ UNCHANGED <<durable, visible, acknowledged, discarded, alive>>

WriteBefore ==
    /\ alive
    /\ state = "Active"
    /\ ~savepoint
    /\ "before" \notin durable
    /\ staged' = staged \cup {"before"}
    /\ discarded' = discarded \ {"before"}
    /\ UNCHANGED <<state, durable, visible, acknowledged, savepoint, alive>>

CreateSavepoint ==
    /\ alive
    /\ state = "Active"
    /\ ~savepoint
    /\ savepoint' = TRUE
    /\ UNCHANGED <<state, staged, durable, visible, acknowledged, discarded, alive>>

WriteAfter ==
    /\ alive
    /\ state = "Active"
    /\ savepoint
    /\ "after" \notin durable
    /\ staged' = staged \cup {"after"}
    /\ discarded' = discarded \ {"after"}
    /\ UNCHANGED <<state, durable, visible, acknowledged, savepoint, alive>>

StatementFail ==
    /\ alive
    /\ state = "Active"
    /\ state' = "Failed"
    /\ UNCHANGED <<staged, durable, visible, acknowledged, discarded, savepoint, alive>>

RollbackToSavepoint ==
    /\ alive
    /\ state \in {"Active", "Failed"}
    /\ savepoint
    /\ state' = "Active"
    /\ staged' = staged \ {"after"}
    /\ discarded' = discarded \cup (staged \cap {"after"})
    /\ UNCHANGED <<durable, visible, acknowledged, savepoint, alive>>

ReleaseSavepoint ==
    /\ alive
    /\ state = "Active"
    /\ savepoint
    /\ savepoint' = FALSE
    /\ UNCHANGED <<state, staged, durable, visible, acknowledged, discarded, alive>>

Commit ==
    /\ alive
    /\ state = "Active"
    /\ state' = "Idle"
    /\ durable' = durable \cup staged
    /\ visible' = durable \cup staged
    /\ acknowledged' = acknowledged \cup staged
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ UNCHANGED <<discarded, alive>>

Rollback ==
    /\ alive
    /\ state \in {"Active", "Failed"}
    /\ state' = "Idle"
    /\ discarded' = discarded \cup staged
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ UNCHANGED <<durable, visible, acknowledged, alive>>

DisconnectOrCrash ==
    /\ alive
    /\ state \in {"Active", "Failed"}
    /\ alive' = FALSE
    /\ state' = "Idle"
    /\ discarded' = discarded \cup staged
    /\ staged' = {}
    /\ savepoint' = FALSE
    /\ UNCHANGED <<durable, visible, acknowledged>>

CrashAfterTerminal ==
    /\ alive
    /\ state = "Idle"
    /\ alive' = FALSE
    /\ UNCHANGED <<state, staged, durable, visible, acknowledged, discarded, savepoint>>

Restart ==
    /\ ~alive
    /\ alive' = TRUE
    /\ state' = "Idle"
    /\ staged' = {}
    /\ visible' = durable
    /\ savepoint' = FALSE
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
    \/ Rollback
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

OnlyDurableWritesAreVisible == visible = durable
AcknowledgedCommitsAreDurable == acknowledged \subseteq durable
DiscardedWritesNeverBecomeDurable == (discarded \cap durable) = {}
IdleHasNoStagedWrites == state = "Idle" => staged = {}
DeadProcessHasNoActiveTransaction == ~alive => state = "Idle" /\ staged = {}

Spec == Init /\ [][Next]_vars

=============================================================================
