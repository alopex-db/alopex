-------------------------- MODULE ForeignKeyCommitRace --------------------------
TxnStates == {"Staged", "Committed", "Conflict"}

VARIABLES parentExists, childExists, deleteTxn, insertTxn

vars == <<parentExists, childExists, deleteTxn, insertTxn>>

Init ==
    /\ parentExists = TRUE
    /\ childExists = FALSE
    /\ deleteTxn = "Staged"
    /\ insertTxn = "Staged"

CommitDelete ==
    /\ deleteTxn = "Staged"
    /\ insertTxn # "Committed"
    /\ parentExists' = FALSE
    /\ deleteTxn' = "Committed"
    /\ UNCHANGED <<childExists, insertTxn>>

RejectDeleteConflict ==
    /\ deleteTxn = "Staged"
    /\ insertTxn = "Committed"
    /\ deleteTxn' = "Conflict"
    /\ UNCHANGED <<parentExists, childExists, insertTxn>>

CommitInsert ==
    /\ insertTxn = "Staged"
    /\ deleteTxn # "Committed"
    /\ childExists' = TRUE
    /\ insertTxn' = "Committed"
    /\ UNCHANGED <<parentExists, deleteTxn>>

RejectInsertConflict ==
    /\ insertTxn = "Staged"
    /\ deleteTxn = "Committed"
    /\ insertTxn' = "Conflict"
    /\ UNCHANGED <<parentExists, childExists, deleteTxn>>

Done ==
    /\ deleteTxn # "Staged"
    /\ insertTxn # "Staged"
    /\ UNCHANGED vars

Next ==
    \/ CommitDelete
    \/ RejectDeleteConflict
    \/ CommitInsert
    \/ RejectInsertConflict
    \/ Done

TypeOK ==
    /\ parentExists \in BOOLEAN
    /\ childExists \in BOOLEAN
    /\ deleteTxn \in TxnStates
    /\ insertTxn \in TxnStates

ReferentialIntegrity == childExists => parentExists

ConflictDoesNotPublish ==
    /\ deleteTxn = "Conflict" => parentExists
    /\ insertTxn = "Conflict" => ~childExists

Spec == Init /\ [][Next]_vars

=============================================================================
