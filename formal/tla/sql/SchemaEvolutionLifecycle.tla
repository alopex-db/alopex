--------------------- MODULE SchemaEvolutionLifecycle ---------------------
EXTENDS TLC

VARIABLES schema, rows, base, view, transaction, pendingSchema, pendingRows

vars == <<schema, rows, base, view, transaction, pendingSchema, pendingRows>>

Init == /\ schema = "old"
        /\ rows = "full"
        /\ base = TRUE
        /\ view = FALSE
        /\ transaction = "idle"
        /\ pendingSchema = "none"
        /\ pendingRows = "none"

Begin == /\ transaction = "idle"
         /\ transaction' = "active"
         /\ pendingSchema' = schema
         /\ pendingRows' = rows
         /\ UNCHANGED <<schema, rows, base, view>>

Alter == /\ transaction = "active"
         /\ pendingSchema' = "new"
         /\ UNCHANGED <<schema, rows, base, view, transaction, pendingRows>>

Truncate == /\ transaction = "active"
            /\ pendingRows' = "empty"
            /\ UNCHANGED <<schema, rows, base, view, transaction, pendingSchema>>

Commit == /\ transaction = "active"
          /\ schema' = pendingSchema
          /\ rows' = pendingRows
          /\ transaction' = "idle"
          /\ pendingSchema' = "none"
          /\ pendingRows' = "none"
          /\ UNCHANGED <<base, view>>

RollbackOrCrash == /\ transaction = "active"
                   /\ transaction' = "idle"
                   /\ pendingSchema' = "none"
                   /\ pendingRows' = "none"
                   /\ UNCHANGED <<schema, rows, base, view>>

CreateView == /\ transaction = "idle"
              /\ base
              /\ view' = TRUE
              /\ UNCHANGED <<schema, rows, base, transaction, pendingSchema, pendingRows>>

DropView == /\ transaction = "idle"
            /\ view' = FALSE
            /\ UNCHANGED <<schema, rows, base, transaction, pendingSchema, pendingRows>>

DropBase == /\ transaction = "idle"
            /\ ~view
            /\ base' = FALSE
            /\ UNCHANGED <<schema, rows, view, transaction, pendingSchema, pendingRows>>

Reopen == /\ transaction = "idle"
          /\ UNCHANGED vars

Next == Begin \/ Alter \/ Truncate \/ Commit \/ RollbackOrCrash
        \/ CreateView \/ DropView \/ DropBase \/ Reopen

Spec == Init /\ [][Next]_vars

TypeOK == /\ schema \in {"old", "new"}
          /\ rows \in {"full", "empty"}
          /\ base \in BOOLEAN
          /\ view \in BOOLEAN
          /\ transaction \in {"idle", "active"}
          /\ pendingSchema \in {"none", "old", "new"}
          /\ pendingRows \in {"none", "full", "empty"}

IdleHasNoPartialMigration == transaction = "idle" =>
  /\ pendingSchema = "none"
  /\ pendingRows = "none"

ViewKeepsDependency == view => base

=============================================================================
