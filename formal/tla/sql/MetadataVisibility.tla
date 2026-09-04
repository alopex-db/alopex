------------------------ MODULE MetadataVisibility ------------------------
EXTENDS FiniteSets, TLC

CONSTANT Objects
VARIABLES durable, temporary, overlay, transaction, remoteOutcome

vars == <<durable, temporary, overlay, transaction, remoteOutcome>>

Init == /\ durable = {"base"}
        /\ temporary = {}
        /\ overlay = {}
        /\ transaction = "idle"
        /\ remoteOutcome = "none"

CreateTemporary == /\ transaction = "idle"
                   /\ temporary' = temporary \union {"scratch"}
                   /\ UNCHANGED <<durable, overlay, transaction, remoteOutcome>>

Begin == /\ transaction = "idle"
         /\ transaction' = "active"
         /\ overlay' = {}
         /\ UNCHANGED <<durable, temporary, remoteOutcome>>

CreateInTransaction == /\ transaction = "active"
                       /\ overlay' = overlay \union {"pending"}
                       /\ UNCHANGED <<durable, temporary, transaction, remoteOutcome>>

Commit == /\ transaction = "active"
          /\ durable' = durable \union overlay
          /\ overlay' = {}
          /\ transaction' = "idle"
          /\ UNCHANGED <<temporary, remoteOutcome>>

Rollback == /\ transaction = "active"
            /\ overlay' = {}
            /\ transaction' = "idle"
            /\ UNCHANGED <<durable, temporary, remoteOutcome>>

Reopen == /\ transaction = "idle"
          /\ temporary' = {}
          /\ UNCHANGED <<durable, overlay, transaction, remoteOutcome>>

RequestDistributed == /\ remoteOutcome = "none"
                      /\ remoteOutcome' = "rejected"
                      /\ UNCHANGED <<durable, temporary, overlay, transaction>>

Next == CreateTemporary \/ Begin \/ CreateInTransaction \/ Commit \/ Rollback
        \/ Reopen \/ RequestDistributed

Spec == Init /\ [][Next]_vars

Visible == durable \union temporary \union overlay
TypeOK == /\ durable \subseteq Objects
          /\ temporary \subseteq Objects
          /\ overlay \subseteq Objects
          /\ transaction \in {"idle", "active"}
          /\ remoteOutcome \in {"none", "rejected"}
TemporaryIsNotDurable == "scratch" \in temporary => "scratch" \notin durable
IdleHasNoOverlay == transaction = "idle" => overlay = {}
DistributedFailsClosed == remoteOutcome # "partial"

=============================================================================
