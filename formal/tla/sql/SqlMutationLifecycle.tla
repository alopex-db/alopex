---------------------- MODULE SqlMutationLifecycle ----------------------
EXTENDS Naturals

CONSTANTS MaxSequence, MaxCascade

VARIABLES rows, txn, stagedRows, output, sequence, pendingSequence,
          lastValue, cascadeDepth, error

vars == <<rows, txn, stagedRows, output, sequence, pendingSequence,
          lastValue, cascadeDepth, error>>

Init == /\ rows = "valid"
        /\ txn = "idle"
        /\ stagedRows = "none"
        /\ output = "none"
        /\ sequence = 1
        /\ pendingSequence = 0
        /\ lastValue = 0
        /\ cascadeDepth = 0
        /\ error = "none"

Begin == /\ txn = "idle"
         /\ txn' = "active"
         /\ stagedRows' = rows
         /\ pendingSequence' = sequence
         /\ error' = "none"
         /\ UNCHANGED <<rows, output, sequence, lastValue, cascadeDepth>>

StageValidDml == /\ txn = "active"
                 /\ stagedRows' = "valid"
                 /\ UNCHANGED <<rows, txn, output, sequence, pendingSequence,
                                 lastValue, cascadeDepth, error>>

StageConstraintViolation == /\ txn = "active"
                            /\ stagedRows' = "invalid"
                            /\ UNCHANGED <<rows, txn, output, sequence,
                                            pendingSequence, lastValue,
                                            cascadeDepth, error>>

RejectInvalidCommit == /\ txn = "active"
                       /\ stagedRows = "invalid"
                       /\ txn' = "failed"
                       /\ error' = "constraint"
                       /\ UNCHANGED <<rows, stagedRows, output, sequence,
                                       pendingSequence, lastValue, cascadeDepth>>

MergeMultipleMatch == /\ txn = "active"
                      /\ txn' = "failed"
                      /\ error' = "multiple-match"
                      /\ UNCHANGED <<rows, stagedRows, output, sequence,
                                      pendingSequence, lastValue, cascadeDepth>>

NextVal == /\ txn = "active"
           /\ pendingSequence \in 1..MaxSequence
           /\ lastValue' = pendingSequence
           /\ pendingSequence' = IF pendingSequence = MaxSequence
                                  THEN 1 ELSE pendingSequence + 1
           /\ UNCHANGED <<rows, txn, stagedRows, output, sequence,
                           cascadeDepth, error>>

Cascade == /\ txn = "active"
           /\ cascadeDepth < MaxCascade
           /\ cascadeDepth' = cascadeDepth + 1
           /\ UNCHANGED <<rows, txn, stagedRows, output, sequence,
                           pendingSequence, lastValue, error>>

RejectCascadeLimit == /\ txn = "active"
                      /\ cascadeDepth = MaxCascade
                      /\ txn' = "failed"
                      /\ error' = "cascade-limit"
                      /\ UNCHANGED <<rows, stagedRows, output, sequence,
                                      pendingSequence, lastValue, cascadeDepth>>

Commit == /\ txn = "active"
          /\ stagedRows = "valid"
          /\ rows' = stagedRows
          /\ sequence' = pendingSequence
          /\ txn' = "idle"
          /\ stagedRows' = "none"
          /\ pendingSequence' = 0
          /\ cascadeDepth' = 0
          /\ error' = "none"
          /\ UNCHANGED <<output, lastValue>>

Rollback == /\ txn \in {"active", "failed"}
            /\ txn' = "idle"
            /\ stagedRows' = "none"
            /\ pendingSequence' = 0
            /\ cascadeDepth' = 0
            /\ error' = "none"
            /\ UNCHANGED <<rows, output, sequence, lastValue>>

CopyStart == /\ output \in {"none", "final"}
             /\ output' = "temporary"
             /\ UNCHANGED <<rows, txn, stagedRows, sequence,
                             pendingSequence, lastValue, cascadeDepth, error>>

CopyPublish == /\ output = "temporary"
               /\ output' = "final"
               /\ UNCHANGED <<rows, txn, stagedRows, sequence,
                               pendingSequence, lastValue, cascadeDepth, error>>

Crash == /\ txn \in {"active", "failed"} \/ output = "temporary"
         /\ txn' = "idle"
         /\ stagedRows' = "none"
         /\ pendingSequence' = 0
         /\ output' = output
         /\ cascadeDepth' = 0
         /\ error' = "none"
         /\ UNCHANGED <<rows, sequence, lastValue>>

RecoverCopyTemporary == /\ output = "temporary"
                        /\ output' = "none"
                        /\ UNCHANGED <<rows, txn, stagedRows, sequence,
                                        pendingSequence, lastValue,
                                        cascadeDepth, error>>

Next == Begin \/ StageValidDml \/ StageConstraintViolation
        \/ RejectInvalidCommit \/ MergeMultipleMatch \/ NextVal
        \/ Cascade \/ RejectCascadeLimit \/ Commit \/ Rollback
        \/ CopyStart \/ CopyPublish \/ Crash \/ RecoverCopyTemporary

Spec == Init /\ [][Next]_vars

TypeOK == /\ rows \in {"valid"}
          /\ txn \in {"idle", "active", "failed"}
          /\ stagedRows \in {"none", "valid", "invalid"}
          /\ output \in {"none", "temporary", "final"}
          /\ sequence \in 1..MaxSequence
          /\ pendingSequence \in 0..MaxSequence
          /\ lastValue \in 0..MaxSequence
          /\ cascadeDepth \in 0..MaxCascade
          /\ error \in {"none", "constraint", "multiple-match", "cascade-limit"}

CommittedRowsSatisfyConstraints == rows = "valid"

FailureDoesNotPublishRows == error # "none" => rows = "valid"

IdleHasNoStagedMutation == txn = "idle" =>
  /\ stagedRows = "none"
  /\ pendingSequence = 0

=============================================================================
