-------------------------- MODULE ExplainLifecycle --------------------------
EXTENDS Naturals, TLC

VARIABLES phase, analyze, writes

vars == <<phase, analyze, writes>>

Init == /\ phase = "idle"
        /\ analyze = FALSE
        /\ writes = 0

StartPlain == /\ phase = "idle"
              /\ phase' = "planned"
              /\ analyze' = FALSE
              /\ UNCHANGED writes

StartAnalyze == /\ phase = "idle"
                /\ phase' = "executing"
                /\ analyze' = TRUE
                /\ UNCHANGED writes

FinishPlain == /\ phase = "planned"
               /\ phase' = "succeeded"
               /\ UNCHANGED <<analyze, writes>>

FinishAnalyze == /\ phase = "executing"
                 /\ phase' = "succeeded"
                 /\ writes' \in 0..2
                 /\ UNCHANGED analyze

FailAnalyze == /\ phase = "executing"
               /\ phase' = "failed"
               /\ writes' = 0
               /\ UNCHANGED analyze

CancelBefore == /\ phase = "idle"
                /\ phase' = "cancelled"
                /\ UNCHANGED <<analyze, writes>>

Next == StartPlain \/ StartAnalyze \/ FinishPlain \/ FinishAnalyze
        \/ FailAnalyze \/ CancelBefore

Spec == Init /\ [][Next]_vars

PlainNeverWrites == ~analyze => writes = 0
FailureIsRolledBack == phase \in {"failed", "cancelled"} => writes = 0
TypeOK == /\ phase \in {"idle", "planned", "executing", "succeeded", "failed", "cancelled"}
          /\ analyze \in BOOLEAN
          /\ writes \in Nat

=============================================================================
