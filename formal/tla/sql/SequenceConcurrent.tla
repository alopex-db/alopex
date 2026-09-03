------------------------ MODULE SequenceConcurrent ------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Clients, MaxSequence

VARIABLES nextValue, version, status, snapshot, staged, committed

vars == <<nextValue, version, status, snapshot, staged, committed>>

Init == /\ nextValue = 1
        /\ version = 0
        /\ status = [client \in Clients |-> "idle"]
        /\ snapshot = [client \in Clients |-> 0]
        /\ staged = [client \in Clients |-> 0]
        /\ committed = {}

Begin(client) == /\ status[client] = "idle"
                 /\ status' = [status EXCEPT ![client] = "active"]
                 /\ snapshot' = [snapshot EXCEPT ![client] = version]
                 /\ staged' = [staged EXCEPT ![client] = 0]
                 /\ UNCHANGED <<nextValue, version, committed>>

Allocate(client) == /\ status[client] = "active"
                    /\ staged[client] = 0
                    /\ nextValue <= MaxSequence
                    /\ staged' = [staged EXCEPT ![client] = nextValue]
                    /\ UNCHANGED <<nextValue, version, status, snapshot, committed>>

Commit(client) == /\ status[client] = "active"
                  /\ staged[client] > 0
                  /\ snapshot[client] = version
                  /\ status' = [status EXCEPT ![client] = "idle"]
                  /\ staged' = [staged EXCEPT ![client] = 0]
                  /\ committed' = committed \union {staged[client]}
                  /\ nextValue' = nextValue + 1
                  /\ version' = version + 1
                  /\ UNCHANGED snapshot

Conflict(client) == /\ status[client] = "active"
                    /\ staged[client] > 0
                    /\ snapshot[client] # version
                    /\ status' = [status EXCEPT ![client] = "conflict"]
                    /\ staged' = [staged EXCEPT ![client] = 0]
                    /\ UNCHANGED <<nextValue, version, snapshot, committed>>

Rollback(client) == /\ status[client] \in {"active", "conflict"}
                    /\ status' = [status EXCEPT ![client] = "idle"]
                    /\ staged' = [staged EXCEPT ![client] = 0]
                    /\ UNCHANGED <<nextValue, version, snapshot, committed>>

Next == \E client \in Clients:
          Begin(client) \/ Allocate(client) \/ Commit(client)
          \/ Conflict(client) \/ Rollback(client)

Spec == Init /\ [][Next]_vars

TypeOK == /\ nextValue \in 1..(MaxSequence + 1)
          /\ version \in Nat
          /\ status \in [Clients -> {"idle", "active", "conflict"}]
          /\ snapshot \in [Clients -> Nat]
          /\ staged \in [Clients -> 0..MaxSequence]
          /\ committed \subseteq 1..MaxSequence

CommittedValuesAreUnique == Cardinality(committed) = version
CommittedPrefixIsGapFree == committed = 1..version

=============================================================================
