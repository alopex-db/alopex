------------------------ MODULE AdvancedDmlAtomicity ------------------------
EXTENDS Naturals

CONSTANT Clients

VARIABLES rows, version, status, snapshot, staged, publishedBy

vars == <<rows, version, status, snapshot, staged, publishedBy>>

Init == /\ rows = "old"
        /\ version = 0
        /\ status = [client \in Clients |-> "idle"]
        /\ snapshot = [client \in Clients |-> 0]
        /\ staged = [client \in Clients |-> "none"]
        /\ publishedBy = {}

Begin(client) == /\ status[client] = "idle"
                 /\ status' = [status EXCEPT ![client] = "active"]
                 /\ snapshot' = [snapshot EXCEPT ![client] = version]
                 /\ UNCHANGED <<rows, version, staged, publishedBy>>

StageValid(client) == /\ status[client] = "active"
                      /\ staged[client] = "none"
                      /\ staged' = [staged EXCEPT ![client] = "updated"]
                      /\ UNCHANGED <<rows, version, status, snapshot, publishedBy>>

StageMultipleMatch(client) == /\ status[client] = "active"
                              /\ staged[client] = "none"
                              /\ staged' = [staged EXCEPT ![client] = "multiple-match"]
                              /\ UNCHANGED <<rows, version, status, snapshot, publishedBy>>

Commit(client) == /\ status[client] = "active"
                  /\ staged[client] = "updated"
                  /\ snapshot[client] = version
                  /\ rows' = client
                  /\ version' = version + 1
                  /\ status' = [status EXCEPT ![client] = "committed"]
                  /\ staged' = [staged EXCEPT ![client] = "none"]
                  /\ publishedBy' = publishedBy \union {client}
                  /\ UNCHANGED snapshot

RejectMultipleMatch(client) ==
    /\ status[client] = "active"
    /\ staged[client] = "multiple-match"
    /\ status' = [status EXCEPT ![client] = "rejected"]
    /\ staged' = [staged EXCEPT ![client] = "none"]
    /\ UNCHANGED <<rows, version, snapshot, publishedBy>>

Conflict(client) == /\ status[client] = "active"
                    /\ staged[client] = "updated"
                    /\ snapshot[client] # version
                    /\ status' = [status EXCEPT ![client] = "conflict"]
                    /\ staged' = [staged EXCEPT ![client] = "none"]
                    /\ UNCHANGED <<rows, version, snapshot, publishedBy>>

Retry(client) == /\ status[client] \in {"rejected", "conflict"}
                 /\ status' = [status EXCEPT ![client] = "idle"]
                 /\ UNCHANGED <<rows, version, snapshot, staged, publishedBy>>

Next == \E client \in Clients:
          Begin(client) \/ StageValid(client) \/ StageMultipleMatch(client)
          \/ Commit(client) \/ RejectMultipleMatch(client) \/ Conflict(client)
          \/ Retry(client)

Spec == Init /\ [][Next]_vars

TypeOK == /\ rows \in {"old"} \union Clients
          /\ version \in Nat
          /\ status \in [Clients -> {"idle", "active", "committed", "rejected", "conflict"}]
          /\ snapshot \in [Clients -> Nat]
          /\ staged \in [Clients -> {"none", "updated", "multiple-match"}]
          /\ publishedBy \subseteq Clients

RejectedOrConflictedDoesNotPublish ==
    \A client \in Clients:
        status[client] \in {"rejected", "conflict"} => client \notin publishedBy

OnlyCommitPublishes == rows = "old" \/ rows \in publishedBy

=============================================================================
