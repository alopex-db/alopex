---------------------- MODULE PreparedStatementLifecycle ----------------------
EXTENDS FiniteSets, Integers

Params == {1, 2}
Schemas == {0, 1}

VARIABLES open, bound, schema, executions

vars == <<open, bound, schema, executions>>

Init ==
    /\ open = TRUE
    /\ bound = {}
    /\ schema = 0
    /\ executions = {}

Bind(p) ==
    /\ open
    /\ p \in Params
    /\ bound' = bound \cup {p}
    /\ UNCHANGED <<open, schema, executions>>

Reset ==
    /\ open
    /\ bound' = {}
    /\ UNCHANGED <<open, schema, executions>>

SchemaChange ==
    /\ open
    /\ schema' = 1 - schema
    /\ UNCHANGED <<open, bound, executions>>

Execute ==
    /\ open
    /\ bound = Params
    /\ executions' = executions \cup {schema}
    /\ UNCHANGED <<open, bound, schema>>

Finalize ==
    /\ open
    /\ open' = FALSE
    /\ bound' = {}
    /\ UNCHANGED <<schema, executions>>

Next ==
    \/ \E p \in Params : Bind(p)
    \/ Reset
    \/ SchemaChange
    \/ Execute
    \/ Finalize

TypeOK ==
    /\ open \in BOOLEAN
    /\ bound \subseteq Params
    /\ schema \in Schemas
    /\ executions \subseteq Schemas

FinalizedHasNoBindings == ~open => bound = {}
ExecutionUsesKnownSchema == executions \subseteq Schemas

Spec == Init /\ [][Next]_vars

=============================================================================
