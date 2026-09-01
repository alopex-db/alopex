# Prepared statements

## Contract

Alopex accepts only positional `?` parameters. Bind indices are one-based. `$1`, named parameters such as `:name`, and parameters in identifier positions are rejected. Parameters represent values only.

The Nim/Rust MessagePack parser contract is `0.21.0`; it carries each placeholder as `Parameter { index }`.

Rust callers use `Arc<Database>::prepare` for auto-commit execution or `SqlSession::prepare` for execution inside the session's active transaction. Both statement types expose `parameter_count`, `bind`, `execute`, `reset`, and `finalize`. `reset` clears bindings for reuse; `finalize` permanently closes the statement. Missing and out-of-range bindings return typed errors.

The implementation does not cache a logical or physical plan. Each execution safely renders bound values and reparses the statement, so schema changes are observed automatically and a statement can be retried after a corrected schema change.

## Value handling

The Rust API accepts `SqlValue` values. It supports null, booleans, integer and finite floating-point values, text, decimal, JSON, and finite vectors. Text and JSON are quoted as SQL literals, so their content cannot introduce identifiers or SQL syntax.

Python exposes the same lifecycle through `Database.prepare`. The CLI binds repeatable JSON values in positional order:

```text
alopex --in-memory sql "SELECT ?" --param '42'
alopex --in-memory sql "SELECT ?" --param '"text"'
```

CLI JSON objects are unsupported. JSON arrays must contain only finite numbers and become vectors.

## Concurrency and lifetime

An auto-commit prepared statement owns an `Arc<Database>` and can move to another thread. A session prepared statement mutably borrows its `SqlSession`, preventing concurrent use of that session while the statement exists. Finalized statements reject bind, reset, execute, and repeated finalize operations.
