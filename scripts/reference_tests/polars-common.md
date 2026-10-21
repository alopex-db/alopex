# Ordinary Polars operation conformance audit

This audit implements the common-operation acceptance matrix for Issue #722.
It runs the same operation against real Polars 1.43.2 and an Alopex package,
in one fresh process per engine/case. It is a functional audit, not a benchmark
or proof of complete Polars coverage.

```bash
python -m pip install -r scripts/reference_tests/requirements-polars-common.txt
python -B -m unittest discover -s scripts/reference_tests -p test_polars_common.py
python -B scripts/reference_tests/polars_common_v0816.py --output <new-run> --data <temporary-fixtures>
```

The command exits 0 only when every contract passes, 1 on subject failures,
and 2 when a reference scenario is invalid. Unsupported APIs are failures;
they are never passing skips or expected failures. A worker exit code 0 means
it wrote a diagnostic successfully, not that its operation was compatible.

The matrix covers construction, eager/lazy selection, Expr and Series
operations, aggregation, table/null operations, joins/concat, namespaces,
types/shapes/windows, CSV/Parquet I/O, batch boundaries and error contracts.
The case registry and generated matrix.json define the exact coverage.
An existing-Series diagnostic isolates arithmetic from direct construction.

Comparison requires output kind, every value and Python scalar type, NULL,
column/row order, and schema/dtype. Missing schema information fails the
contract even when values match. Error cases compare exception type names;
library-specific exception classes do not establish exception compatibility.
Batch comparison consumes every batch and compares the complete result.

To test a locally built candidate, install its wheel instead of the pinned
Alopex baseline and pass `--subject-version <installed-version>`. The Polars
reference remains pinned. The run records both package versions, the source
commit and source/fixture hashes. Use a new output and fixture directory for
every run; retain failed evidence and remove temporary fixtures afterward.

The audit can remain red while product gaps are open. Passing the audit's
unit tests proves the result classifier, not product compatibility. Issue
#722 must retain the full matrix as a regression requirement and must not be
closed merely because its initially reported six examples have been fixed.
