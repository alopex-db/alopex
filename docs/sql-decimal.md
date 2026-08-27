# Exact DECIMAL and NUMERIC values

Alopex v0.8.10 supports `DECIMAL` and `NUMERIC` with up to 38 digits. The accepted forms are `DECIMAL`, `DECIMAL(p)`, and `DECIMAL(p,s)`; `NUMERIC` is an alias. `DECIMAL '12.34'` creates an exact literal without first converting through floating point.

The runtime stores a signed 128-bit coefficient and a scale. Assignment and `CAST` round discarded digits halfway away from zero and reject precision overflow. Arithmetic, comparison, `SUM`, `AVG`, `MIN`, and `MAX` preserve exact decimal values; `AVG` returns at least six fractional digits.

```sql
CREATE TABLE invoice (id INTEGER PRIMARY KEY, amount DECIMAL(10,2));
INSERT INTO invoice VALUES (1, DECIMAL '12.345');
SELECT amount, amount * DECIMAL '2.0' FROM invoice;
```

Arrow and Parquet use `Decimal128`. Python results use `decimal.Decimal`, while CLI output renders decimal text to avoid binary floating-point loss. Existing row tags and protocol fields remain unchanged; DECIMAL uses appended row tag `0x0d` and gRPC field 13. B-tree indexes reject DECIMAL in v0.8.10 because their byte ordering does not yet normalize differing scales.
