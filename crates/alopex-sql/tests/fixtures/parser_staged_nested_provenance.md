# Frozen staged nested-query fixtures

`parser_staged_nested_v026_120f.json` contains exact MessagePack bytes emitted
by commit `120f8b0654fe1421f332b9b2a9f135f9e317d372`, not the changed producer.
The fixture records the source commit, exported contract, source-manifest
digest, exact SQL, matching public SELECT, and raw payload hex for each case.

The capture used Nim 2.2.10 (compiler commit
`bfeb3146d1638b39f69007a4ae5a23e23ae4e5ef`), msgpack4nim 0.4.4
(VCS revision `f4cc097ca9694f17feced9f82994f583ef7911fe`, Nimble package-directory
checksum `af47afd90e9523ea360f7c9bc0aa6b5f6bcaf786`), and npeg 1.3.0
(VCS revision `409f6796d0e880b3f0222c964d1da7de6e450811`, Nimble package-directory
checksum `64f15c85a059c889cb11c5fe72372677c50da621`). An isolated archive of the old
parser source and existing `test_msgpack_output.nim` supplied all producer
code. The probe enabled `alopexSqlParserContractTests` and called
`encodeContinuousAggregateV040ToMsgPack(parseSql(sql))` without rewriting
the AST or encoded bytes.

The same process ran only the existing canonical golden test, selected with
`MessagePack output - staged continuous aggregate contract::requirements canonical SQL owns exact JSON, spans, and MessagePack bytes`.
That JSON/Span/byte control passed; all six new inputs were accepted.

| Case | Old payload bytes | Distinct writer path |
|---|---:|---|
| scalar | 1882 | ScalarSubquery → Statement |
| exists | 1883 | Exists → Statement |
| in | 1993 | InSubquery → Statement |
| quantified | 2005 | Quantified → Statement |
| derived_set | 2754 | Derived → QueryBody → UNION ALL right branch |
| cte_values | 2719 | CTE → QueryBody → VALUES → ScalarSubquery |

Capture identities:

- Old source manifest: `f5668e30693c90469f37baa229495185daef2b12d1bd093b58825f56d009c496`
- Probe: `2a232aa21ee537e0188f78573aa1bd963af76b87420a52e74ea804a37e009005`
- Capture log: `5ffbf8f6baf53db83da4667c40eefad6f0860baeddce3a3664e31b06c3557708`
- Fixture: `d1d4e7ca11f78f8bc1696a2da7dfdc3949838981507a1a68f3d366fdfc750e9e`

The owning tests live in `nim-sql-parser/tests/test_msgpack_output.nim`,
suite `MessagePack output - staged nested JOIN compatibility`. Each staged
case compares complete bytes, including spans, against this immutable old
reference. Each public SELECT separately requires exactly one JOIN and its
`natural: false` field. The existing public NATURAL test owns `natural: true`.
Regenerating this fixture from the changed producer would erase the contract
being tested; a version bump alone does not authorize that replacement.
