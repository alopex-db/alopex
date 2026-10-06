# Parser wire fixtures

Codex captured both fixtures from `alopex_parse_sql` in a normally built macOS
arm64 parser, not from Rust serialization. Each row records its SQL, raw
MessagePack hex and payload SHA-256. The Rust and Nim tests compare decoded
wire maps, so key order is not an added compatibility requirement.

| Fixture | Producer source identity | Dynamic library SHA-256 |
|---|---|---|
| `parser_wire_v026_120f.json` | base `120f8b0654fe1421f332b9b2a9f135f9e317d372`; tree `b8806ae97e132b1ac087ab4e6eeaaa8ffe60dad422b8ee3c910f62df8f94aa47` | `53d5d8e95b905a0ff4d41097c1c863e8e8f697168c4da29046fb1152343bebcd` |
| `parser_wire_v027.json` | same base plus #571/#586 producer fix; tree `4afe5740b2c62351c7d0b6304811e1b434e1955ec24722648bc762832bc13b8c` | `9d78c8a0b8ed75cfb65f1551669d792200f9adf342ffb3301dfe91d6df3e266e` |

Codex used Nim2.2.10 (`1071aa82f23f3448e2bf91f18f5b8ddb65fa037ae4ccafb35fd264439aedde70`),
Nimble0.22.3 at `42ef70c2102a942c46f13eb76872326edd525cec`,
msgpack4nim0.4.4 (`462002b97d57683173c49a0110182f0edb4bfb74d523cb15f569f79bcf88f4fc`),
and npeg1.3.0 (`83cd5c1fd9ee21e81306b5e15a5080c7aac132984ef87d5c804a870b55959b0b`).
The normal build used release/ORC/speed, fixed target-qualified dependency
paths, SOURCE_DATE_EPOCH=0 and TZ=UTC. The source-tree digest follows the
release parser-asset producer's canonical input inventory.

The 0.26 fixture is immutable historical output, including the missing JOIN
natural flag. The 0.27 fixture fixes the current public wire shape. Future
semantic wire changes require a new versioned fixture and contract decision;
the old file must not be relabeled. The separate continuous-aggregate golden
retains its historical staged-byte contract.
