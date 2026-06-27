# Package
version       = "0.1.0"
author        = "Alopex Team"
description   = "SQL parser for Alopex DB — trial implementation in Nim"
license       = "Apache-2.0"
srcDir        = "src"

# Dependencies
requires "nim >= 2.2"
requires "npeg"
requires "msgpack4nim"

# Tasks
task test, "Run tests":
  exec "nim c -r tests/test_parser.nim"

task lib, "Build shared library":
  exec "nim c -d:release --app:lib --mm:orc --opt:speed -o:libalopex_sql_parser.so src/alopex_sql_parser.nim"
