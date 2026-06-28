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
  exec "nim c -r tests/test_msgpack_output.nim"

task lib, "Build shared library":
  # OS 別に build.rs が探す正確なファイル名で出力する。
  #   Linux:   libalopex_sql_parser.so   (lib 接頭辞 + .so)
  #   macOS:   libalopex_sql_parser.dylib(lib 接頭辞 + .dylib)
  #   Windows: alopex_sql_parser.dll      (接頭辞なし + .dll)
  # `--app:lib` の OS 既定名に依存せず、-o: で明示することで
  # クロスプラットフォームで build.rs の nim_lib_filename() と一致させる。
  let outName =
    when defined(windows):
      "alopex_sql_parser.dll"
    elif defined(macosx):
      "libalopex_sql_parser.dylib"
    else:
      "libalopex_sql_parser.so"
  exec "nim c -d:release --app:lib --mm:orc --opt:speed -o:" & outName &
    " src/alopex_sql_parser.nim"
