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
  # Windows: MinGW ランタイム (libgcc_s_seh-1.dll / libwinpthread-1.dll 等) を
  # DLL へ静的リンクし、DLL を自己完結にする。Python の os.add_dll_directory()
  # は登録ディレクトリから推移的依存も解決するが、MinGW ランタイム DLL は
  # ビルド環境固有で配置が保証されないため、動的リンクのままだと
  # `import alopex` が DLL load failed になる (PR #32)。DLL 同梱 (issue #33) の
  # 布石として静的リンクを採用する。Linux/macOS のフラグは不変。
  let staticFlags =
    when defined(windows):
      " --passL:-static --passL:-static-libgcc"
    else:
      ""
  exec "nim c -d:release --app:lib --mm:orc --opt:speed" & staticFlags &
    " -o:" & outName & " src/alopex_sql_parser.nim"
