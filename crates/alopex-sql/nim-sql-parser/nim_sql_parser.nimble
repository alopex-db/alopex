import std/[os, strutils]

# Package
version       = "0.2.0"
author        = "Alopex Team"
description   = "SQL parser for Alopex DB — trial implementation in Nim"
license       = "Apache-2.0"
srcDir        = "src"

# Dependencies
requires "nim >= 2.2"
requires "npeg == 1.3.0"
requires "msgpack4nim == 0.4.4"

# Tasks
task test, "Run tests":
  const requiredNimVersion = "2.2.10"
  const requiredNimbleVersion = "0.22.3"
  const requiredNimbleSha = "42ef70c2102a942c46f13eb76872326edd525cec"
  const failureInjectionEnv = "ALOPEX_NIM_PARSER_INJECT_FAILURE"
  const buildRootEnv = "ALOPEX_NIM_TEST_BUILD_ROOT"
  const nimBinEnv = "ALOPEX_NIM_BIN"

  let nimBin = getEnv(nimBinEnv)
  if nimBin.len == 0 or not fileExists(nimBin):
    echo nimBinEnv & " must name the resolved Nim executable"
    quit(2)

  let nimIdentity = gorgeEx(quoteShell(nimBin) & " --version")
  if nimIdentity.exitCode != 0:
    echo nimIdentity.output
    echo "failed to inspect the resolved Nim executable"
    quit(2)
  var actualNimVersion = ""
  const nimVersionPrefix = "Nim Compiler Version "
  for line in nimIdentity.output.splitLines:
    if line.startsWith(nimVersionPrefix):
      let fields = line[nimVersionPrefix.len .. ^1].splitWhitespace
      if fields.len > 0:
        actualNimVersion = fields[0]
      break
  if actualNimVersion != requiredNimVersion:
    echo "Nim " & requiredNimVersion & " is required, found " &
      (if actualNimVersion.len == 0: "unknown" else: actualNimVersion)
    quit(2)

  let nimbleIdentity = gorgeEx(quoteShell(nimbleExe) & " --version")
  if nimbleIdentity.exitCode != 0:
    echo nimbleIdentity.output
    echo "failed to inspect the invoking Nimble executable"
    quit(2)
  let nimbleShaLine = "git hash: " & requiredNimbleSha
  var hasRequiredNimbleVersion = false
  var hasRequiredNimbleSha = false
  for line in nimbleIdentity.output.splitLines:
    if line.startsWith("nimble v"):
      let fields = line["nimble v".len .. ^1].splitWhitespace
      if fields.len > 0 and fields[0] == requiredNimbleVersion:
        hasRequiredNimbleVersion = true
    if line == nimbleShaLine:
      hasRequiredNimbleSha = true
  if not hasRequiredNimbleVersion:
    echo "Nimble " & requiredNimbleVersion &
      " is required from the invoking executable"
    quit(2)
  if not hasRequiredNimbleSha:
    echo "Nimble must be built from " & requiredNimbleSha
    quit(2)

  let injectFailure = getEnv(failureInjectionEnv, "0")
  if injectFailure notin ["0", "1"]:
    echo failureInjectionEnv & " must be 0 or 1"
    quit(2)

  let suppliedBuildRoot = getEnv(buildRootEnv)
  if suppliedBuildRoot.len == 0:
    echo buildRootEnv & " must name an invocation-owned temporary directory"
    quit(2)
  let buildRoot = suppliedBuildRoot
  let binDir = buildRoot / "bin"
  let nimcacheDir = buildRoot / "nimcache"
  if not dirExists(binDir) or not dirExists(nimcacheDir):
    echo buildRootEnv & " must contain pre-created bin and nimcache directories"
    quit(2)

  let testFiles =
    if injectFailure == "1":
      @["tests/test_harness_failure.nim"]
    else:
      @[
        "tests/test_parser.nim",
        "tests/test_promql_parser.nim",
        "tests/test_msgpack_output.nim",
        "tests/test_ffi_boundary.nim",
        "tests/test_security_limits.nim"
      ]

  for testFile in testFiles:
    let executable = binDir / splitFile(testFile).name
    let command = quoteShell(nimBin) & " c -r --nimcache:" &
      quoteShell(nimcacheDir) & " -o:" & quoteShell(executable) & " " &
      quoteShell(testFile)
    let result = gorgeEx(command)
    echo result.output
    if result.exitCode != 0:
      quit(result.exitCode)

task lib, "Build shared library":
  # 前提 (issue #40 対応時に Nim 2.2.10 / alopex-parity コンテナで実測検証済み):
  # このタスクは `--panics` / `--exceptions` を明示指定しない。Nim 2.2 の
  # 既定は `--panics:off` (Defect は Exception 階層に属し catchable) と
  # `--exceptions:goto` であり、`-d:release --mm:orc --opt:speed` を付けても
  # 変わらない。`alopex_sql_parser.nim` の `alopex_parse_sql` は
  # `except CatchableError` に加えて `except Defect` で IndexDefect/
  # FieldDefect 等の内部不変条件違反を捕捉して prkError に変換しており、
  # これは `--panics:off` (Defect が catchable) 前提で成立する。将来
  # `--panics:on` (Defect が回復不能な fatal abort になる、Nim 3 相当の
  # 動作) へ切り替える場合は、この except 節が到達不能になり FFI 境界の
  # no-throw 保証が崩れるため、alopex_sql_parser.nim 側の設計も併せて
  # 見直すこと。フラグを明示追加しない理由: `--exceptions:goto` を明示すると
  # Windows MinGW ターゲットでのコード生成差分を検証していないため、
  # 既定値への暗黙依存よりリスクが高いと判断した。
  #
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
