import std/[os, unittest]

const FailureInjectionEnv = "ALOPEX_NIM_PARSER_INJECT_FAILURE"

suite "Nim test harness failure propagation":
  test "controlled failure reaches the process exit status":
    check getEnv(FailureInjectionEnv, "0") != "1"
