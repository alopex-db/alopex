"""Exercise the build script's EXIT cleanup without compiling the parser."""

import subprocess
import unittest
from pathlib import Path

BUILD_SCRIPT = Path(__file__).resolve().parents[3] / "scripts/build-nim-parser.sh"


def cleanup_harness() -> str:
    text = BUILD_SCRIPT.read_text(encoding="utf-8")
    start = text.index("  cleanup_host_build() {\n")
    end = text.index("\n  }", start) + len("\n  }")
    cleanup = text[start:end]
    return (
        r"""
set -eu
sandbox=$(mktemp -d)
trap 'command rm -rf -- "$sandbox"' EXIT
"""
        + cleanup
        + r"""
for scenario in transient silent_transient permanent original_error unowned recreated_temp; do
  build_root="$sandbox/$scenario"
  build_lock="$build_root.lock"
  OUTPUT="$sandbox/output-$scenario"
  attempts="$sandbox/attempts-$scenario"
  mkdir "$build_root" "$build_lock"
  touch "$build_root/owned" "$OUTPUT"
  set +e
  (
    build_root_created=1
    build_lock_created=1
    build_succeeded=1
    cleanup_environment=("HOME=$sandbox" "USERPROFILE" "XDG_CACHE_HOME=" "TEMP=$sandbox" "TMP=$sandbox" "TMPDIR=$sandbox")
    export HOME="$build_root/home" USERPROFILE="$build_root/home"
    export XDG_CACHE_HOME="$build_root/cache" TEMP="$build_root/tmp"
    export TMP="$build_root/tmp" TMPDIR="$build_root/tmp"
    count=0
    if [[ "$scenario" == unowned ]]; then
      build_root_created=0
      build_lock_created=0
    fi
    if [[ "$scenario" == original_error ]]; then build_succeeded=0; fi
    rm() {
      if [[ "${!#}" == "$build_root" ]]; then
        count=$((count + 1))
        echo "$count" > "$attempts"
        case "$scenario" in
          transient) if [[ "$count" == 1 ]]; then return 1; fi ;;
          silent_transient) if [[ "$count" == 1 ]]; then return 0; fi ;;
          permanent|original_error) return 1 ;;
        esac
      fi
      command rm "$@"
    }
    rmdir() {
      # Reproduce a Windows cleanup child recreating its configured temp dir
      # after rm has already removed the root successfully.
      if [[ "$scenario" == recreated_temp ]]; then
        mkdir -p "$TEMP"
        [[ "$HOME" == "$sandbox" && "${USERPROFILE+x}" != x && "$XDG_CACHE_HOME" == "" ]] || return 1
        [[ "$TMP" == "$sandbox" && "$TMPDIR" == "$sandbox" ]] || return 1
      fi
      command rmdir "$@"
    }
    sleep() { :; }
    trap cleanup_host_build EXIT
    if [[ "$scenario" == original_error ]]; then exit 23; fi
    exit 0
  )
  status=$?
  set -e
  case "$scenario" in
    transient|silent_transient)
      [[ "$status" == 0 && ! -e "$build_root" && ! -e "$build_lock" ]] || exit 1
      [[ "$(cat "$attempts")" == 2 && -e "$OUTPUT" ]] || exit 1
      ;;
    recreated_temp)
      [[ "$status" == 0 && ! -e "$build_root" && ! -e "$build_lock" ]] || exit 1
      [[ "$(cat "$attempts")" == 1 && -e "$OUTPUT" ]] || exit 1
      ;;
    permanent)
      [[ "$status" == 1 && -e "$build_root/owned" && ! -e "$build_lock" ]] || exit 1
      [[ "$(cat "$attempts")" == 5 && -e "$OUTPUT" ]] || exit 1
      ;;
    original_error)
      [[ "$status" == 23 && -e "$build_root/owned" && ! -e "$build_lock" ]] || exit 1
      [[ "$(cat "$attempts")" == 5 && ! -e "$OUTPUT" ]] || exit 1
      ;;
    unowned)
      [[ "$status" == 0 && -e "$build_root/owned" && -d "$build_lock" ]] || exit 1
      [[ ! -e "$attempts" && -e "$OUTPUT" ]] || exit 1
      ;;
  esac
  echo "PASS $scenario"
done
"""
    )


class ParserBuildCleanupTests(unittest.TestCase):
    def test_cleanup_contract(self) -> None:
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-s"],
            input=cleanup_harness(),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count("PASS "), 6)


if __name__ == "__main__":
    unittest.main()
