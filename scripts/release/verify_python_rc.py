"""Install one qualified wheel and execute the existing bounded public API checks."""
import argparse
import os
from pathlib import Path
import subprocess
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=("linux", "macos", "windows"), required=True)
    args = parser.parse_args(argv)
    workspace = Path(os.environ["GITHUB_WORKSPACE"])
    temporary = Path(os.environ["RUNNER_TEMP"])
    windows = args.kind == "windows"
    venv = temporary / ("alopex-wheel-clean-venv" if windows else "alopex-wheel-api-smoke")
    if venv.exists() or venv.is_symlink():
        raise RuntimeError("wheel qualification requires a fresh owned virtual environment")
    wheels = sorted((workspace / "dist").glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("expected one primary wheel for this host")
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    python = venv / ("Scripts/python.exe" if windows else "bin/python")
    dependencies = ["numpy==2.2.1"] + (["pytest"] if args.kind == "macos" else [])
    subprocess.run([str(python), "-m", "pip", "install", "--no-cache-dir", "--only-binary=:all:", *dependencies], check=True)
    subprocess.run([str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(wheels[0])], check=True)
    subprocess.run([str(python), str(workspace / "scripts/release/verify_python_vector_api.py")],
                   cwd=temporary if windows else workspace, check=True)
    if args.kind == "macos":
        subprocess.run([str(python), "-m", "pytest", "-q", "crates/alopex-py/tests/test_sql.py"],
                       cwd=workspace, check=True)


if __name__ == "__main__":
    main()
