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
    dependencies = ["numpy==2.2.1", "pyarrow==26.0.0", "pytest==9.1.1"]
    subprocess.run([str(python), "-m", "pip", "install", "--no-cache-dir", "--only-binary=:all:", *dependencies], check=True)
    subprocess.run([str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(wheels[0])], check=True)
    subprocess.run([str(python), str(workspace / "scripts/release/verify_python_vector_api.py")],
                   cwd=temporary if windows else workspace, check=True)
    environment = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1", PYTHONDONTWRITEBYTECODE="1")
    for name in ("PYTHONPATH", "PYTHONHOME", "PYTEST_ADDOPTS", "PYTEST_PLUGINS",
                 "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH",
                 "NIM_SQL_PARSER_LIB_DIR", "ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD"):
        environment.pop(name, None)
    subprocess.run([
        str(python), "-I", str(workspace / "scripts/release/verify_python_parquet.py"),
        "--wheel", str(wheels[0]), "--workspace", str(workspace),
        "--report", str(workspace / "artifacts/python-release/vector-parquet.json"),
    ], cwd=temporary, env=environment, check=True)
    if args.kind == "macos":
        subprocess.run([str(python), "-m", "pytest", "-q", "crates/alopex-py/tests/test_sql.py"],
                       cwd=workspace, check=True)


if __name__ == "__main__":
    main()
