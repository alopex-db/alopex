"""One complete #804-profile host allocation sample for the bounded monitor."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "test-env"))
from capacity import Budget  # noqa: E402
from common import load_profile  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", type=Path, required=True)
    args = parser.parse_args()
    profile = load_profile(args.profile)
    print(Budget(profile["storage"]).used())


if __name__ == "__main__":
    main()
