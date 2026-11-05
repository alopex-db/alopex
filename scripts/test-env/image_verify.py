"""Verify the installed package inventory against the resolved image lock."""
import json
from pathlib import Path
import subprocess
import sys

from common import require

lock = json.loads(Path('/tool/image-manifest.json').read_text())
require(sys.version_info[:2] == (3, 12), 'IDENTITY')
actual = dict(line.split('\t', 1) for line in subprocess.check_output(
    ['dpkg-query', '-W', '-f=${Package}\t${Version}\n'], text=True).splitlines())
require(actual == lock['packages'], 'IDENTITY')
require(subprocess.check_output(['/usr/lib/postgresql/16/bin/postgres', '--version'], text=True).split()[2] == '16.14', 'IDENTITY')
