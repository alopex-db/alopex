"""Build the pinned CSV/Parquet fixtures once for every comparison subject."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'scripts/performance/workloads'))
from polars_entrypoints_v0816 import fixtures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory',type=Path,required=True)
    parser.add_argument('--rows',type=int,choices=(3,34250),required=True)
    parser.add_argument('--manifest',type=Path,required=True)
    args = parser.parse_args()
    fixtures(args.directory,args.rows)
    files = {}
    for ext in ('csv','parquet'):
        p=args.directory/('rows.'+ext);data=p.read_bytes()
        assert 0<len(data)<2_000_000
        p.chmod(0o400)
        files[ext]=dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
    args.manifest.write_text(json.dumps(dict(rows=args.rows,files=files))+'\n')


if __name__=='__main__':main()
