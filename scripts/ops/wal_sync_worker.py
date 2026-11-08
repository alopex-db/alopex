"""Run one source-bound ordinary-extension sync case on a manual managed VM."""
import argparse
import json
import os
from pathlib import Path
import runpy
import sys


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--configuration',type=Path,required=True)
    args=parser.parse_args();config=json.loads(args.configuration.read_bytes())
    assert sys.platform=='linux' and not sys.flags.optimize and os.getuid()!=0
    assert os.environ.get('GITHUB_EVENT_NAME')=='workflow_dispatch'
    assert os.environ.get('RUNNER_KIND')=='github-hosted'
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'scripts/compatibility'),str(root/'scripts/ops/dataframe_harness')]
    from subject_identity import verify_subject
    from wal_cost_worker import sha
    from wal_sync_contract import CASES
    assert config['case'] in CASES
    assert sorted(os.sched_getaffinity(0))==config['affinity']
    for name,expected in config['sources'].items():assert sha(root/name)==expected
    identity=verify_subject();assert identity['manifest_sha256']==config['subject_manifest_sha256']
    from alopex import _alopex
    assert sha(Path(_alopex.__file__))==config['native_sha256']
    (Path(config['output'])/'identity.json').write_text(json.dumps(identity)+'\n')
    sys.argv=[str(root/'scripts/compatibility/sync_syscall.py'),'--case',config['case'],
              '--output',str(Path(config['output'])/'result')]
    runpy.run_path(sys.argv[0],run_name='__main__')


if __name__=='__main__':main()
