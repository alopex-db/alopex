"""Encrypt bounded wrapper evidence; publish only fixed outcomes and hashes."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile

CLOSURE_INPUT_LIMIT = 2_500_000
TOTAL_INPUT_LIMIT = 70_000_000


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def seal(qualification, frozen, private, output):
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    public = {'schema': 'alopex.wal-cost-same-job/v1', 'case': 'WAL_COST_682',
              'outcome': 'failed', 'error_code': 'INCOMPLETE'}
    evidence = []
    wrapper = private / 'wrapper-result.private.json'
    log = private / 'comparator.stdout.private.log'
    cost = frozen / 'wal-cost-export/result.json'
    encrypted_cost = frozen / 'wal-cost-export/private-evidence.cms'
    closure = private / 'closure-phase.private.json'
    closure_run = None
    if closure.is_file():
        evidence.append(closure)
        try:
            phase = json.loads(closure.read_bytes())
            if (phase.get('schema') != 'alopex.trace-closure-phase/v1' or
                    phase.get('case') != 'TRACE_CLOSURE_682' or
                    phase.get('approval') != 'unapproved' or
                    phase.get('formal_lock') is not False or
                    phase.get('status') not in ('candidate', 'mismatch', 'guard_failed') or
                    not re.fullmatch(r'hosted-804-[0-9]+-trace-lock',
                                     phase.get('run_id', '')) or
                    not re.fullmatch(r'[0-9a-f]{64}', phase.get('proof_sha256', ''))):
                raise ValueError('closure phase')
            closure_run = private / 'runs' / phase['run_id']
            if ((private / 'runs').is_symlink() or closure_run.is_symlink() or
                    (closure_run / 'collected').is_symlink()):
                raise ValueError('closure directory link')
            if phase['status'] in ('candidate', 'mismatch'):
                record = json.loads((closure_run / 'run.json').read_bytes())
                final = json.loads((closure_run / 'final-result.private.json').read_bytes())
                if (record.get('phase') != 'deleted' or
                        record.get('run_id') != phase['run_id'] or
                        final.get('schema') != 'alopex.trace-closure-final/v1' or
                        final.get('proof_sha256') != phase['proof_sha256'] or
                        final.get('status') != ('candidate' if phase['status'] == 'candidate'
                                                else 'failed') or
                        final.get('error_code') != (None if phase['status'] == 'candidate'
                                                     else 'CLOSURE_MISMATCH') or
                        not (closure_run / 'collection.json').is_file() or
                        not (closure_run / 'collected/result.json').is_file() or
                        (phase['status'] == 'candidate' and not
                         (closure_run / 'collected/candidate.private.json').is_file())):
                    raise ValueError('closure final')
            public['closure_outcome'] = ('candidate' if phase['status'] == 'candidate'
                                         else 'failed')
            public['closure_error_code'] = (None if phase['status'] == 'candidate' else
                                            'CLOSURE_MISMATCH' if phase['status'] == 'mismatch'
                                            else 'CLOSURE_GUARD')
        except (OSError, ValueError, TypeError, AttributeError):
            public['closure_outcome'] = 'failed'
            public['closure_error_code'] = 'CLOSURE_GUARD'
        for path in (closure_run,) if closure_run is not None else ():
            for name in ('run.json', 'collection.json', 'final-result.private.json',
                         'collected/result.json', 'collected/candidate.private.json'):
                item = path / name
                if item.is_file():
                    evidence.append(item)
    else:
        public['closure_outcome'] = 'failed'
        public['closure_error_code'] = 'CLOSURE_GUARD'
    public['closure_approval'] = 'unapproved'
    public['formal_lock'] = False
    if wrapper.is_file():
        evidence.append(wrapper)
        try:
            if json.loads(wrapper.read_bytes()).get('pre_comparator_error') is True:
                public['error_code'] = 'WRAPPER_FAILURE'
        except (OSError, ValueError, TypeError, AttributeError):
            public['error_code'] = 'WRAPPER_FAILURE'
    if log.is_file():
        evidence.append(log)
    if (cost.is_file() and encrypted_cost.is_file() and wrapper.is_file() and
            public['error_code'] != 'WRAPPER_FAILURE'):
        evidence.extend((cost, encrypted_cost))
        try:
            result = json.loads(wrapper.read_bytes())
            comparison = json.loads(cost.read_bytes())
            success = (result['original_error'] is None and result['cleanup_error'] is None and
                       result['comparator_exit'] == 0 and result['all_owned_children_waited'] and
                       comparison['status'] == 'passed' and comparison['complete_cells'] == 48 and
                       comparison['planned_cells'] == 48 and comparison['work_removed'] and
                       comparison['performance_acceptance'] is False and
                       comparison['encrypted_sha256'] == digest(encrypted_cost))
            public['outcome'] = 'passed' if success else 'failed'
            public['error_code'] = None if success else 'COST_FAILURE'
            public['cost_evidence_sha256'] = digest(encrypted_cost)
            public['wal_outcome'] = 'passed' if success else 'failed'
        except (OSError, ValueError, KeyError, TypeError):
            public['error_code'] = 'COST_FAILURE'
            public['wal_outcome'] = 'failed'
    if public['outcome'] == 'passed':
        public['outcome'] = 'failed'
        public['error_code'] = ('LOCK_UNAPPROVED' if
                                public['closure_outcome'] == 'candidate' else
                                public['closure_error_code'])
    archive = private / 'wrapper-evidence.tar.gz'
    encrypted = output / 'private-evidence.cms'
    if evidence:
        try:
            total = 0
            for path in evidence:
                size = path.stat().st_size
                if (path.is_symlink() or size > 32_000_000 or
                        (path == closure or any(part.endswith('-trace-lock') for part in path.parts)) and
                        size > CLOSURE_INPUT_LIMIT):
                    raise RuntimeError('evidence size or link violation')
                total += size
            if total > TOTAL_INPUT_LIMIT:
                raise RuntimeError('evidence total size violation')
            with tarfile.open(archive, 'w:gz') as bundle:
                for index, path in enumerate(evidence):
                    bundle.add(path, arcname=f'evidence/{index}-{path.name}', recursive=False)
            certificate = qualification / '.github/ops/native-evidence-cert.pem'
            command = ['openssl', 'cms', '-encrypt', '-aes-256-cbc', '-binary',
                       '-outform', 'DER', '-in', str(archive), '-out', str(encrypted), str(certificate)]
            subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                           timeout=60, check=True)
            public['wrapper_evidence_sha256'] = digest(encrypted)
        except Exception:
            public['outcome'] = 'failed'
            public['error_code'] = 'EVIDENCE_FAILURE'
            public.pop('wrapper_evidence_sha256', None)
            encrypted.unlink(missing_ok=True)
        finally:
            archive.unlink(missing_ok=True)
            log.unlink(missing_ok=True)
            wrapper.unlink(missing_ok=True)
            closure.unlink(missing_ok=True)
    (output / 'public.json').write_text(json.dumps(public, sort_keys=True) + '\n')
    return public


def main():
    parser = argparse.ArgumentParser()
    for key in ('qualification', 'frozen', 'private', 'output'):
        parser.add_argument('--' + key, required=True, type=Path)
    args = parser.parse_args()
    outcome = seal(args.qualification, args.frozen, args.private, args.output)
    print(json.dumps({'case': outcome['case'], 'outcome': outcome['outcome'],
                      'error_code': outcome['error_code']}))
    return 0 if outcome['outcome'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
