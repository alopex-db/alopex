"""Encrypt bounded wrapper evidence; publish only fixed outcomes and hashes."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile


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
    if wrapper.is_file():
        evidence.append(wrapper)
    if log.is_file():
        evidence.append(log)
    if cost.is_file() and encrypted_cost.is_file() and wrapper.is_file():
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
        except (OSError, ValueError, KeyError, TypeError):
            public['error_code'] = 'COST_FAILURE'
    archive = private / 'wrapper-evidence.tar.gz'
    encrypted = output / 'private-evidence.cms'
    if evidence:
        try:
            for path in evidence:
                if path.is_symlink() or path.stat().st_size > 32_000_000:
                    raise RuntimeError('evidence size or link violation')
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
