"""Audit collected completion-stage evidence without executing product assets."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from audit_hnsw_writer_overhead import audit
from audit_native_sql_matrix import Evidence, require, sha
from hosted_hnsw_writer_overhead import validate_config

BASE = Path(__file__).resolve().parent


def decrypt_and_audit(root, config, controller):
    evidence = None
    with tempfile.TemporaryDirectory(prefix='overhead-audit-', dir=BASE) as directory:
        temporary = Path(directory)
        try:
            for name, suffix in (('private-evidence', 'tar.gz'), ('cleanup', 'json')):
                result = subprocess.run(
                    ['openssl', 'cms', '-decrypt', '-binary', '-inform', 'DER',
                     '-in', str(root / (name + '.cms')), '-inkey', str(BASE / 'native-evidence.key'),
                     '-out', str(temporary / (name + '.' + suffix))],
                    capture_output=True, timeout=30)
                require(result.returncode == 0, 'evidence decryption failed')
            evidence = Evidence(temporary / 'private-evidence.tar.gz')
            return audit(evidence, json.loads((root / 'result.json').read_bytes()),
                         json.loads((temporary / 'cleanup.json').read_bytes()), config, controller)
        finally:
            if evidence is not None:
                evidence.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=int, required=True)
    parser.add_argument('--controller', required=True)
    parser.add_argument('--configuration', type=Path, required=True)
    args = parser.parse_args()
    require(args.run > 0 and re.fullmatch('[0-9a-f]{40}', args.controller), 'invalid run identity')
    os.umask(0o077)
    raw_config = args.configuration.read_bytes()
    config = validate_config(json.loads(raw_config))
    result = subprocess.run(
        ['gh', 'api', f'repos/alopex-db/alopex/actions/runs/{args.run}'],
        capture_output=True, check=True, timeout=30)
    run = json.loads(result.stdout)
    require(run['id'] == args.run and run['head_sha'] == args.controller
            and run['status'] == 'completed' and run['conclusion'] == 'success'
            and run['event'] == 'workflow_dispatch'
            and run['head_repository']['full_name'] == 'alopex-db/alopex', 'run identity differs')
    root = BASE / f'hnsw-writer-overhead-run-{args.run}'
    proof = decrypt_and_audit(root, config, args.controller)
    proof.update(run=args.run, configuration_sha256=sha(raw_config),
                 auditor_sha256=sha(Path(__file__).read_bytes()),
                 contract_sha256=sha((BASE / 'hnsw_writer_overhead_contract.py').read_bytes()),
                 reconstruction_sha256=sha((BASE / 'audit_hnsw_writer_overhead.py').read_bytes()),
                 public_result_sha256=sha((root / 'result.json').read_bytes()),
                 encrypted_evidence_sha256=sha((root / 'private-evidence.cms').read_bytes()),
                 encrypted_cleanup_sha256=sha((root / 'cleanup.cms').read_bytes()),
                 plaintext_removed=True)
    output = BASE / f'hnsw-writer-overhead-{args.run}-proof.private.json'
    output.write_text(json.dumps(proof, indent=2) + '\n')
    output.chmod(0o600)
    print(json.dumps({key: proof[key] for key in
                     ('status', 'run', 'stage_rows', 'finish', 'completed_samples', 'plaintext_removed')}))


if __name__ == '__main__':
    main()
