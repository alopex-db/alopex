"""Seal the bounded one-pair diagnostic wrapper receipt without raw public data."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile


MAX_PRIVATE_BYTES = 16_000_000


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def seal(qualification, private, diagnostic_private, diagnostic_export, output):
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    public = dict(schema='alopex.wal-diagnostic-wrapper/v1', case='WAL_COST_682',
                  outcome='failed', error_code='INCOMPLETE', performance_acceptance=False)
    receipt = private / 'diagnostic-wrapper-result.private.json'
    log = private / 'diagnostic.stdout.private.log'
    result = diagnostic_export / 'result.json'
    encrypted_result = diagnostic_export / 'private-evidence.cms'
    failure = diagnostic_private / 'diagnostic-failure.private.json'
    evidence = [path for path in (receipt, log, result, encrypted_result, failure)
                if path.is_file() or path.is_symlink()]
    try:
        if any(path.is_symlink() for path in evidence):
            raise ValueError('linked evidence')
        if receipt.is_file():
            wrapper = json.loads(receipt.read_bytes())
            if wrapper.get('pre_diagnostic_error'):
                public['error_code'] = 'WRAPPER_FAILURE'
            elif result.is_file() and encrypted_result.is_file():
                diagnostic = json.loads(result.read_bytes())
                success = (wrapper.get('original_error') is None and
                           wrapper.get('cleanup_error') is None and
                           wrapper.get('diagnostic_exit') == 0 and
                           wrapper.get('all_owned_children_waited') is True and
                           diagnostic.get('schema') == 'alopex.wal-diagnostic/v1' and
                           diagnostic.get('case') == 'WAL_COST_682' and
                           diagnostic.get('outcome') == 'observed' and
                           diagnostic.get('error_code') is None and
                           diagnostic.get('complete_subjects') == 2 and
                           diagnostic.get('performance_acceptance') is False and
                           diagnostic.get('encrypted_sha256') == digest(encrypted_result) and
                           not failure.exists() and not failure.is_symlink() and
                           not (diagnostic_private / 'diagnostic').exists() and
                           not (diagnostic_private / 'diagnostic').is_symlink())
                public['outcome'] = 'observed' if success else 'failed'
                public['error_code'] = None if success else 'DIAGNOSTIC_FAILURE'
                public['diagnostic_evidence_sha256'] = digest(encrypted_result)
            else:
                public['error_code'] = 'DIAGNOSTIC_FAILURE'
    except (OSError, ValueError, TypeError, AttributeError):
        public['outcome'] = 'failed'
        public['error_code'] = 'DIAGNOSTIC_FAILURE'

    archive = private / 'diagnostic-wrapper-evidence.tar.gz'
    encrypted = output / 'private-evidence.cms'
    if evidence:
        try:
            if any(path.is_symlink() or path.stat().st_size > MAX_PRIVATE_BYTES
                   for path in evidence):
                raise RuntimeError('private evidence differs')
            if sum(path.stat().st_size for path in evidence) > MAX_PRIVATE_BYTES:
                raise RuntimeError('private evidence exceeds bound')
            with tarfile.open(archive, 'w:gz') as bundle:
                for index, path in enumerate(evidence):
                    bundle.add(path, arcname=f'evidence/{index}-{path.name}', recursive=False)
            certificate = qualification / '.github/ops/native-evidence-cert.pem'
            subprocess.run(['openssl', 'cms', '-encrypt', '-aes-256-cbc', '-binary',
                            '-outform', 'DER', '-in', str(archive), '-out', str(encrypted),
                            str(certificate)], stdin=subprocess.DEVNULL, capture_output=True,
                           timeout=60, check=True)
            if (not encrypted.is_file() or encrypted.is_symlink() or
                    not 0 < encrypted.stat().st_size <= MAX_PRIVATE_BYTES):
                raise RuntimeError('encrypted evidence differs')
            public['wrapper_evidence_sha256'] = digest(encrypted)
        except Exception:
            public['outcome'] = 'failed'
            public['error_code'] = 'EVIDENCE_FAILURE'
            public.pop('wrapper_evidence_sha256', None)
            encrypted.unlink(missing_ok=True)
        finally:
            archive.unlink(missing_ok=True)
            receipt.unlink(missing_ok=True)
            log.unlink(missing_ok=True)
            failure.unlink(missing_ok=True)
    (output / 'public.json').write_text(json.dumps(public, sort_keys=True) + '\n')
    return public


def main():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    for name in ('qualification', 'private', 'diagnostic-private',
                 'diagnostic-export', 'output'):
        parser.add_argument('--' + name, required=True, type=Path)
    args = parser.parse_args()
    public = seal(args.qualification, args.private, args.diagnostic_private,
                  args.diagnostic_export, args.output)
    print(json.dumps(dict(case=public['case'], outcome=public['outcome'],
                          error_code=public['error_code'])))
    return 0 if public['outcome'] == 'observed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
