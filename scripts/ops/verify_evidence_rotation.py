"""Private-host cutover verification; never issue/install keys or export plaintext."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile

from check_evidence_certificate import check

LIMIT = 512_000_000


def require(condition, code):
    if not condition:
        raise ValueError(code)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def regular(path, limit, *, private=False):
    path = Path(path)
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= limit, 'INPUT_INVALID')
    if private:
        require(info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o600,
                'KEY_MODE_INVALID')
    return path


def run(args, *, output=None, must_succeed=True):
    result = subprocess.run(['openssl', *map(str, args)], stdin=subprocess.DEVNULL,
                            stdout=output if output is not None else subprocess.PIPE,
                            stderr=subprocess.DEVNULL, timeout=120, check=False)
    if must_succeed:
        require(result.returncode == 0, 'CRYPTO_CHECK_FAILED')
    return result


def public_key(certificate, key):
    regular(certificate, 16384)
    regular(key, 32768, private=True)
    cert_public = run(['x509', '-in', certificate, '-pubkey', '-noout']).stdout
    key_public = run(['pkey', '-in', key, '-passin', 'pass:', '-pubout']).stdout
    require(cert_public == key_public, 'KEY_PAIR_MISMATCH')
    return cert_public


def private_temp_root(path, old_key, new_key):
    root = Path(path)
    try:
        canonical = root.resolve(strict=True)
        info = root.lstat()
    except OSError as error:
        raise ValueError('TEMP_ROOT_INVALID') from error
    require(root.is_absolute() and canonical == root, 'TEMP_ROOT_INVALID')
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700
            and info.st_dev == Path(old_key).stat().st_dev
            and info.st_dev == Path(new_key).stat().st_dev, 'TEMP_ROOT_INVALID')
    return root


def verify(*, old_certificate, old_key, new_certificate, new_key, archived_cms,
           old_certificate_sha256, new_certificate_sha256, ciphertext_sha256,
           plaintext_sha256, private_temp_root_path):
    for value in (old_certificate_sha256, new_certificate_sha256, ciphertext_sha256, plaintext_sha256):
        require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value), 'DIGEST_INVALID')
    for path, expected in ((old_certificate, old_certificate_sha256),
                           (new_certificate, new_certificate_sha256)):
        regular(path, 16384)
        require(digest(path) == expected, 'CERT_MISMATCH')
    # Old validity is intentionally not checked: historical CMS decryption is allowed.
    new_identity = check(new_certificate, new_certificate_sha256, minimum_seconds=14 * 86400)
    old_public = public_key(old_certificate, old_key)
    new_public = public_key(new_certificate, new_key)
    require(old_public != new_public, 'KEY_REUSE_REJECTED')
    regular(archived_cms, LIMIT)
    require(digest(archived_cms) == ciphertext_sha256, 'ARCHIVE_HASH_MISMATCH')
    temporary_parent = private_temp_root(private_temp_root_path, old_key, new_key)
    previous = os.umask(0o077)
    try:
        with tempfile.TemporaryDirectory(prefix='alopex-rotation-', dir=temporary_parent) as temporary:
            root = Path(temporary)
            recovered = root / 'old-receipt'
            with recovered.open('xb') as output:
                run(['cms', '-decrypt', '-binary', '-inform', 'DER', '-in', archived_cms,
                     '-recip', old_certificate, '-inkey', old_key, '-passin', 'pass:'], output=output)
            require(recovered.stat().st_size <= LIMIT and digest(recovered) == plaintext_sha256,
                    'ARCHIVE_PLAINTEXT_MISMATCH')
            with (root / 'wrong-key-output').open('xb') as output:
                wrong = run(['cms', '-decrypt', '-binary', '-inform', 'DER', '-in', archived_cms,
                             '-recip', new_certificate, '-inkey', new_key, '-passin', 'pass:'],
                            output=output, must_succeed=False)
            require(wrong.returncode != 0, 'OLD_ARCHIVE_ACCEPTED_NEW_KEY')
            synthetic = b'alopex evidence recipient rotation synthetic receipt v1\n'
            (root / 'synthetic').write_bytes(synthetic)
            run(['cms', '-encrypt', '-binary', '-aes-256-cbc', '-outform', 'DER',
                 '-in', root / 'synthetic', '-out', root / 'new.cms', new_certificate])
            actual = run(['cms', '-decrypt', '-binary', '-inform', 'DER', '-in', root / 'new.cms',
                          '-recip', new_certificate, '-inkey', new_key, '-passin', 'pass:']).stdout
            require(actual == synthetic, 'NEW_ROUNDTRIP_MISMATCH')
    finally:
        os.umask(previous)
    return {'outcome': 'passed', 'old_certificate_sha256': old_certificate_sha256,
            'new_certificate_sha256': new_certificate_sha256,
            'new_not_after': new_identity['not_after'], 'historical_decryption': 'passed',
            'new_roundtrip': 'passed', 'wrong_key_rejected': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('old-certificate', 'old-key', 'new-certificate', 'new-key', 'archived-cms',
                 'old-certificate-sha256', 'new-certificate-sha256', 'ciphertext-sha256',
                 'plaintext-sha256', 'private-temp-root-path'):
        parser.add_argument('--'+name, required=True)
    try:
        result = verify(**vars(parser.parse_args()))
    except Exception as error:
        codes = {'INPUT_INVALID', 'KEY_MODE_INVALID', 'CRYPTO_CHECK_FAILED', 'KEY_PAIR_MISMATCH',
                 'DIGEST_INVALID', 'CERT_MISMATCH', 'KEY_REUSE_REJECTED', 'ARCHIVE_HASH_MISMATCH',
                 'ARCHIVE_PLAINTEXT_MISMATCH', 'OLD_ARCHIVE_ACCEPTED_NEW_KEY',
                 'NEW_ROUNDTRIP_MISMATCH', 'CERT_INVALID', 'CERT_NOT_YET_VALID',
                 'CERT_EXPIRED', 'CERT_EXPIRING', 'TEMP_ROOT_INVALID'}
        code = str(error) if isinstance(error, ValueError) and str(error) in codes else 'ROTATION_CHECK_FAILED'
        print(json.dumps({'outcome': 'failed', 'error_code': code}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
