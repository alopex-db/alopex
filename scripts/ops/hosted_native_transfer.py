"""Retrieve source-built product and environment artifacts with a read-only token."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys


def verify_run(run, expected_source):
    if not (run['head_sha'] == expected_source and run['status'] == 'completed' and
            run['conclusion'] == 'success' and run['event'] == 'workflow_dispatch' and
            run['head_repository']['full_name'] == 'alopex-db/alopex'):
        raise ValueError('unqualified artifact producer')


def main():
    if os.environ.get('RUNNER_KIND') != 'github-hosted' or os.geteuid() == 0:
        print('{"status":"failed","error_code":"VM_SCOPE"}')
        return 78
    os.umask(0o077)
    repo = Path(os.environ['GITHUB_WORKSPACE']).resolve()
    sys.path.insert(0, str(repo / 'scripts/test-env'))
    from capacity import Budget
    from common import atomic_json, digest, private_dir, require
    private = private_dir(repo / 'native-fetch-private')
    try:
        config = json.loads(base64.b64decode(os.environ['PUBLIC_SQL_CONFIG'], validate=True))
        roots = [repo.parent, Path(os.environ['RUNNER_TEMP'])]
        for root in (repo.parent.parent / '_actions', Path.home() / '.cargo', Path.home() / '.cache'):
            if root.exists(): roots.append(root)
        budget = Budget({'roots': list(map(str, roots)), 'stop_bytes': 40_000_000_000,
                         'reserve_bytes': config['download_reserve_bytes'], 'poll_seconds': 2,
                         'planned_bytes': {'fetch': config['download_bound_bytes']}})
        budget.before('fetch')
        private_dir(private / 'tmp')
        os.environ['TMPDIR'] = str(private / 'tmp')
        def metadata(endpoint):
            result = subprocess.run(['gh', 'api', endpoint], capture_output=True, text=True,
                                    check=True, timeout=30)
            return json.loads(result.stdout)
        api = 'repos/alopex-db/alopex/actions/runs/'
        for name, expected_source, number, directory in (
            ('native-test-environment', config['image_run_source'], config['image_run'], 'native-image-import'),
            ('native-product-assets', config['product_run_source'], config['product_run'], 'native-product-import'),
        ):
            verify_run(metadata(api + str(number)), expected_source)
            records = metadata(api + str(number) + '/artifacts')['artifacts']
            records = [item for item in records if item['name'] == name]
            require(len(records) == 1 and not records[0]['expired'] and
                    records[0]['size_in_bytes'] < config['artifact_bound_bytes'], 'IDENTITY')
            budget.run(['gh', 'run', 'download', str(number), '--repo', 'alopex-db/alopex',
                        '--name', name, '--dir', str(repo / directory)], timeout=300,
                       log=private / (directory + '.log'))
        image = repo / config['image']['image_archive']
        require(digest(image) == config['image']['image_archive_sha256'], 'IDENTITY')
        public = json.loads((image.parent / 'result.json').read_text())
        require(public['status'] == 'passed' and public['owned_resources_cleaned'] is True and
                public['controller_commit'] == config['image_run_source'], 'IDENTITY')
        from hosted_native_sql import verify_products
        product = repo / 'native-product-import'
        verify_products(product / 'assets/assets.json', product / 'result.json', config)
        budget.check()
        print('{"status":"passed","error_code":null}')
        return 0
    except BaseException as error:
        try:
            atomic_json(private / 'failure.json', {'error': repr(error)})
            export = private_dir(repo / 'native-export')
            subprocess.run(['openssl', 'cms', '-encrypt', '-aes-256-cbc', '-binary', '-outform', 'DER',
                            '-in', str(private / 'failure.json'), '-out', str(export / 'transfer-failure.cms'),
                            str(repo / '.github/ops/native-evidence-cert.pem')], capture_output=True, check=True, timeout=30)
        except BaseException:
            pass
        print('{"status":"failed","error_code":"TRANSFER"}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
