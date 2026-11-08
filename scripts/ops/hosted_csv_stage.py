"""Serial, bounded plain-CSV release comparison, invoked manually only."""
import base64
import csv
import json
import os
from pathlib import Path
import runpy
import shutil
import signal
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/'scripts/test-env'), str(ROOT/'scripts/performance')]
from capacity import Budget
from common import atomic_json
from hosted_dataframe_comparison import install_product, install_reference, require, sha
from wal_cost_supervisor import run_cell
from csv_stage_contract import plan, verify_cell


def main():
    require(sys.platform == 'linux' and os.getuid() != 0 and not sys.flags.optimize
        and os.environ.get('RUNNER_KIND') == 'github-hosted'
        and os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
        and os.environ.get('ALOPEX_MANUAL_CSV') == '1', 'unexpected CSV execution boundary')
    os.umask(0o077)
    config = json.loads(base64.b64decode(os.environ['CSV_CONFIG'], validate=True))
    cells = plan(config['mode'])
    require(config['cpu_selection'] == 'first_allowed' and config['rss_bytes'] > 0, 'invalid CSV profile')
    allowed = sorted(os.sched_getaffinity(0))
    require(len(allowed) >= 2, 'CPU assignment unavailable')
    allocation = dict(client=allowed[:1], server=allowed[1:2])
    work, export = ROOT/'csv-stage-diagnosis-private', ROOT/'csv-stage-diagnosis-export'
    roots = [ROOT.parent, Path(os.environ['RUNNER_TEMP'])]
    roots.extend(p for p in (ROOT.parent.parent/'_actions', Path.home()/'.cargo', Path.home()/'.cache') if p.exists())
    budget = Budget(dict(roots=list(map(str, roots)), stop_bytes=40_000_000_000, reserve_bytes=256_000_000,
        poll_seconds=2, planned_bytes=dict(prepare=2_000_000_000, measure=128_000_000, collect=64_000_000)))
    initial = budget.before('prepare')
    work.mkdir();export.mkdir()
    for name in ('imports', 'venvs', 'identities', 'cells', 'fixtures', 'tmp'):
        (work/name).mkdir()
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PIP_DISABLE_PIP_VERSION_CHECK='1',
        PIP_NO_CACHE_DIR='1', TMPDIR=str(work/'tmp'))
    for key in ('GH_TOKEN', 'GITHUB_TOKEN', 'CSV_CONFIG', 'PYTHONPATH', 'PYTHONHOME', 'LD_LIBRARY_PATH',
        'ALOPEX_PERF_SUBJECT_MANIFEST', 'ALOPEX_PERF_SUBJECT_SHA256', 'ALOPEX_PERF_SERVER_URL',
        'NIM_SQL_PARSER_LIB_DIR', 'ALOPEX_NIM_PARSER_ALLOW_LOCAL_BUILD'):
        env.pop(key, None)
    for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                'POLARS_MAX_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'):
        env[key] = '1'
    deadline = time.monotonic() + config['stage_seconds']
    signal.alarm(config['stage_seconds'])
    rows, status, failure = [], 'failed', None
    try:
        for name, expected in config['sources'].items():
            require(sha(ROOT/name) == expected, 'CSV source differs')
        test_env = dict(env, PYTHONPATH=os.pathsep.join(str(ROOT/p) for p in
            ('scripts/ops', 'scripts/performance', 'scripts/performance/tests', 'scripts/test-env')))
        budget.run([sys.executable, '-B', '-m', 'unittest', 'test_csv_phase_measure',
            'test_csv_phase_contract', 'test_csv_stream_profile', 'test_csv_stage_measure', '-v'], timeout=60, log=work/'contracts.log', env=test_env)
        products = {name:install_product(name, config['products'][name], work, budget, env)
                    for name in ('before', 'candidate')}
        for name, product in products.items():
            subject = dict(schema='alopex.performance-subject/v1', version=product['receipt']['version'],
                source=product['receipt']['source'], cpu=allocation,
                python_wheel=dict(path=str(product['files']['wheel']), sha256=sha(product['files']['wheel'])),
                build_receipt=dict(path=str(product['files']['receipt']), sha256=sha(product['files']['receipt'])))
            path = work/'identities'/name/'subject.json'
            atomic_json(path, subject);product['subject'] = path
        # Use the pinned original fixture writer, not a replacement generator.
        write_csv = runpy.run_path(str(ROOT/'scripts/performance/workloads/csv_streaming_v0816.py'),
                                  init_globals=dict(Path=Path, csv=csv))['_write_csv']
        fixtures = {}
        for size in sorted({c['rows'] for c in cells}):
            path = work/'fixtures'/f'rows-{size}.csv'
            write_csv(path, size);path.chmod(0o400)
            require(path.stat().st_size == config['fixtures'][str(size)]['bytes']
                and sha(path) == config['fixtures'][str(size)]['sha256'], 'CSV input differs')
            fixtures[size] = path
        atomic_json(work/'identity.json', dict(controller=os.environ['GITHUB_SHA'], configuration=config,
            allocation=allocation, python_version=sys.version, cells=cells))
        budget.before('measure')
        oracles = {}
        for cell in cells:
            require(deadline-time.monotonic() > config['cell_seconds']+60, 'insufficient CSV stage time')
            directory = work/'cells'/cell['id'];directory.mkdir()
            parameters = dict(cell, root=str(ROOT), sources=config['sources'], output=str(directory),
                fixture=str(fixtures[cell['rows']]), input_sha256=sha(fixtures[cell['rows']]),
                affinity=allocation['client'], rss_bytes=config['rss_bytes'])
            child_env = dict(env, PYTHONPATH=str(ROOT/'scripts/performance'))
            if cell['subject'] == 'polars':
                python = reference_python
                parameters.update(reference_identity=str(reference_identity), reference_identity_sha256=sha(reference_identity))
            else:
                product = products[cell['subject']];python = product['python'];manifest = product['subject']
                parameters['subject_manifest_sha256'] = sha(manifest)
                child_env.update(ALOPEX_PERF_SUBJECT_MANIFEST=str(manifest), ALOPEX_PERF_SUBJECT_SHA256=sha(manifest),
                    LD_LIBRARY_PATH=str(product['files']['library'].parent))
            path = directory/'configuration.json';atomic_json(path, parameters)
            run_cell(['taskset', '--cpu-list', ','.join(map(str, allocation['client'])), str(python), '-B',
                str(ROOT/'scripts/ops/csv_stage_worker.py'), '--configuration', str(path)],
                child_env, directory, budget, config['cell_seconds'], config['rss_bytes'])
            result = json.loads((directory/'measurement.json').read_bytes())
            require(result['worker_sha256'] == sha(ROOT/'scripts/ops/csv_stage_worker.py')
                and result['raw_sha256'] == sha(directory/'raw.ndjson'), 'CSV worker/raw identity differs')
            raw = [json.loads(line) for line in (directory/'raw.ndjson').read_text().splitlines()]
            observed = json.loads((directory/'observations.json').read_bytes())
            checked = verify_cell(cell, result, raw, observed, config['rss_bytes'])
            pair = (checked['input_sha256'], checked['result_sha256'])
            require(oracles.setdefault(cell['rows'], pair) == pair, 'CSV cross-subject oracle differs')
            rows.append(dict(cell=cell, checked=checked, status='passed'));atomic_json(work/'cells.json', rows)
        status = 'passed'
    except BaseException as error:
        failure = type(error).__name__;atomic_json(work/'failure.json', dict(error=repr(error)))
    finally:
        signal.alarm(0)
        for name in ('imports', 'venvs', 'reference-wheels', 'fixtures', 'tmp'):
            path = work/name
            if path.exists():shutil.rmtree(path)
        atomic_json(work/'cleanup.json', dict(imports_venvs_fixtures_removed=True,
            initial_allocated_bytes=initial, final_allocated_bytes=budget.check()))
        budget.before('collect')
        members = [p for p in work.rglob('*') if p.is_file()]
        require(sum(p.stat().st_size for p in members) < 32_000_000
            and all(not p.is_symlink() for p in members), 'CSV evidence bound exceeded')
        archive = work/'evidence.tar.gz'
        with tarfile.open(archive, 'w:gz') as output:
            for path in members:output.add(path, arcname=path.relative_to(work).as_posix())
        subprocess.run(['openssl', 'cms', '-encrypt', '-aes-256-cbc', '-binary', '-outform', 'DER',
            '-in', str(archive), '-out', str(export/'private-evidence.cms'),
            str(ROOT/'.github/ops/native-evidence-cert.pem')], capture_output=True, check=True, timeout=30)
        evidence_hash = sha(archive);shutil.rmtree(work);budget.check()
        atomic_json(export/'result.json', dict(schema='alopex.csv-stage-diagnosis/v1', status=status, error_type=failure,
            controller=os.environ['GITHUB_SHA'], sources=config['sources'], mode=config['mode'],
            complete_cells=len(rows), planned_cells=len(cells), evidence_sha256=evidence_hash,
            encrypted_sha256=sha(export/'private-evidence.cms'), work_removed=not work.exists(), performance_acceptance=False))
    return 0 if status == 'passed' else 1


if __name__ == '__main__':
    def interrupted(signum, frame):raise InterruptedError('CSV comparison interrupted')
    signal.signal(signal.SIGTERM, interrupted);signal.signal(signal.SIGALRM, interrupted)
    try:code = main()
    except BaseException:code = 1
    print(json.dumps(dict(status='passed' if code == 0 else 'failed')))
    raise SystemExit(code)
