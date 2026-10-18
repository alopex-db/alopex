"""Keep Cargo compilation, product assertions, and verification resources distinct."""
import json
import re


def cargo_result(log, exit_code, kind):
    build = 'incomplete'
    counts = None
    for line in log.splitlines():
        if line.startswith('{'):
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if entry.get('reason') == 'build-finished':
                build = 'passed' if entry['success'] else 'failed'
        else:
            match = re.match(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed;', line)
            if match:
                counts = {'passed': int(match[1]), 'failed': int(match[2])}
    tests = 'not_run'
    if build == 'passed':
        if kind == 'lint':
            tests = 'not_applicable'
        elif counts is not None and sum(counts.values()) != 0:
            tests = 'failed' if counts['failed'] else 'passed'
        else:
            tests = 'incomplete'
    if build != 'passed':
        outcome = 'build_failed' if build == 'failed' else 'invalid'
    elif tests == 'failed':
        outcome = 'product_test_failed'
    elif exit_code == 0 and tests in ('passed', 'not_applicable'):
        outcome = 'passed'
    else:
        outcome = 'invalid'
    return {'build_status': build, 'product_test_status': tests,
            'product_test_counts': counts, 'outcome': outcome}


if __name__ == '__main__':
    built = '{"reason":"build-finished","success":true}\n'
    failed = '{"reason":"build-finished","success":false}\n'
    assert cargo_result(failed, 101, 'test')['product_test_status'] == 'not_run'
    assert cargo_result('', -15, 'test')['outcome'] == 'invalid'
    assert cargo_result(built, 0, 'test')['outcome'] == 'invalid'
    assert cargo_result(built, 0, 'lint')['outcome'] == 'passed'
    assert cargo_result(built + 'test result: FAILED. 1 passed; 3 failed;', 101,
                        'test')['outcome'] == 'product_test_failed'
    assert cargo_result(built + 'test result: ok. 4 passed; 0 failed;', 0,
                        'test')['outcome'] == 'passed'
    print('6 verification-boundary checks passed')
