"""Independent, tiny trace oracle for the six ordinary-extension EIO cases."""
import re

CASES = tuple(f'{surface}-{stage}' for surface in ('kv', 'sql')
              for stage in ('spool', 'directory', 'reference_append'))


def events(text):
    active = False
    markers = [0, 0]
    counts = {'fsync': 0, 'fdatasync': 0}
    rows = []
    for line in text.splitlines():
        if line.startswith('write(2<') and '"ALOPEX_COMMIT_BEGIN\\n"' in line:
            assert not active
            active = True; markers[0] += 1
        elif line.startswith('write(2<') and '"ALOPEX_COMMIT_END\\n"' in line:
            assert active
            active = False; markers[1] += 1
        match = re.fullmatch(r'(fsync|fdatasync)\(\d+<([^>]+)>\)\s+= (.+)', line)
        if match:
            call, path, result = match.groups(); counts[call] += 1
            rows.append(dict(call=call, path=path, result=result, ordinal=counts[call], active=active))
    assert markers == [1, 1] and not active
    return rows


def verify(report, case, producer, sources, baseline, fault):
    assert case in CASES and report['status'] == 'passed'
    assert report['native_sha256'] == producer['native_sha256']
    assert report['source_sha256'] == {name: sources['scripts/compatibility/'+name]
        for name in ('sync_syscall.py', 'publication_python.py', 'spool_os_write.py')}
    assert len(report['cases']) == 1
    row = report['cases'][0]
    assert row['case'] == case and row['status'] == 'passed'
    assert type(row['fixture_bytes']) is int and 0 < row['fixture_bytes'] <= 1048576
    assert row['reopen_cycles'] == 2
    stage = case.split('-', 1)[1]
    assert row['replay'] == ('complete' if stage == 'reference_append' else 'prior-only')
    good = events(baseline); failed = events(fault)
    wal = [e for e in good if e['active'] and e['call']=='fdatasync' and e['path'].endswith('/lsm.wal')]
    assert len(wal) == 1 and wal[0]['result'] == '0'
    assert all(e['result'] == '0' for e in good)
    hits = [e for e in failed if 'INJECTED' in e['result']]
    assert len(hits) == 1
    hit = hits[0]
    assert hit['active'] and hit['result'].startswith('-1 EIO ')
    assert (row['syscall'], row['ordinal']) == (hit['call'], hit['ordinal'])
    suffix = hit['path'].split('fault.alopex.d/', 1)[1]
    assert row['target_suffix'] == suffix
    if stage == 'spool':
        assert hit['call']=='fsync' and suffix.startswith('overflow/') and suffix.endswith('.tmp')
    elif stage == 'directory':
        assert hit['call']=='fsync' and suffix=='overflow'
    else:
        assert hit['call']=='fdatasync' and suffix=='lsm.wal'
    target = [e for e in good if e['call']==hit['call'] and e['ordinal']==hit['ordinal']]
    assert len(target)==1 and target[0]['active']
    assert target[0]['path'].replace('baseline.alopex', 'fault.alopex', 1)==hit['path']
    return row
