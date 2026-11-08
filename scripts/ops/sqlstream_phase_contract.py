"""Fixed SQL streaming lanes and full-consumption evidence contracts."""
from dataframe_phase_contract import require, number, verify_cell as verify_base
SUBJECTS = ('published', 'before', 'candidate', 'datafusion')
CASES = ('original', 'rows')


def plan(mode):
    require(mode in ('preflight', 'full'), 'unknown SQL stream mode')
    cells = []
    for trial in range(1 if mode == 'preflight' else 3):
        for offset, case in enumerate(CASES):
            shift = (trial + offset) % 4
            for subject in SUBJECTS[shift:] + SUBJECTS[:shift]:
                cells.append(dict(id=f'trial{trial}-{case}-{subject}', trial=trial, case=case,
                    rows=3 if mode == 'preflight' else 34250, subject=subject,
                    engine='datafusion' if subject == 'datafusion' else 'alopex',
                    warmups=0 if mode == 'preflight' else 3, samples=1 if mode == 'preflight' else 21))
    return cells


def verify_cell(cell, result, raw, observations, rss_limit):
    expected = (cell['rows'] + 1) // 2
    require(result['connection_closed'] is True and result['fixture_checks'] == 2
        and result['full_result_checks'] == 2 + (cell['warmups'] + cell['samples'] if cell['case'] == 'rows' else 0),
        'SQL stream result/cleanup coverage missing')
    require(result['returned_rows'] == expected and result['rows_lane'] is (cell['case'] == 'rows'),
        'SQL stream lane differs')
    if cell['engine'] == 'datafusion':
        require(result['versions'] == {'datafusion':'50.0.0', 'pyarrow':'26.0.0'}
            and result['target_partitions'] == 1, 'reference settings differ')
    clean = []
    for row in raw:
        require(row['rows'] == expected and type(row['first_chunk_rows']) is int
            and 0 < row['first_chunk_rows'] <= expected, 'incomplete SQL stream')
        require(number(row['first_seconds']) and number(row['seconds'])
            and 0 <= row['first_seconds'] <= row['seconds'], 'invalid first/total interval')
        require(row['remaining_seconds'] == row['seconds'] - row['first_seconds'], 'remaining interval differs')
        require(row['rows_verified'] is (cell['case'] == 'rows'), 'row validation differs')
        if cell['engine'] == 'alopex':require(row['first_chunk_rows'] == 1, 'Alopex row boundary differs')
        clean.append({k:row[k] for k in ('phase', 'index', 'seconds', 'resources')})
    checked = verify_base(cell, result, clean, observations, rss_limit)
    checked['first_seconds'] = [r['first_seconds'] for r in raw]
    return checked


def validate_fixture(actual, rows):
    from sql_delete_profile import digest
    require(type(actual) is list and len(actual) == rows, 'fixture row count differs')
    for index, row in enumerate(actual):
        require(type(row) is dict and list(row) == ['id','value','enabled'], 'fixture columns differ')
        require(type(row['id']) is int and type(row['value']) is float and type(row['enabled']) is bool
            and row == dict(id=index,value=float(index),enabled=index%2==0), 'fixture typed value differs')
    return digest([(i,float(i),i%2==0) for i in range(rows)])
