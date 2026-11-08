"""Original four Lazy cases, fixed inputs and complete resource coverage."""
from dataframe_phase_contract import require, verify_cell as verify_base

SUBJECTS = ('published', 'before', 'candidate', 'polars')
CASES = ('csv-collect', 'csv-batches', 'parquet-numeric-collect', 'parquet-numeric-batches')


def plan(mode):
    require(mode in ('preflight', 'full'), 'unknown Lazy comparison mode')
    cells = []
    for trial in range(1 if mode == 'preflight' else 3):
        for offset, case in enumerate(CASES):
            shift = (trial + offset) % 4
            for subject in SUBJECTS[shift:] + SUBJECTS[:shift]:
                cells.append(dict(id=f'trial{trial}-{case}-{subject}', trial=trial, case=case,
                    rows=3 if mode == 'preflight' else 34250, subject=subject,
                    engine='polars' if subject == 'polars' else 'alopex',
                    warmups=0 if mode == 'preflight' else 3, samples=1 if mode == 'preflight' else 21))
    return cells


def verify_cell(cell, result, raw, observations, rss_limit):
    require(result['output_rows'] == cell['rows'], 'incomplete Lazy output')
    clean = []
    for row in raw:
        require(set(row) == {'phase','index','seconds','validated','output_rows','resources'}
            and row['validated'] is True and row['output_rows'] == cell['rows'], 'unvalidated Lazy sample')
        clean.append({k:v for k,v in row.items() if k not in ('validated','output_rows')})
    return verify_base(cell, result, clean, observations, rss_limit)
