"""Fixed original and checked SQLite composite lanes, without conflating them."""
from dataframe_phase_contract import require,verify_cell as verify_base
SUBJECTS=('published','before','candidate','sqlite')
CASES=('original','checked')
def plan(mode):
    require(mode in ('preflight','full'),'unknown SQLite mode')
    cells=[]
    for trial in range(1 if mode=='preflight' else 3):
        for offset,case in enumerate(CASES):
            shift=(trial+offset)%4
            for subject in SUBJECTS[shift:]+SUBJECTS[:shift]:
                cells.append(dict(id=f'trial{trial}-{case}-{subject}',trial=trial,case=case,
                    rows=3 if mode=='preflight' else 34250,subject=subject,
                    engine='sqlite' if subject=='sqlite' else 'alopex',
                    warmups=0 if mode=='preflight' else 3,samples=1 if mode=='preflight' else 21))
    return cells

def verify_cell(cell,result,raw,observations,rss_limit):
    require(result['connection_closed'] is True and result['state_checks']==cell['warmups']+cell['samples'],
        'SQL rollback/cleanup coverage missing')
    require(result['checked_contracts'] is (cell['case']=='checked'),'SQL lane differs')
    if cell['engine']=='sqlite':
        require(result['sqlite_version']=='3.46.1','SQLite version differs')
        require(result['foreign_keys']==(1 if cell['case']=='checked' else 0),'SQLite FK boundary differs')
    clean=[]
    for row in raw:
        require(row['state_verified'] is True and row['result_verified'] is (cell['case']=='checked'),
            'SQL validation missing')
        clean.append({k:v for k,v in row.items() if k not in ('state_verified','result_verified','original_usage')})
    return verify_base(cell,result,clean,observations,rss_limit)
