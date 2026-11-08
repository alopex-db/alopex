"""Probe matrix and receipts for the unmodified continuous search loop."""
import json
from hnsw_search_contract import plan as normal_plan
from hnsw_search_sampling import MODES,require


def plan(mode):
    cells=[]
    for trial in range(1 if mode=='preflight' else 3):
        states=MODES[trial:]+MODES[:trial]
        for state in states:
            selected=[c for c in normal_plan(mode) if c['trial']==trial and c['subject'] in ('before','candidate')]
            for cell in selected:
                cells.append(dict(id=cell['id']+'-'+state,sampling_mode=state,normal=cell))
    require(mode in ('preflight','full'),'unknown sampling plan')
    return cells


def verify_sampling(record,cell,collector_sha):
    mode=cell['sampling_mode']
    require(record['mode']==mode and record['status']=='passed','sampling completion')
    require(record['collector_sha256']==(None if mode=='baseline' else collector_sha),'sampling collector identity')
    expected=[dict(ef_search=ef,sampling=(mode=='sampled' and ef in (128,256)),completed=True)
              for ef in cell['normal']['ef_values']]
    require(record['calls']==expected,'sampling windows differ')
    require(record['product_worker_modified'] is False and record['query_loop_modified'] is False
            and record['performance_acceptance'] is False,'sampling scope differs')


def collector_command(tool,experiment,command):
    import signal
    return [str(tool),'collect','app','-p','10','-y',str(signal.SIGUSR2),'-F','off','-a','off',
            '-S','off','-j','off','-s','off','-H','off','-i','off','-o',str(experiment),*command]
