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
    require(record['initial_paused'] is (mode!='baseline'),'startup pause not verified')
    require(record['calls']==expected,'sampling windows differ')
    require(record['product_worker_modified'] is False and record['query_loop_modified'] is False
            and record['performance_acceptance'] is False,'sampling scope differs')


def collector_command(tool,experiment,command):
    # gprofng 2.42 resolves signal names; numeric arguments can disable this option.
    return [str(tool),'collect','app','-p','10','-y','SIGUSR2','-F','off','-a','off',
            '-S','off','-j','off','-s','off','-H','off','-i','off','-o',str(experiment),*command]


def verify_profile(mode,stage,log,profile_bytes,report):
    import re
    require(mode in ('paused','sampled') and stage in ('preflight','full'),'profile scope')
    require('kind="cerror"' not in log,'collector reported error')
    require(log.count('kind="resume"')==(2 if mode=='sampled' else 0),'unexpected sampling interval')
    require(type(profile_bytes) is int and profile_bytes>=0,'profile size')
    if profile_bytes==0:
        require(report is None,'report without samples')
        require(mode=='paused' or stage=='preflight','no full-loop samples')
        return dict(sample_status='disabled' if mode=='paused' else 'no_sample_at_minimal_duration',observed_cpu_seconds=None)
    require(mode=='sampled' and type(report) is str,'samples outside selected loop')
    values=re.findall(r'^\s*<Total>\s+([0-9]+(?:\.[0-9]*)?)\s*$',report,re.M)
    require(len(values)==1 and float(values[0])>0,'sampling total missing')
    return dict(sample_status='observed',observed_cpu_seconds=float(values[0]))
