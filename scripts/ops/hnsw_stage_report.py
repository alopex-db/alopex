"""Publish workload results only; keep machine and execution-profile data private."""
import argparse
import json
from pathlib import Path

from hnsw_stage_contract import PHASES, TIMED_PHASES, require


def report(proof):
    require(proof['status'] == 'verified' and proof['completed_samples'] == 10
            and proof['cold_cache_contracts_verified'] and proof['cleanup_verified'], 'stage not audited')
    out = {key:proof[key] for key in ('run','stage_rows','controller','assets','public_result_sha256')}
    out.update(schema='alopex.hnsw-stage-report/v1',dimensions=384,samples_per_subject=5,
               performance_qualification=False,comparisons={})
    fields = ('before','candidate','baseline_median','candidate_median','baseline_mad','candidate_mad',
              'noise','difference','ratio','improvement_at_least_ten_percent','increase_exceeds_noise')
    for metric, rows in proof['comparisons'].items():
        require(metric in ('elapsed_ns','kernel_peak_rss_bytes'), 'unknown metric')
        require(set(rows) == set(TIMED_PHASES if metric == 'elapsed_ns' else PHASES), 'phase inventory differs')
        out['comparisons'][metric] = {}
        for phase, values in rows.items():
            clean = {key:values[key] for key in fields}
            require(len(clean['before']) == len(clean['candidate']) == 5, 'sample count differs')
            clean.update(before_observed_max=max(clean['before']),candidate_observed_max=max(clean['candidate']),
                         before_observed_min=min(clean['before']),candidate_observed_min=min(clean['candidate']))
            out['comparisons'][metric][phase] = clean
    return out


def markdown(data):
    lines = [f"# HNSW段階比較: {data['stage_rows']:,}行 × 384次元", '',
             f"実行: https://github.com/alopex-db/alopex/actions/runs/{data['run']}",
             f"制御commit: `{data['controller']}`", '',
             '修正前・候補各5標本。全標本を含め、中央値とMADを再計算した。',
             '区間RSSはLinux VmHWMのkernel報告値で、プロセス全体の区間最高値。allocation量ではない。',
             '最大値は5標本で実際に観測した最大値であり、将来の上界を保証しない。', '',
             'SQL commit・rollbackの時間と、INSERT内部のfork/staging別時間はこの測定で分離していない。',
             '本段階の結果だけでリリース性能受入の完了とはしない。', '']
    for metric, unit, scale in (('elapsed_ns','ms',1e6),('kernel_peak_rss_bytes','MB',1e6)):
        lines.extend([f'## {metric} ({unit})','',
                      '| phase | 修正前中央値 | 候補中央値 | 修正前MAD | 候補MAD | 修正前最大 | 候補最大 | 増減 |',
                      '| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |'])
        for phase,v in data['comparisons'][metric].items():
            keys=('baseline_median','candidate_median','baseline_mad','candidate_mad','before_observed_max','candidate_observed_max')
            lines.append('| '+phase+' | '+' | '.join(f'{v[k]/scale:.6f}' for k in keys)+f" | {(v['ratio']-1)*100:.2f}% |")
        lines.extend(['','### 全標本','',f'| phase | 修正前5標本 ({unit}) | 候補5標本 ({unit}) |','| --- | --- | --- |'])
        for phase,v in data['comparisons'][metric].items():
            lines.append('| '+phase+' | '+' | '.join(', '.join(f'{x/scale:.6f}' for x in v[k]) for k in ('before','candidate'))+' |')
        lines.append('')
    return '\n'.join(lines)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows',type=int,choices=(1000,10000,100000),required=True)
    args=parser.parse_args();base=Path(__file__).resolve().parent
    proof=json.loads((base/f'hnsw-stage-{args.rows}-audit-proof.private.json').read_bytes())
    output=report(proof)
    config=json.loads((base/f'hnsw-stage-{args.rows}-config.private.json').read_bytes())
    require(proof['assets']=={role:s['assets_digest'] for role,s in config['subjects'].items()},'asset binding differs')
    output['subjects']={role:{key:subject[key] for key in ('source_commit','source_tree','binary_sha256',
                                                         'parser_sha256','example_source_sha256','cargo_lock_sha256')}
                        for role,subject in config['subjects'].items()}
    require(output['stage_rows']==args.rows,'stage identity differs')
    (base/f'hnsw-stage-{args.rows}-report.json').write_text(json.dumps(output,indent=2)+'\n')
    (base/f'hnsw-stage-{args.rows}-report.md').write_text(markdown(output))
    print(json.dumps(dict(status='generated',stage_rows=args.rows,samples=10)))


if __name__=='__main__':main()
