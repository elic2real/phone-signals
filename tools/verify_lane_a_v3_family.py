#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from statistics import mean


def load_json(p: str):
    return json.loads(Path(p).read_text(encoding='utf-8'))


def run_sweep(trace: str, target: str, cfile: str, out: str):
    cmd=[
        'python3','tools/sweep_from_trace.py',
        '--trace',trace,
        '--target-key',target,
        '--candidates',cfile,
        '--out',out,
    ]
    r=subprocess.run(cmd,capture_output=True,text=True)
    if r.returncode!=0:
        raise RuntimeError(r.stderr or r.stdout)
    return load_json(out)


def family_for_pair(pair: str) -> str:
    if 'CHF' in pair:
        return 'CHF_FAMILY'
    if 'JPY' in pair:
        return 'JPY_FAMILY'
    return 'USD_FAMILY'


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()

    traceset=load_json('proof_artifacts/TRACESET_4x24H_MANIFEST_v2.json')
    targets=load_json('proof_artifacts/SWEEP_TARGETS_V3_TOP10.json').get('targets',[])[:args.top]
    shards=[{'id':s.get('shard_id') or Path(s.get('trace_path','')).stem,'trace':s.get('trace_path')} for s in traceset.get('shards',[])]

    family_knobs={
      'USD_FAMILY': {
        'entry.tick.confirm_disp_atr':0.1,
        'entry.tick.base_max_dist_atr':0.2,
        'promote_mfe_atr':0.15,
        'extension_allow_energy_min':1.05,
        'aee.fail_windows':1,
      },
      'JPY_FAMILY': {
        'entry.tick.confirm_disp_atr':0.1,
        'entry.tick.base_max_dist_atr':0.2,
        'promote_mfe_atr':0.15,
        'extension_allow_energy_min':1.05,
        'aee.fail_windows':1,
      },
      'CHF_FAMILY': {
        'entry.tick.confirm_disp_atr':0.1,
        'entry.tick.base_max_dist_atr':0.2,
        'promote_mfe_atr':0.15,
        'extension_allow_energy_min':0.85,
        'aee.fail_windows':1,
      }
    }

    per_target=[]
    for t in targets:
        key=t['key']
        pair=key.split('|')[0]
        fam=family_for_pair(pair)
        cands=[
          {'name':'base','knobs':{}},
          {'name':fam,'knobs':family_knobs[fam]},
        ]
        cfile=f'/tmp/v3fam_{pair}.json'
        Path(cfile).write_text(json.dumps(cands),encoding='utf-8')
        shard_rows=[]
        for sh in shards:
            out=f"/tmp/v3fam_{pair}_{sh['id']}.json"
            d=run_sweep(sh['trace'],key,cfile,out)
            ranked=d.get('ranked_candidates',[])
            cand=next((r for r in ranked if r.get('candidate')==fam), None)
            if not cand:
                continue
            dEE=float(cand.get('delta_expected_extraction_atr',0.0) or 0.0)
            dCAP=float(cand.get('delta_capture_to_ceiling',0.0) or 0.0)
            dEph=float(cand.get('delta_extraction_per_hour',0.0) or 0.0)
            ok=(dEE>=1e-5 and dCAP>=0.005 and dEph>=0.01 and float(cand.get('delta_pnl_atr_p10',0.0) or 0.0)>=0.0)
            shard_rows.append({'shard_id':sh['id'],'dEE':dEE,'dCAP':dCAP,'dEph':dEph,'dTail':float(cand.get('delta_pnl_atr_p10',0.0) or 0.0),'pass':ok})
        pass_shards=sum(1 for r in shard_rows if r['pass'])
        per_target.append({'target_key':key,'family':fam,'pass_shards':pass_shards,'decision':'PASS' if pass_shards>=3 else 'HOLD','shards':shard_rows})

    fam_rollup={}
    for fam in ['USD_FAMILY','JPY_FAMILY','CHF_FAMILY']:
        rows=[r for r in per_target if r['family']==fam]
        if not rows:
            continue
        all_sh=[s for r in rows for s in r['shards']]
        fam_rollup[fam]={
          'targets':len(rows),
          'pass_targets':sum(1 for r in rows if r['decision']=='PASS'),
          'mean_dEE':mean([s['dEE'] for s in all_sh]) if all_sh else 0.0,
          'mean_dCAP':mean([s['dCAP'] for s in all_sh]) if all_sh else 0.0,
          'mean_dEph':mean([s['dEph'] for s in all_sh]) if all_sh else 0.0,
        }

    out={
      'generated_utc': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),
      'state_model':'pair|session|atr_bucket',
      'targets_evaluated':len(per_target),
      'results':per_target,
      'family_rollup':fam_rollup,
      'family_knobs':family_knobs,
    }
    Path('proof_artifacts/LANE_A_V3_FAMILY_VERIFY.json').write_text(json.dumps(out,indent=2),encoding='utf-8')
    print(json.dumps({'targets_evaluated':len(per_target),'pass_count':sum(1 for r in per_target if r['decision']=='PASS'),'family_rollup':fam_rollup}))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
