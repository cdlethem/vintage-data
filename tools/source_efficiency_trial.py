#!/usr/bin/env python3
"""Capture and report bounded source-efficiency trial observations. Stdlib only."""
from __future__ import annotations
import argparse, json, os, pathlib
from datetime import datetime, timezone

def root(source, trial):
    return pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT") or "~/.local/share/vintage-data/extract").expanduser()/"experiments"/"efficiency"/source/trial
def load(path): return json.loads(pathlib.Path(path).read_text())
def main(argv=None):
    p=argparse.ArgumentParser(); sub=p.add_subparsers(dest="cmd",required=True)
    c=sub.add_parser("capture"); c.add_argument("--source",required=True); c.add_argument("--trial-id",required=True); c.add_argument("--production-manifest",required=True); c.add_argument("--candidate-manifest",required=True); c.add_argument("--cadence-seconds",type=int,required=True)
    r=sub.add_parser("report"); r.add_argument("--source",required=True); r.add_argument("--trial-id",required=True)
    a=p.parse_args(argv); out=root(a.source,a.trial_id); out.mkdir(parents=True,exist_ok=True)
    if a.cmd=="capture":
        prod,candidate=load(a.production_manifest),load(a.candidate_manifest)
        observation={"captured_at":datetime.now(timezone.utc).isoformat(),"source":a.source,"trial_id":a.trial_id,"cadence_seconds":a.cadence_seconds,"production":prod,"candidate":candidate}
        name=f"observation-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"; (out/name).write_text(json.dumps(observation,indent=2)+"\n"); print(out/name); return 0
    observations=[load(x) for x in sorted(out.glob('observation-*.json'))]
    if not observations: raise SystemExit("no captured observations")
    def metric(o, side, key): return o[side].get(key, o[side].get("metrics",{}).get(key,0)) or 0
    report={"source":a.source,"trial_id":a.trial_id,"observations":len(observations),"generated_at":datetime.now(timezone.utc).isoformat(),"production_bytes":sum(metric(o,"production","bytes") for o in observations),"candidate_bytes":sum(metric(o,"candidate","bytes") for o in observations),"production_records":sum(metric(o,"production","records") for o in observations),"candidate_records":sum(metric(o,"candidate","records") for o in observations)}
    report["byte_ratio"]=(report["candidate_bytes"]/report["production_bytes"] if report["production_bytes"] else None)
    (out/"report.json").write_text(json.dumps(report,indent=2)+"\n"); print(json.dumps(report,indent=2)); return 0
if __name__=="__main__": raise SystemExit(main())
