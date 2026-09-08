#!/usr/bin/env python3
"""Fetch GDELT DOC with a persistent 429 circuit breaker. Stdlib only."""
from __future__ import annotations
import argparse, json, os, pathlib, sys, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

BASE="https://api.gdeltproject.org/api/v2/doc/doc"; USER_AGENT=os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1"; PREFIX="VINTAGE_RUN_SUMMARY\t"
def now(): return datetime.now(timezone.utc)
def state_path(): return pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT") or "~/.local/share/vintage-data/extract").expanduser()/"state"/"gdelt_doc.json"
def load_state(path):
    try: return json.loads(path.read_text())
    except FileNotFoundError: return {"consecutive_429":0,"next_probe_at":None,"last_status":None,"last_attempt_at":None,"last_success_at":None,"retry_count":0,"cooldown_s":0}
def save_state(path, state):
    path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_suffix(".tmp"); tmp.write_text(json.dumps(state,indent=2)+"\n"); os.replace(tmp,path)
def _get(query):
    url=BASE+"?"+urllib.parse.urlencode({"query":query,"mode":"ArtList","format":"json","sort":"DateDesc","timespan":"60min","maxrecords":250})
    req=urllib.request.Request(url,headers={"User-Agent":USER_AGENT})
    with urllib.request.urlopen(req,timeout=30) as r: return json.load(r), getattr(r,"status",200)
def main(argv=None):
    p=argparse.ArgumentParser(); p.add_argument("--query",default='"artificial intelligence"'); p.add_argument("--state-file"); a=p.parse_args(argv)
    path=pathlib.Path(a.state_file) if a.state_file else state_path(); state=load_state(path); started=now(); state["last_attempt_at"]=started.isoformat()
    due=state.get("next_probe_at")
    if due and started < datetime.fromisoformat(due):
        print(PREFIX+json.dumps({"health":"open_circuit","completeness":"unknown","records":0,"requests":{"attempted":0},"metrics":{"next_probe_at":due,"cooldown_s":state["cooldown_s"]}}),file=sys.stderr); save_state(path,state); return 0
    try: data,status=_get(a.query)
    except urllib.error.HTTPError as exc:
        state["last_status"]=exc.code
        if exc.code != 429: save_state(path,state); print(PREFIX+json.dumps({"health":"failed","completeness":"failed","records":0,"requests":{"attempted":1},"error":str(exc)}),file=sys.stderr); return 1
        n=state["consecutive_429"]+1; retry=exc.headers.get("Retry-After")
        try: retry_s=max(0,int(float(retry)))
        except (TypeError,ValueError): retry_s=0
        cooldown=min(86400,max(retry_s,3600*2**(n-1))); state.update({"consecutive_429":n,"retry_count":state["retry_count"]+1,"cooldown_s":cooldown,"next_probe_at":(started+timedelta(seconds=cooldown)).isoformat()}); save_state(path,state)
        print(PREFIX+json.dumps({"health":"open_circuit","completeness":"unknown","records":0,"requests":{"attempted":1},"metrics":{"status":429,"cooldown_s":cooldown,"next_probe_at":state["next_probe_at"]}}),file=sys.stderr); return 0
    except Exception as exc:
        state["last_status"]="error"; save_state(path,state); print(PREFIX+json.dumps({"health":"failed","completeness":"failed","records":0,"requests":{"attempted":1},"error":str(exc)}),file=sys.stderr); return 1
    articles=data.get("articles") if isinstance(data,dict) else None
    if not isinstance(articles,list): print(PREFIX+json.dumps({"health":"failed","completeness":"failed","records":0,"requests":{"attempted":1},"error":"invalid GDELT schema"}),file=sys.stderr); return 1
    stamp=started.isoformat()
    for item in articles: print(json.dumps({"source":"gdelt_doc","fetched_at":stamp,"id":item.get("url"),"published":item.get("seendate"),"title":item.get("title"),"domain":item.get("domain"),"language":item.get("language"),"sourcecountry":item.get("sourcecountry"),"url":item.get("url")},ensure_ascii=False))
    state.update({"last_status":status,"consecutive_429":0,"cooldown_s":3600,"next_probe_at":(started+timedelta(hours=1)).isoformat()})
    health="healthy" if articles else "degraded"
    if articles: state["last_success_at"]=stamp
    save_state(path,state); print(PREFIX+json.dumps({"health":health,"completeness":"complete" if articles else "unknown","records":len(articles),"requests":{"attempted":1}}),file=sys.stderr); return 0
if __name__=="__main__": raise SystemExit(main())
