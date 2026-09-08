#!/usr/bin/env python3
"""Capture immutable OData endpoint evidence before source onboarding. Stdlib only."""
from __future__ import annotations
import argparse, json, pathlib, urllib.error, urllib.request
from datetime import datetime, timezone

def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--url",required=True); p.add_argument("--out",required=True); p.add_argument("--timeout",type=int,default=30); a=p.parse_args(argv)
 req=urllib.request.Request(a.url,headers={"User-Agent":"vintage-data/0.1"})
 try:
  with urllib.request.urlopen(req,timeout=a.timeout) as resp:
   raw=resp.read(); status=getattr(resp,"status",200); headers=dict(resp.headers.items())
 except urllib.error.HTTPError as exc:
  report={"captured_at":datetime.now(timezone.utc).isoformat(),"url":a.url,"status":exc.code,"headers":dict(exc.headers.items()),"error":str(exc)}
  out=pathlib.Path(a.out); out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(report,indent=2)+"\n"); print(out); return 1
 try: body=json.loads(raw)
 except json.JSONDecodeError: raise SystemExit("endpoint did not return JSON")
 if not isinstance(body,dict) or "value" not in body: raise SystemExit("OData response lacks value list")
 report={"captured_at":datetime.now(timezone.utc).isoformat(),"url":a.url,"status":status,"headers":headers,"rows":len(body["value"]),"next_link":body.get("@odata.nextLink"),"sample":body["value"][:3]}
 out=pathlib.Path(a.out); out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(report,indent=2)+"\n"); print(out); return 0
if __name__=="__main__": raise SystemExit(main())
