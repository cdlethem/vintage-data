#!/usr/bin/env python3
"""Prepare an isolated runtime as an operator; install its immutable files as root."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

SOURCE=Path(__file__).resolve().parent

def prepare(output: Path, omp: Path):
    if output.exists():raise SystemExit('Choose a new staging directory')
    output.mkdir(mode=0o755)
    for name in ['launcher.py','worker.py','relay.py','model_config.py']:
        shutil.copy2(SOURCE/name,output/name)
    shutil.copy2(omp,output/'omp')
    (output/'launcher.py').chmod(0o755)
    (output/'omp').chmod(0o755)
    (output/'runtime.json').write_text(json.dumps({'confinement':'rootless-userns-cgroup-v1','network':'deny-all-except-model-gateway-v1','memory_max':'4G','tasks_max':128,'cpu_quota':'200%','max_seconds':2400},indent=2)+'\n')
    subprocess.run(['uv','venv','--relocatable','--python','/usr/bin/python3',str(output/'venv')],check=True)
    repo=SOURCE.parents[1]
    subprocess.run(['uv','pip','install','--python',str(output/'venv/bin/python'),'-r',str(repo/'transform/requirements.txt'),'-r',str(repo/'visualization/requirements.txt'),'pytest==8.4.2'],check=True)
    print(f'Prepared {output}; install it as root with this script install --source {output}')

def install(source: Path,destination: Path):
    if os.geteuid()!=0:raise SystemExit('Installation requires root to protect the launcher and runtime')
    if destination.exists():raise SystemExit('Install into a new versioned directory; do not modify a live runtime')
    if not (source/'runtime.json').is_file():raise SystemExit('Not a prepared runtime')
    shutil.copytree(source,destination,symlinks=True)
    for path in [destination,*destination.rglob('*')]:
        os.chown(path,0,0,follow_symlinks=False)
        if not path.is_symlink():path.chmod(path.stat().st_mode & ~0o022)
    print(f'Installed {destination}/launcher.py; run its confinement probe as the Airflow service user before enabling execution')

if __name__=='__main__':
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('prepare');p.add_argument('--output',type=Path,required=True);p.add_argument('--omp',type=Path,default=Path(shutil.which('omp') or '/usr/local/bin/omp'))
    p=sub.add_parser('install');p.add_argument('--source',type=Path,required=True);p.add_argument('--destination',type=Path,default=Path('/opt/vintage-bot-runtime-v1'))
    args=parser.parse_args()
    if args.command=='prepare':prepare(args.output,args.omp)
    else:install(args.source,args.destination)
