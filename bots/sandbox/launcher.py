#!/usr/bin/python3
"""Root-installed, unprivileged protocol-v2 Linux sandbox launcher.

Only the admitted worktree, public runtime and one per-run model socket enter the
namespace. Provider credentials and the parent's Git metadata stay outside it.
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import uuid

RUNTIME = Path(__file__).resolve().parent


def trusted_file(path: Path) -> None:
    for entry in [path, *path.parents]:
        info = entry.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError(f"runtime path is not root-owned and immutable: {entry}")


def check_runtime() -> dict:
    for name in ['launcher.py', 'worker.py', 'relay.py', 'model_config.py', 'omp', 'runtime.json']:
        trusted_file(RUNTIME / name)
    # Python dependencies are executable code too. A writable installed package
    # would let a confined run execute attacker-modified trusted runtime code.
    trusted_file(RUNTIME / 'venv')
    for directory, names, files in os.walk(RUNTIME / 'venv', followlinks=False):
        for name in names + files:
            path = Path(directory) / name
            info = path.lstat()
            if info.st_uid != 0 or info.st_mode & 0o022 and not stat.S_ISLNK(info.st_mode):
                raise ValueError('runtime dependency is not root-owned and immutable')
            if path.is_symlink():
                target = path.resolve(strict=True)
                if not any(target.is_relative_to(root) for root in [RUNTIME, Path('/usr/bin'), Path('/usr/lib'), Path('/usr/lib64')]):
                    raise ValueError('runtime dependency symlink leaves public runtime')
                trusted_file(target)
    config = json.loads((RUNTIME / 'runtime.json').read_text())
    if config.get('confinement') != 'rootless-userns-cgroup-v1' or config.get('network') != 'deny-all-except-model-gateway-v1':
        raise ValueError('runtime policy mismatch')
    for executable in ['/usr/bin/bwrap','/usr/bin/systemd-run',str(RUNTIME/'venv/bin/python')]:
        if not os.access(executable, os.X_OK):
            raise ValueError(f'runtime dependency unavailable: {executable}')
    return config


def command(work: Path, admission: Path, output: Path, socket: Path, config: dict, seconds: int, *, probe: bool=False) -> list[str]:
    # A service cgroup kills every child at timeout, including double-forked tools.
    cmd = ['/usr/bin/systemd-run','--user','--quiet','--wait','--pipe','--collect',
           '--unit=vintage-bot-'+uuid.uuid4().hex,
           '-p',f'MemoryMax={config["memory_max"]}', '-p',f'TasksMax={config["tasks_max"]}',
           '-p',f'CPUQuota={config["cpu_quota"]}', '-p',f'RuntimeMaxSec={seconds}',
           '-p','KillMode=control-group','-p','TimeoutStopSec=5s',
           '/usr/bin/bwrap','--unshare-all','--die-with-parent','--new-session',
           '--cap-drop','ALL','--clearenv',
           '--dir','/usr','--ro-bind','/usr/bin','/usr/bin',
           '--ro-bind','/usr/lib','/usr/lib','--ro-bind','/usr/lib64','/usr/lib64',
           '--ro-bind','/usr/share','/usr/share','--symlink','usr/bin','/bin',
           '--symlink','usr/lib','/lib','--symlink','usr/lib64','/lib64',
           '--proc','/proc','--dev','/dev','--tmpfs','/tmp',
           '--dir','/home/bot','--dir','/etc',
           '--ro-bind',str(RUNTIME/'venv'),str(RUNTIME/'venv'),
           '--ro-bind',str(RUNTIME/'worker.py'),'/opt/worker.py',
           '--ro-bind',str(RUNTIME/'relay.py'),'/opt/relay.py',
           '--ro-bind',str(RUNTIME/'omp'),'/opt/bin/omp',
           '--ro-bind',str(admission),'/admission.json',
           '--ro-bind',str(socket),'/model.sock',
           '--bind',str(output),'/output',
           '--setenv','PATH',f'{RUNTIME}/venv/bin:/opt/bin:/usr/bin:/bin',
           '--setenv','HOME','/home/bot','--setenv','LANG','C.UTF-8',
           '--setenv','PYTHONDONTWRITEBYTECODE','1',
           '--setenv','PYTEST_ADDOPTS','-p no:cacheprovider',
           '--setenv','DBT_DEV_WAREHOUSE','/output/dev.duckdb',
           '--setenv','EXTRACT_WAREHOUSE','/output/raw.duckdb',
           '--setenv','VINTAGE_VISUALIZATION_PYTHON',str(RUNTIME/'venv/bin/python'),
           '--setenv','OMP_CONFIG_DIR','/output/home/.omp',
           '--setenv','PI_CONFIG_DIR','/output/home/.omp',
           '--setenv','PI_CODING_AGENT_DIR','/output/home/.omp/agent']
    read_only = json.loads(admission.read_text())['kind'] == 'pr_reviewer'
    review_context = admission.parent / 'review-context.json'
    if read_only and (review_context.exists() or review_context.is_symlink()):
        info = review_context.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o400 or not 1 <= info.st_size <= 4 * 1024 * 1024):
            raise ValueError('review context must be bounded, immutable and caller-owned')
        cmd += ['--ro-bind', str(review_context), '/review-context.json']
    cmd += ['--ro-bind' if read_only else '--bind',str(work),'/work','--chdir','/work']
    if probe:
        cmd += ['/usr/bin/python3','/opt/relay.py','--probe']
    else:
        cmd += [str(RUNTIME/'venv/bin/python'),'/opt/relay.py','--',
                str(RUNTIME/'venv/bin/python'),'/opt/worker.py',
                '--admission','/admission.json','--workdir','/work','--result','/output/result.json']
    return cmd


def validate_paths(workdir: str, admission_path: str, result_path: str) -> tuple[Path, Path, Path, Path]:
    # Reject aliases before resolving so neither admission nor writable paths can
    # redirect the launcher to a sibling run or a dangling output symlink.
    work=Path(workdir).absolute()
    admission=Path(admission_path).absolute()
    result=Path(result_path).absolute()
    if any(path.is_symlink() for path in [work, admission, result]):
        raise ValueError('run paths must not be symlinks')
    if any(path.parent.resolve(strict=True) != path.parent for path in [work, admission, result]):
        raise ValueError('run paths must use canonical parent directories')
    root=admission.parent
    if work.parent != root or result.parent != root or result.exists() or not work.is_dir():
        raise ValueError('work, admission and new result must share one private run directory')
    info=admission.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o400 or not stat.S_ISREG(info.st_mode):
        raise ValueError('admission must be private, immutable and owned by the caller')
    if stat.S_IMODE(root.stat().st_mode) != 0o700 or root.stat().st_uid != os.getuid():
        raise ValueError('run directory must be private and caller-owned')
    socket=root/'model.sock'
    if not stat.S_ISSOCK(socket.lstat().st_mode) or socket.stat().st_uid != os.getuid():
        raise ValueError('per-run model socket unavailable')
    return work, admission, result, socket


def launch(args) -> None:
    config=check_runtime()
    work, admission, result, socket = validate_paths(args.workdir, args.admission, args.result)
    root=admission.parent
    data=json.loads(admission.read_text())
    deadline=dt.datetime.fromisoformat(data['deadline_at'].replace('Z','+00:00'))
    if deadline.tzinfo is None:
        raise ValueError('admission deadline must include a timezone')
    if data.get('kind') not in ['executor', 'pr_reviewer']:
        raise ValueError('unknown admission kind')
    seconds=min(config['max_seconds'],int((deadline-dt.datetime.now(dt.timezone.utc)).total_seconds())-60)
    if seconds<30:raise ValueError('admission deadline exhausted')
    with tempfile.TemporaryDirectory(prefix='sandbox-output-',dir=root) as out:
        output=Path(out)
        # This configuration contains no key, broker token, host endpoint or auth store.
        model=data['model']['model']
        from model_config import write_config
        write_config(output/'home',model)
        env={'PATH':'/usr/bin:/bin','LANG':'C.UTF-8','XDG_RUNTIME_DIR':f'/run/user/{os.getuid()}',
             'DBUS_SESSION_BUS_ADDRESS':f'unix:path=/run/user/{os.getuid()}/bus'}
        process=subprocess.run(command(work,admission,output,socket,config,seconds,probe=args.probe),
                               stdin=subprocess.DEVNULL,env=env,timeout=seconds+15,check=False)
        if process.returncode:raise RuntimeError(f'confined process exited {process.returncode}')
        if args.probe:return
        source=output/'result.json'
        fd=os.open(source,os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as stream:
            meta=os.fstat(stream.fileno())
            if not stat.S_ISREG(meta.st_mode) or meta.st_size>1_048_576 or stat.S_IMODE(meta.st_mode)!=0o600:
                raise ValueError('invalid sandbox result')
            payload=json.load(stream)
        # Git metadata remains host-side. Report the same staged path set the trusted
        # parent will validate; generated ignored test artifacts are not source edits.
        if data['kind']=='executor':
            git=['/usr/bin/git',f'--git-dir={root}/baseline.git',f'--work-tree={work}']
            subprocess.run([*git,'add','-A'],cwd=work,check=True,capture_output=True)
            paths=subprocess.check_output([*git,'diff','--cached','--name-only','-z','HEAD','--'],cwd=work,text=True).rstrip('\0').split('\0')
            paths = [path for path in paths if path]
            payload['report']['changed_paths']=paths
        fd=os.open(result,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'w') as stream:json.dump(payload,stream)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--check',action='store_true')
    parser.add_argument('--protocol',choices=['v2'])
    parser.add_argument('--workdir');parser.add_argument('--admission');parser.add_argument('--result')
    parser.add_argument('--probe',action='store_true')
    args=parser.parse_args()
    if args.check:
        print(json.dumps({'status':'installed','policy':check_runtime()}));return
    if args.protocol!='v2' or not all([args.workdir,args.admission,args.result]):parser.error('protocol v2 requires workdir, admission and result')
    launch(args)

if __name__=='__main__':main()
