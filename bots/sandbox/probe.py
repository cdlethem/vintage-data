#!/usr/bin/env python3
"""Exercise the installed confined runtime without creating dashboard tasks or PRs."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile

# Permit both `python -m bots.sandbox.probe` and invocation by absolute file path.
if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from bots.admitted_runner import _initialize_baseline
from bots.model_gateway import model_gateway
from bots.provider_dashboard import DashboardClient, ControlPlaneError


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--profile', choices=['junior', 'senior', 'staff'], default='junior')
    result.add_argument('--model-smoke', action='store_true', help='Also spend a small model call creating and verifying an isolated hello.txt fixture')
    return result


def select_model(settings: dict, profile: str) -> tuple[dict, str]:
    role = 'executor_' + profile
    assignment = next((item for item in settings.get('assignments', []) if item.get('role') == role), None)
    if assignment is None:
        raise RuntimeError(f'Map {role} in Model settings before running the probe')
    provider = next((item for item in settings.get('providers', []) if item.get('id') == assignment.get('provider_id')), None)
    if provider is None:
        raise RuntimeError(f'The provider mapped to {role} is missing; reconnect it in Model settings')
    return provider, assignment['model']


def run_probe(profile: str, model_smoke: bool) -> dict:
    launcher = os.environ.get('BOT_DASHBOARD_SANDBOX_LAUNCHER', '')
    if not launcher:
        raise RuntimeError('Set BOT_DASHBOARD_SANDBOX_LAUNCHER to the installed production launcher')
    path = Path(launcher)
    info = path.stat()
    if not path.is_absolute() or not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022 or not os.access(path, os.X_OK):
        raise RuntimeError('The launcher must be an absolute root-owned executable that is not group/other writable')
    deadline = datetime.now(timezone.utc) + timedelta(minutes=5)
    provider, model = select_model(DashboardClient.from_environment(deadline_at=deadline).model_settings(), profile)
    with tempfile.TemporaryDirectory(prefix='vintage-runtime-probe-') as temporary:
        root = Path(temporary)
        root.chmod(0o700)
        work = root / 'workdir'
        work.mkdir(mode=0o700)
        baseline_text = 'Isolated runtime probe fixture.\n'
        (work / 'README.md').write_text(baseline_text)
        _initialize_baseline(root, work)
        admission = {
            'protocol_version': 2, 'kind': 'executor',
            'task_id': '00000000-0000-0000-0000-000000000001',
            'profile': profile, 'model_role': {'junior':'@task', 'senior':'@default', 'staff':'@plan'}[profile],
            'model': {'provider_id': provider['id'], 'model': model, 'base_url': provider['base_url']},
            'reviewer_required': True, 'sequence': 1, 'revision': 1,
            'execution_id': '1' * 64, 'deadline_at': deadline.isoformat(), 'base_sha': '1' * 40,
            'task': {
                'title': 'Verify isolated execution', 'category': 'other',
                'planned_resolution': 'Create hello.txt containing exactly hello followed by a newline. This is a runtime fixture. Do not change README.md.',
                'verification_commands': [['python3', '-c', "from pathlib import Path; assert Path('hello.txt').read_text() == 'hello\\n'"]],
                'allowed_path_globs': ['hello.txt'], 'resource_keys': [],
            },
        }
        admission_path = root / 'admission.json'
        admission_path.write_text(json.dumps(admission))
        admission_path.chmod(0o400)
        result_path = root / 'result.json'
        command = [str(path), '--protocol', 'v2', '--workdir', str(work), '--admission', str(admission_path), '--result', str(result_path)]
        # The launcher receives no API credentials; only the trusted gateway holds them.
        environment = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C.UTF-8', 'XDG_RUNTIME_DIR': f'/run/user/{os.getuid()}', 'DBUS_SESSION_BUS_ADDRESS': f'unix:path=/run/user/{os.getuid()}/bus'}
        with model_gateway(provider, model, deadline_at=deadline, socket_path=root / 'model.sock'):
            for arguments, timeout in [(command + ['--probe'], 45)] + ([(command, 240)] if model_smoke else []):
                process = subprocess.run(arguments, env=environment, capture_output=True, timeout=timeout, check=False)
                if process.returncode:
                    raise RuntimeError(f'Runtime probe command failed with exit code {process.returncode}; inspect installed runtime and user service configuration')
            if model_smoke:
                payload = json.loads(result_path.read_text())
                checks = payload.get('report', {}).get('verification', [])
                if (work / 'hello.txt').read_text() != 'hello\n' or (work / 'README.md').read_text() != baseline_text:
                    raise RuntimeError('Model smoke fixture contents failed verification')
                if payload.get('report', {}).get('changed_paths') != ['hello.txt'] or not checks or any(item.get('exit_code') != 0 for item in checks):
                    raise RuntimeError('Model smoke report failed path or verification checks')
    return {'status': 'passed', 'profile': profile, 'confinement_probe': 'passed', 'model_smoke': 'passed' if model_smoke else 'not_requested'}


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        result = run_probe(args.profile, args.model_smoke)
    except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired, ControlPlaneError) as error:
        # Never print child model output, raw provider responses, or admission files.
        print(f'Runtime probe failed: {error}', file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
