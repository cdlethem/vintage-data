"""The Compose wrapper must preserve an explicitly selected stack."""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import tempfile
import unittest


class ComposeEnvironmentTest(unittest.TestCase):
    def test_caller_environment_wins_over_sourced_defaults(self):
        root = pathlib.Path(__file__).resolve().parent
        with tempfile.TemporaryDirectory() as directory:
            temp = pathlib.Path(directory)
            defaults = temp / "airflow.env"
            secrets = temp / "secrets.env"
            defaults.write_text("LIGHTDASH_STATE_ROOT=/defaults/state\nLIGHTDASH_URL=http://defaults\n")
            secrets.write_text("LIGHTDASH_API_KEY=default-token\n")
            fake_bin = temp / "bin"
            fake_bin.mkdir()
            docker = fake_bin / "docker"
            docker.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os\n"
                "print(json.dumps({key: os.environ.get(key) for key in "
                "('LIGHTDASH_STATE_ROOT', 'LIGHTDASH_URL', 'LIGHTDASH_API_KEY')}))\n"
            )
            docker.chmod(0o755)
            environment = {
                **os.environ,
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "VINTAGE_DATA_ENV_FILE": str(defaults),
                "VINTAGE_DATA_SECRETS_ENV_FILE": str(secrets),
                "LIGHTDASH_STATE_ROOT": "/isolated/state",
                "LIGHTDASH_URL": "http://isolated",
                "LIGHTDASH_API_KEY": "isolated-token",
            }
            result = subprocess.run(
                [str(root / "bin" / "compose"), "version"],
                check=True,
                capture_output=True,
                text=True,
                env=environment,
            )
        self.assertEqual(json.loads(result.stdout), {
            "LIGHTDASH_STATE_ROOT": "/isolated/state",
            "LIGHTDASH_URL": "http://isolated",
            "LIGHTDASH_API_KEY": "isolated-token",
        })
