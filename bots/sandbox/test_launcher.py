import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from bots.sandbox import launcher, model_config


class LauncherTests(unittest.TestCase):
    def setUp(self):
        runtime = patch.object(launcher, 'RUNTIME', Path('/opt/vintage-bot-runtime'))
        runtime.start()
        self.addCleanup(runtime.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.work = self.root / 'work'
        self.work.mkdir()
        self.admission = self.root / 'admission.json'
        self.admission.write_text(json.dumps({'kind': 'executor'}))
        self.admission.chmod(0o400)
        self.result = self.root / 'result.json'

    def test_namespace_has_only_public_runtime_and_scoped_run_mounts(self):
        cmd = launcher.command(self.work, self.admission, self.root / 'output', self.root / 'model.sock',
                               {'memory_max': '2G', 'tasks_max': 64, 'cpu_quota': '100%'}, 90)
        self.assertIn('--unshare-all', cmd)
        self.assertIn('--clearenv', cmd)
        self.assertIn('KillMode=control-group', cmd)
        self.assertIn('RuntimeMaxSec=90', cmd)
        mounts = [(cmd[i], cmd[i+1], cmd[i+2]) for i in range(len(cmd)-2) if cmd[i] in ['--bind', '--ro-bind']]
        self.assertNotIn(('--ro-bind', '/usr', '/usr'), mounts)
        self.assertFalse(any(source.startswith(('/usr/local', '/home/colin', '/etc', '/run')) for _, source, _ in mounts))
        self.assertIn(('--ro-bind', '/usr/bin', '/usr/bin'), mounts)
        self.assertIn(('--bind', str(self.work), '/work'), mounts)
        self.assertIn(('--ro-bind', str(self.root / 'model.sock'), '/model.sock'), mounts)
        self.assertNotIn('--share-net', cmd)

    def test_dbt_profile_uses_disposable_warehouses(self):
        cmd = launcher.command(self.work, self.admission, self.root / 'output', self.root / 'model.sock',
                               {'memory_max': '2G', 'tasks_max': 64, 'cpu_quota': '100%'}, 90)
        environment = {cmd[i+1]: cmd[i+2] for i in range(len(cmd)-2) if cmd[i] == '--setenv'}
        self.assertEqual(environment['EXTRACT_WAREHOUSE'], '/output/raw.duckdb')
        self.assertEqual(environment['DBT_DEV_WAREHOUSE'], '/output/dev.duckdb')

    def test_reviewer_worktree_is_read_only(self):
        self.admission.chmod(0o600)
        self.admission.write_text(json.dumps({'kind': 'pr_reviewer'}))
        cmd = launcher.command(self.work, self.admission, self.root / 'output', self.root / 'model.sock',
                               {'memory_max': '2G', 'tasks_max': 64, 'cpu_quota': '100%'}, 90)
        index = cmd.index('/work')
        self.assertEqual(cmd[index-2:index+1], ['--ro-bind', str(self.work), '/work'])

    def test_review_evidence_is_read_only_and_reviewer_only(self):
        context = self.root / 'review-context.json'
        context.write_text('{}')
        context.chmod(0o400)
        config = {'memory_max': '2G', 'tasks_max': 64, 'cpu_quota': '100%'}
        cmd = launcher.command(self.work, self.admission, self.root/'output', self.root/'model.sock', config, 90)
        self.assertNotIn('/review-context.json', cmd)
        self.admission.chmod(0o600)
        self.admission.write_text(json.dumps({'kind': 'pr_reviewer'}))
        cmd = launcher.command(self.work, self.admission, self.root/'output', self.root/'model.sock', config, 90)
        index = cmd.index('/review-context.json')
        self.assertEqual(cmd[index-2:index+1], ['--ro-bind', str(context), '/review-context.json'])
        context.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'immutable'):
            launcher.command(self.work, self.admission, self.root/'output', self.root/'model.sock', config, 90)

    def test_rejects_symlink_admission_and_dangling_result(self):
        alias = self.root / 'alias.json'
        alias.symlink_to(self.admission)
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            launcher.validate_paths(str(self.work), str(alias), str(self.result))
        self.result.symlink_to(self.root / 'missing')
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            launcher.validate_paths(str(self.work), str(self.admission), str(self.result))

    def test_rejects_unsafe_admission_permissions_before_launch(self):
        self.admission.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'immutable'):
            launcher.validate_paths(str(self.work), str(self.admission), str(self.result))

    def test_rejects_regular_file_in_place_of_worktree(self):
        file = self.root / 'file'
        file.touch()
        with self.assertRaisesRegex(ValueError, 'private run directory'):
            launcher.validate_paths(str(file), str(self.admission), str(self.result))

    def test_configuration_contains_only_scoped_gateway(self):
        model_config.write_config(self.root / 'home', 'vendor/model')
        config = json.loads((self.root / 'home/.omp/agent/models.yml').read_text())
        provider = config['providers']['gateway']
        self.assertEqual(provider['baseUrl'], 'http://127.0.0.1:18080/v1')
        self.assertEqual(provider['apiKey'], 'sandbox-scoped')
        self.assertEqual(provider['models'][0]['id'], 'vendor/model')
        with self.assertRaises(ValueError):
            model_config.write_config(self.root / 'other', 'model\ninvalid')


if __name__ == '__main__':
    unittest.main()
