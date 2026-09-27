import unittest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from bots.admitted_runner import _validate_paths
from bots.provider_dashboard import ControlPlaneError
from bots.bot_runner import _failure_detail

class PathDiagnosticsTest(unittest.TestCase):
    def test_rejected_path_survives_safe_failure_projection(self):
        admission={'task':{'allowed_path_globs':['extract/scripts/**']},'repository_policy':{'allowed_path_globs':['extract/**','transform/**'],'denied_path_globs':[], 'max_changed_files':40,'max_diff_bytes':500000}}
        path='transform/models/marts/family/fct_family.sql'
        with self.assertRaises(ControlPlaneError) as caught:
            _validate_paths(admission,[path],100,ControlPlaneError)
        self.assertEqual('task_path_policy_rejected',caught.exception.code)
        self.assertEqual('terminal',caught.exception.retry_class)
        self.assertIn(path,_failure_detail(caught.exception))
