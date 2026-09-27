from __future__ import annotations
import unittest
from unittest.mock import patch
from bots.sandbox import probe

class ProbeTest(unittest.TestCase):
    def test_defaults_and_profile_validation(self):
        args = probe.parser().parse_args([])
        self.assertEqual('junior', args.profile)
        self.assertFalse(args.model_smoke)
        self.assertTrue(probe.parser().parse_args(['--profile','staff','--model-smoke']).model_smoke)

    def test_missing_launcher_or_mapping_is_actionable(self):
        with patch.dict(probe.os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, 'BOT_DASHBOARD_SANDBOX_LAUNCHER'):
                probe.run_probe('junior', False)
        with self.assertRaisesRegex(RuntimeError, 'Map executor_junior'):
            probe.select_model({'providers': [], 'assignments': []}, 'junior')
        with self.assertRaisesRegex(RuntimeError, 'provider mapped to executor_staff is missing'):
            probe.select_model({'providers': [], 'assignments': [{'role':'executor_staff','provider_id':'missing','model':'model'}]}, 'staff')

    def test_selection_uses_requested_executor_profile(self):
        provider = {'id':'connected','base_url':'https://provider.example/v1','api_key':'private-key'}
        settings = {'providers':[provider],'assignments':[{'role':'executor_junior','provider_id':'connected','model':'small'},{'role':'executor_staff','provider_id':'connected','model':'large'}]}
        self.assertEqual((provider,'large'), probe.select_model(settings,'staff'))

if __name__ == '__main__': unittest.main()
