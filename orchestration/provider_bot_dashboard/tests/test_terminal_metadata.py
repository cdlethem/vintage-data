import unittest
from types import SimpleNamespace
from airflow.providers.vintage.bot_dashboard.execution import _record_terminal_metadata

class TerminalMetadataTest(unittest.TestCase):
    def test_blocked_result_preserves_bounded_explanation(self):
        row=SimpleNamespace()
        report=SimpleNamespace(failure_class=None,failure_detail=None,body_json={'status':'blocked','summary':'A required preview identity is unavailable. '+ 'x'*9000})
        _record_terminal_metadata(row,report,'execution_blocked')
        self.assertEqual('execution_blocked',row.terminal_reason_code)
        self.assertEqual('ExecutionBlocked',row.terminal_failure_class)
        self.assertTrue(row.terminal_detail.startswith('A required preview identity'))
        self.assertEqual(8192,len(row.terminal_detail))
    def test_reported_failure_takes_precedence_over_summary(self):
        row=SimpleNamespace()
        report=SimpleNamespace(failure_class='ModelRateLimited',failure_detail='Provider rate limit',body_json={'summary':'unrelated'})
        _record_terminal_metadata(row,report,'model_rate_limited')
        self.assertEqual('ModelRateLimited',row.terminal_failure_class)
        self.assertEqual('Provider rate limit',row.terminal_detail)
    def test_malformed_summary_is_not_exposed_as_an_object_repr(self):
        row=SimpleNamespace()
        report=SimpleNamespace(failure_class=None,failure_detail=None,body_json={'summary':{'private':'value'}})
        _record_terminal_metadata(row,report,'execution_blocked')
        self.assertEqual('',row.terminal_detail)
