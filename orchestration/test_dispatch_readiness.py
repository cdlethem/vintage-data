"""Dispatch readiness must follow the API, not stripped SDK configuration."""
import sys
from pathlib import Path
from unittest import TestCase, mock
sys.path.insert(0, str(Path(__file__).parent / 'dags'))
import provider_bot_dashboard_dags as dags

class DispatchReadinessTest(TestCase):
    def test_authoritative_readiness_and_fail_closed(self):
        with mock.patch.object(dags.provider_dashboard.DashboardClient, 'from_environment') as factory:
            for value, expected in [({'ready': True}, True), ({'ready': False}, False), ({}, False)]:
                factory.return_value.execution_readiness.return_value = value
                self.assertEqual(dags._executor_available(), expected)
            factory.return_value.execution_readiness.side_effect = RuntimeError('unavailable')
            with self.assertRaises(RuntimeError):
                dags._executor_available()

    def test_client_uses_existing_internal_endpoint(self):
        client = object.__new__(dags.provider_dashboard.DashboardClient)
        with mock.patch.object(client, '_request', return_value={'ready': True}) as request:
            self.assertEqual(client.execution_readiness(), {'ready': True})
            request.assert_called_once_with('GET', 'executions/readiness')
