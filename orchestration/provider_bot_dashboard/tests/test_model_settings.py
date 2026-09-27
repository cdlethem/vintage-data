from __future__ import annotations
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
import httpx
from airflow.providers.vintage.bot_dashboard import model_settings as settings
from airflow.providers.vintage.bot_dashboard.service import DomainError, PreconditionFailed

class ModelSettingsTest(unittest.TestCase):
    def test_current_labels_prefer_saved_dashboard_assignments(self):
        provider = SimpleNamespace(password='private-test-key', extra_dejson={'name':'Connected provider','base_url':'https://example.com/v1','models':['small']})
        mappings = [{'role':'manager','provider_id':'gateway','model':'small'}]
        with patch.object(settings, '_connections', return_value={'gateway':provider}), patch.object(settings, '_assignments', return_value=mappings), patch.object(settings, 'current_models', return_value=[{'role':'manager','current_provider':'command','current_model':'@plan'}]):
            result = settings.get_settings(Mock())
        self.assertEqual([{'role':'manager','current_provider':'Connected provider','current_model':'small'}],result['current_models'])
        self.assertNotIn('private-test-key',json.dumps(result))

    def test_private_gateway_requires_exact_operator_allowlist(self):
        answers = [(None, None, None, None, ('127.0.0.1', 8000))]
        with patch.object(settings.socket, 'getaddrinfo', return_value=answers), patch.dict(settings.os.environ, {}, clear=True):
            with self.assertRaises(DomainError): settings.validate_base_url('http://localhost:8000/v1')
            with patch.dict(settings.os.environ, {'BOT_DASHBOARD_MODEL_ALLOWED_HOSTS': 'localhost:8000'}):
                self.assertEqual('http://localhost:8000/v1', settings.validate_base_url('http://localhost:8000/v1/'))
                with self.assertRaises(DomainError): settings.validate_base_url('http://localhost:8001/v1')
        for url in ['https://user:key@example.com/v1', 'https://example.com/v1?key=secret', 'file:///etc/passwd']:
            with self.assertRaises(DomainError): settings.validate_base_url(url)

    def test_provider_key_is_redacted_and_not_reused_for_changed_url(self):
        existing = SimpleNamespace(password='private-test-key', extra_dejson={'name':'Gateway','base_url':'https://example.com/v1','models':['small']})
        public = settings._public_provider('gateway', existing)
        self.assertTrue(public['has_api_key'])
        self.assertNotIn('private-test-key', json.dumps(public))
        body = settings.ProviderBody(id='gateway', name='Gateway', base_url='https://different.example/v1')
        with patch.object(settings, '_connections', return_value={'gateway':existing}), patch.object(settings, 'validate_base_url', side_effect=lambda value:value):
            with self.assertRaises(DomainError): settings.test_provider(Mock(), body)

    def test_connection_test_loads_models_without_persisting(self):
        requests = []
        def respond(request):
            requests.append(request)
            return httpx.Response(200, json={'data':[{'id':'small'}, {'id':'large'}, {'id':'small'}]})
        client = httpx.Client(transport=httpx.MockTransport(respond))
        body = settings.ProviderBody(id='gateway', name='Gateway', base_url='https://example.com/v1', api_key='private-test-key')
        session = Mock()
        with patch.object(settings, '_connections', return_value={}), patch.object(settings, 'validate_base_url', side_effect=lambda value:value), patch.object(settings.httpx, 'Client', return_value=client):
            result = settings.test_provider(session, body)
        self.assertEqual(['large','small'],result['models'])
        self.assertEqual('Bearer private-test-key',requests[0].headers['Authorization'])
        self.assertNotIn('private-test-key',json.dumps(result))
        session.add.assert_not_called()
        session.flush.assert_not_called()

    def test_mapping_rejects_missing_models_and_duplicate_roles(self):
        provider = SimpleNamespace(extra_dejson={'models':['small']})
        with patch.object(settings, '_connections', return_value={'gateway':provider}):
            for values in [[{'role':'manager','provider_id':'gateway','model':'missing'}], [{'role':'unknown','provider_id':'gateway','model':'small'}], [{'role':'manager','provider_id':'gateway','model':'small'}]*2]:
                with self.assertRaises(DomainError):
                    settings.save_assignments(Mock(),settings.SettingsBody(assignments=values))

    def test_runtime_secret_stays_out_of_admission_snapshot(self):
        provider = SimpleNamespace(password='private-test-key', extra_dejson={'base_url':'https://example.com/v1'})
        mappings = [{'role':'executor_junior','provider_id':'gateway','model':'small'}]
        with patch.object(settings, '_connections', return_value={'gateway':provider}), patch.object(settings, '_assignments', return_value=mappings):
            snapshot = settings.model_for_role(Mock(),'executor_junior')
            self.assertEqual({'provider_id':'gateway','model':'small','base_url':'https://example.com/v1'},snapshot)
            self.assertEqual('private-test-key',settings.runtime_settings(Mock())['providers'][0]['api_key'])
            with self.assertRaises(PreconditionFailed): settings.model_for_role(Mock(),'executor_staff')

if __name__ == '__main__': unittest.main()
