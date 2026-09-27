import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from bots.admitted_runner import _admitted_gateway
from bots.provider_dashboard import ControlPlaneError

class GatewayTest(unittest.TestCase):
    def test_gateway_uses_admitted_identity_and_private_socket(self):
        provider = {'id':'p','base_url':'https://example.test/v1','api_key':'private'}
        client = Mock(spec=['model_settings'])
        client.model_settings.return_value = {'providers':[provider]}
        admission = {'model': {'provider_id':'p','base_url':provider['base_url'],'model':'chosen'}}
        deadline = datetime.now(timezone.utc)+timedelta(minutes=2)
        with tempfile.TemporaryDirectory() as tmp, patch('model_gateway.model_gateway') as gateway:
            _admitted_gateway(admission, client, Path(tmp), deadline, ControlPlaneError)
            gateway.assert_called_once_with(provider,'chosen',deadline_at=deadline,socket_path=Path(tmp)/'model.sock')
            admission['model']['base_url']='https://changed.test/v1'
            with self.assertRaisesRegex(ControlPlaneError, 'admitted_provider_changed'):
                _admitted_gateway(admission, client, Path(tmp), deadline, ControlPlaneError)
            self.assertEqual(gateway.call_count,1)
