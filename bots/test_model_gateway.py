import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from bots.model_gateway import model_gateway, write_omp_config


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.upstream_status = 200
        outer = self
        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                outer.requests.append((self.path, dict(self.headers), body))
                self.send_response(outer.upstream_status)
                self.send_header('Content-Type', 'text/event-stream' if body.get('stream') else 'application/json')
                self.end_headers()
                if outer.upstream_status != 200:
                    self.wfile.write(b'private-upstream-key must never be returned')
                elif body.get('stream'):
                    self.wfile.write(b'data: {"choices":[{"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n')
                    self.wfile.flush()
                else:
                    self.wfile.write(b'{"choices":[]}')
        self.upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        self.thread = threading.Thread(target=self.upstream.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.upstream.server_close)
        self.addCleanup(self.upstream.shutdown)
        self.provider = {'id': 'test', 'base_url': f'http://127.0.0.1:{self.upstream.server_port}/v1',
                         'api_key': 'private-upstream-key'}

    def request(self, gateway, method='POST', path='/v1/chat/completions', body=None):
        address = urlsplit(gateway.base_url)
        connection = http.client.HTTPConnection(address.hostname, address.port, timeout=3)
        self.addCleanup(connection.close)
        connection.request(method, path, body=json.dumps(body or {'model': 'provider/assigned'}),
                           headers={'Authorization': 'Bearer caller-untrusted-key'})
        response = connection.getresponse()
        return response.status, response.read()

    def test_model_and_routes_pinned_and_only_host_auth_forwarded(self):
        with model_gateway(self.provider, 'provider/assigned', deadline_at=time.time()+10) as gateway:
            self.assertEqual(self.request(gateway, path='/v1/other')[0], 404)
            self.assertEqual(self.request(gateway, body={'model': 'other'})[0], 403)
            self.assertEqual(self.request(gateway, method='DELETE')[0], 405)
            self.assertEqual(self.request(gateway)[0], 200)
        self.assertEqual(len(self.requests), 1)
        path, headers, body = self.requests[0]
        self.assertEqual(path, '/v1/chat/completions')
        self.assertEqual(headers['Authorization'], 'Bearer private-upstream-key')
        self.assertEqual(body['model'], 'provider/assigned')

    def test_streaming_and_static_models(self):
        with model_gateway(self.provider, 'provider/assigned', deadline_at=time.time()+10) as gateway:
            status, body = self.request(gateway, body={'model': 'provider/assigned', 'stream': True, 'tools': []})
            self.assertEqual(status, 200)
            self.assertIn(b'data: [DONE]', body)
            status, body = self.request(gateway, method='GET', path='/v1/models')
            self.assertEqual(json.loads(body)['data'][0]['id'], 'provider/assigned')
        self.assertEqual(len(self.requests), 1)

    def test_upstream_errors_and_redirects_never_return_credentials(self):
        with model_gateway(self.provider, 'provider/assigned', deadline_at=time.time()+10) as gateway:
            for status in (401, 302, 500):
                self.upstream_status = status
                returned_status, body = self.request(gateway)
                self.assertEqual(returned_status, 502)
                self.assertNotIn(b'private-upstream-key', body)
        self.assertEqual(len(self.requests), 3)

    def test_rate_limit_is_actionable_redacted_and_cleared_after_recovery(self):
        with model_gateway(self.provider, 'provider/assigned', deadline_at=time.time()+10) as gateway:
            self.upstream_status = 429
            status, body = self.request(gateway)
            self.assertEqual(429, status)
            self.assertEqual('model_rate_limited', json.loads(body)['error']['code'])
            self.assertNotIn(b'private-upstream-key', body)
            self.assertEqual('model_rate_limited', gateway.telemetry['last_error'])
            self.assertEqual(60, gateway.telemetry['retry_after_seconds'])
            self.upstream_status = 200
            self.assertEqual(200, self.request(gateway)[0])
            self.assertIsNone(gateway.telemetry.get('last_error'))

    def test_expired_deadline_does_not_call_upstream(self):
        with model_gateway(self.provider, 'provider/assigned', deadline_at=time.time()-1) as gateway:
            self.assertEqual(self.request(gateway)[0], 408)
        self.assertFalse(self.requests)

    def test_request_and_response_sizes_bounded(self):
        with model_gateway(self.provider, 'provider/assigned', deadline_at=time.time()+10) as gateway:
            with patch('bots.model_gateway.MAX_REQUEST_BYTES', 8):
                self.assertEqual(self.request(gateway)[0], 413)
            self.assertFalse(self.requests)
            with patch('bots.model_gateway.MAX_RESPONSE_BYTES', 5):
                _, body = self.request(gateway)
                self.assertLessEqual(len(body), 5)

    def test_unix_socket_private_and_removed_and_config_has_no_credential(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'model.sock'
            with model_gateway(self.provider, 'provider/assigned', deadline_at=time.time()+10, socket_path=path):
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                connection = socket.socket(socket.AF_UNIX)
                connection.settimeout(3)
                connection.connect(str(path))
                connection.sendall(b'GET /v1/models HTTP/1.0\r\nHost: local\r\n\r\n')
                self.assertIn(b'200', connection.recv(4096))
                connection.close()
            self.assertFalse(path.exists())
            root = write_omp_config(Path(temp) / 'home', 'http://127.0.0.1:18080/v1', 'provider/assigned')
            config = json.loads((root / 'config.yml').read_text())
            self.assertEqual(config['modelRoles']['task'], 'gateway/provider/assigned')
            for path in root.iterdir():
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                self.assertNotIn('private-upstream-key', path.read_text())


if __name__ == '__main__':
    unittest.main()
