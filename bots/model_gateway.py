"""Per-run model gateway: provider credentials remain outside agent processes."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import socketserver
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_RESPONSE_BYTES = 64 * 1024 * 1024


class ModelRateLimited(RuntimeError):
    code = "model_rate_limited"
    retry_class = "capacity"

    def __init__(self):
        super().__init__("The selected model is rate limited. A bounded retry will use the current model assignment. Choose another model in Models & connections if the limit persists.")


class UnixHTTPServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    block_on_close = False


def _deadline(value) -> float:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
    return float(value)


def _handler(provider: dict, model: str, deadline: float, telemetry: dict):
    upstream = urlsplit(provider['base_url'])
    if upstream.scheme not in ('http', 'https') or not upstream.hostname or upstream.username or upstream.password or upstream.query or upstream.fragment:
        raise ValueError('invalid_model_provider_url')
    upstream_path = upstream.path.rstrip('/') + '/chat/completions'
    api_key = provider.get('api_key') or ''
    if not isinstance(api_key, str) or '\r' in api_key or '\n' in api_key:
        raise ValueError('invalid_model_provider_key')

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def setup(self):
            super().setup()
            self.connection.settimeout(max(.1, min(15, deadline - time.time())))

        def log_message(self, *args):
            pass  # Requests and upstream exceptions must never enter credential-bearing logs.

        def reply(self, status: int, value: dict, retry_after=None):
            body = json.dumps(value, separators=(',', ':')).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Connection', 'close')
            if retry_after is not None:
                self.send_header('Retry-After', str(retry_after))
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

        def fail(self, status=502):
            self.reply(status, {'error': {'message': 'Model gateway request rejected or unavailable.'}})

        def do_GET(self):
            if self.path != '/v1/models':
                return self.fail(404)
            if time.time() >= deadline:
                return self.fail(408)
            self.reply(200, {'object': 'list', 'data': [{'id': model, 'object': 'model', 'owned_by': 'gateway'}]})

        def do_POST(self):
            if self.path != '/v1/chat/completions':
                return self.fail(404)
            if time.time() >= deadline:
                return self.fail(408)
            # Chunked or ambiguous framing is not accepted.
            lengths = self.headers.get_all('Content-Length', [])
            if self.headers.get('Transfer-Encoding') or len(lengths) != 1:
                return self.fail(400)
            try:
                length = int(lengths[0])
                if not 1 <= length <= MAX_REQUEST_BYTES:
                    return self.fail(413)
                raw = self.rfile.read(length)
                if len(raw) != length:
                    return self.fail(400)
                body = json.loads(raw)
            except (ValueError, OSError):
                return self.fail(400)
            if not isinstance(body, dict) or body.get('model') != model:
                return self.fail(403)
            # Tool definitions are data; tools are executed by the confined caller.
            # Pin the model and fixed upstream route; never proxy client headers.
            connection_type = http.client.HTTPSConnection if upstream.scheme == 'https' else http.client.HTTPConnection
            remaining = deadline - time.time()
            if remaining <= 0:
                return self.fail(408)
            connection = connection_type(upstream.hostname, upstream.port, timeout=min(120, remaining))
            sent_headers = False
            try:
                headers = {'Content-Type': 'application/json', 'Accept': 'text/event-stream' if body.get('stream') else 'application/json'}
                if api_key:
                    headers['Authorization'] = 'Bearer ' + api_key
                connection.request('POST', upstream_path, body=raw, headers=headers)
                transport_socket = connection.sock
                response = connection.getresponse()
                # Redirects are never followed; upstream errors may echo credentials.
                if response.status == 429:
                    try:
                        retry_after = max(1, min(900, int(response.getheader('Retry-After', '60'))))
                    except ValueError:
                        retry_after = 60
                    telemetry.update(last_error='model_rate_limited', retry_after_seconds=retry_after)
                    return self.reply(429, {'error': {'code': 'model_rate_limited', 'message': 'The selected model is rate limited. Choose another model in Models & connections or retry later.'}}, retry_after)
                if response.status != 200:
                    telemetry.update(last_error=None)
                    return self.fail(502)
                telemetry.update(last_error=None)
                content_type = response.getheader('Content-Type', '').split(';')[0].strip().lower()
                expected_type = 'text/event-stream' if body.get('stream') else 'application/json'
                if content_type != expected_type:
                    return self.fail(502)
                self.send_response(200)
                self.send_header('Content-Type', expected_type)
                self.send_header('Cache-Control', 'no-store')
                self.send_header('Connection', 'close')
                self.end_headers()
                sent_headers = True
                self.close_connection = True
                total = 0
                while True:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    if transport_socket is not None:
                        transport_socket.settimeout(min(120, remaining))
                    chunk = response.read1(min(65536, MAX_RESPONSE_BYTES - total + 1))
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_RESPONSE_BYTES:
                        break
                    self.wfile.write(chunk)
                    self.wfile.flush()
            except (OSError, http.client.HTTPException, ValueError):
                if not sent_headers:
                    self.fail(502)
            finally:
                connection.close()
                self.close_connection = True

        do_PUT = do_DELETE = do_PATCH = do_OPTIONS = do_HEAD = lambda self: self.fail(405)

    return Handler


@contextmanager
def model_gateway(provider: dict, model: str, *, deadline_at, socket_path: Path | str | None = None):
    """Yield {base_url, socket_path}; None socket starts a loopback-only TCP server."""
    if not isinstance(model, str) or not model or len(model) > 200:
        raise ValueError('invalid_gateway_model')
    telemetry = {"last_error": None}
    handler = _handler(provider, model, _deadline(deadline_at), telemetry)
    path = Path(socket_path) if socket_path is not None else None
    if path is not None:
        # Never replace an existing socket or symlink.
        server = UnixHTTPServer(str(path), handler)
        path.chmod(0o600)
        base_url = 'http://127.0.0.1:18080/v1'  # Sandbox relay binds this address.
    else:
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        server.daemon_threads = True
        base_url = f'http://127.0.0.1:{server.server_port}/v1'
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .1}, daemon=True)
    thread.start()
    try:
        yield SimpleNamespace(base_url=base_url, socket_path=path, telemetry=telemetry)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        if path is not None:
            path.unlink(missing_ok=True)


def write_omp_config(home: Path | str, local_base_url: str, model: str) -> Path:
    """Write JSON (a YAML subset) config containing only a public placeholder key."""
    root = Path(home) / '.omp' / 'agent'
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    provider = {'baseUrl': local_base_url, 'api': 'openai-completions', 'apiKey': 'gateway-placeholder',
                'models': [{'id': model, 'name': model, 'contextWindow': 131072, 'maxTokens': 16384,
                            'reasoning': True, 'input': ['text']}]}
    values = {'models.yml': {'providers': {'gateway': provider}},
              'config.yml': {'modelRoles': {role: f'gateway/{model}' for role in ('task', 'default', 'plan', 'smol', 'tiny')},
                             'retry': {'enabled': False}, 'tools': {'approvalMode': 'yolo'}}}
    for name, value in values.items():
        descriptor = os.open(root / name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'w') as handle:
            os.fchmod(handle.fileno(), 0o600)
            json.dump(value, handle)
    return root
