"""Trusted, per-run public HTTP egress for validation sandboxes.

The candidate has no network namespace access.  It can reach only a loopback relay,
which forwards HTTP proxy requests over a private Unix socket to this parent-side
proxy.  The parent resolves every hostname, rejects any result that is not global,
and connects to the selected resolved address rather than resolving again.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import ipaddress
import os
from pathlib import Path
import re
import select
import socket
import socketserver
import tempfile
import threading
import time
from typing import Iterator
from urllib.parse import urlsplit

MAX_HEADER_BYTES = 16 * 1024
MAX_TUNNEL_BYTES = 32 * 1024 * 1024
MAX_CONNECTION_SECONDS = 300
RELAY_LISTEN_HOST = "127.0.0.1"
RELAY_LISTEN_PORT = 18081
_SOCKET_NAME = "public-egress.sock"
_HOST = re.compile(r"^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$", re.I)


class PublicEgressError(RuntimeError):
    pass


@dataclass(frozen=True)
class PublicEgressRelay:
    """The trusted files/arguments a validation executor mounts into bwrap."""

    socket_path: Path
    relay_path: Path
    proxy_url: str
    argv_prefix: tuple[str, ...]


def _public_address(value: str) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    address = ipaddress.ip_address(value)
    if not address.is_global or any((
        address.is_private,
        address.is_loopback,
        address.is_link_local,
        address.is_multicast,
        address.is_reserved,
        address.is_unspecified,
    )):
        raise PublicEgressError("egress destination is not public")
    return address


def _resolve_public(host: str, port: int) -> list[tuple[int, tuple]]:
    """Resolve once and reject the host if *any* answer is non-public."""
    if not isinstance(host, str) or not _HOST.fullmatch(host):
        raise PublicEgressError("egress hostname is invalid")
    if port not in {80, 443}:
        raise PublicEgressError("egress port is not allowed")
    try:
        answers = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise PublicEgressError("egress hostname cannot be resolved") from exc
    values: list[tuple[int, tuple]] = []
    seen: set[tuple[int, str]] = set()
    for family, socktype, protocol, _name, sockaddr in answers:
        if family not in {socket.AF_INET, socket.AF_INET6} or socktype != socket.SOCK_STREAM:
            raise PublicEgressError("egress hostname has an unsupported address")
        address = _public_address(sockaddr[0])
        key = (family, str(address))
        if key not in seen:
            seen.add(key)
            values.append((family, sockaddr))
    if not values:
        raise PublicEgressError("egress hostname has no usable public address")
    return values


def _connect_public(host: str, port: int, deadline: float) -> socket.socket:
    answers = _resolve_public(host, port)
    last_error: OSError | None = None
    for family, sockaddr in answers:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        peer = socket.socket(family, socket.SOCK_STREAM)
        peer.settimeout(min(15, remaining))
        try:
            peer.connect(sockaddr)  # sockaddr is the resolved IP: never re-resolve host.
            peer.settimeout(None)
            return peer
        except OSError as exc:
            last_error = exc
            peer.close()
    raise PublicEgressError("egress public connection failed") from last_error


def _read_head(connection: socket.socket) -> tuple[bytes, bytes]:
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = connection.recv(min(4096, MAX_HEADER_BYTES + 1 - len(data)))
        if not chunk:
            raise PublicEgressError("egress proxy request is incomplete")
        data.extend(chunk)
        if len(data) > MAX_HEADER_BYTES:
            raise PublicEgressError("egress proxy request headers exceed bound")
    head, remainder = bytes(data).split(b"\r\n\r\n", 1)
    return head, remainder


def _request_line(head: bytes) -> tuple[str, str, str, list[str]]:
    try:
        lines = head.decode("iso-8859-1").split("\r\n")
        method, target, version = lines[0].split(" ")
    except (UnicodeDecodeError, ValueError, IndexError) as exc:
        raise PublicEgressError("egress proxy request line is invalid") from exc
    if version not in {"HTTP/1.0", "HTTP/1.1"} or not method.isupper() or not target:
        raise PublicEgressError("egress proxy request line is invalid")
    if any("\x00" in line or len(line) > 4096 for line in lines[1:]):
        raise PublicEgressError("egress proxy request headers are invalid")
    return method, target, version, lines[1:]


def _connect_target(target: str) -> tuple[str, int]:
    if target.count(":") != 1:
        raise PublicEgressError("egress CONNECT target is invalid")
    host, value = target.rsplit(":", 1)
    try:
        port = int(value)
    except ValueError as exc:
        raise PublicEgressError("egress CONNECT target is invalid") from exc
    if port != 443 or not _HOST.fullmatch(host):
        raise PublicEgressError("egress CONNECT target is invalid")
    return host.lower(), port

def _http_target(method: str, target: str) -> tuple[str, int, str]:
    if method not in {"GET", "HEAD"}:
        raise PublicEgressError("egress HTTP method is not allowed")
    parsed = urlsplit(target)
    try:
        port = parsed.port
    except ValueError as exc:
        raise PublicEgressError("egress HTTP target is invalid") from exc
    if (parsed.scheme != "http" or not parsed.hostname or not _HOST.fullmatch(parsed.hostname)
            or parsed.username or parsed.password or parsed.fragment or port not in {None, 80}):
        raise PublicEgressError("egress HTTP target is invalid")
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    return parsed.hostname.lower(), 80, path


def _copy_bidirectional(left: socket.socket, right: socket.socket, deadline: float, initial: bytes = b"") -> None:
    total = 0
    if initial:
        right.sendall(initial)
        total += len(initial)
    sockets = [left, right]
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PublicEgressError("egress connection timed out")
        ready, _, _ = select.select(sockets, [], [], min(1, remaining))
        if not ready:
            continue
        for source in ready:
            data = source.recv(min(65536, MAX_TUNNEL_BYTES + 1 - total))
            if not data:
                return
            total += len(data)
            if total > MAX_TUNNEL_BYTES:
                raise PublicEgressError("egress connection exceeds byte bound")
            (right if source is left else left).sendall(data)


def _forward_http(client: socket.socket, upstream: socket.socket, method: str, target: str, headers: list[str], deadline: float) -> None:
    host, _port, path = _http_target(method, target)
    filtered = [line for line in headers if not line.lower().startswith(("host:", "connection:", "proxy-", "content-length:", "transfer-encoding:"))]
    payload = (f"{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n"
               + "\r\n".join(filtered) + "\r\n\r\n").encode("iso-8859-1")
    upstream.sendall(payload)
    _copy_bidirectional(upstream, client, deadline)


class _ProxyHandler(socketserver.BaseRequestHandler):
    timeout_seconds = MAX_CONNECTION_SECONDS

    def handle(self) -> None:
        self.request.settimeout(self.timeout_seconds)
        deadline = time.monotonic() + self.timeout_seconds
        try:
            head, remainder = _read_head(self.request)
            method, target, _version, headers = _request_line(head)
            if method == "CONNECT":
                host, port = _connect_target(target)
                with _connect_public(host, port, deadline) as upstream:
                    self.request.sendall(b"HTTP/1.1 200 Connection Established\r\nConnection: close\r\n\r\n")
                    _copy_bidirectional(self.request, upstream, deadline, remainder)
            else:
                if remainder:
                    raise PublicEgressError("egress HTTP request body is not allowed")
                host, port, _path = _http_target(method, target)
                with _connect_public(host, port, deadline) as upstream:
                    _forward_http(self.request, upstream, method, target, headers, deadline)
        except (OSError, PublicEgressError):
            try:
                self.request.sendall(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            except OSError:
                pass


class _UnixProxy(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    block_on_close = False


@contextmanager
def public_egress(candidate_root: Path, timeout_seconds: int) -> Iterator[PublicEgressRelay]:
    """Run a private parent-side proxy for one candidate validation.

    ``candidate_root`` is accepted so callers bind proxy lifetime to the exact
    candidate materialization; no candidate path is ever opened by this service.
    """
    if not isinstance(candidate_root, Path) or not candidate_root.is_dir():
        raise PublicEgressError("candidate root is unavailable")
    if not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= 300:
        raise PublicEgressError("egress timeout is outside bounds")
    with tempfile.TemporaryDirectory(prefix="validation-public-egress-") as directory:
        root = Path(directory)
        root.chmod(0o700)
        socket_path = root / _SOCKET_NAME
        handler = type("BoundedProxyHandler", (_ProxyHandler,), {"timeout_seconds": timeout_seconds})
        server = _UnixProxy(str(socket_path), handler)
        socket_path.chmod(0o600)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
        thread.start()
        try:
            yield PublicEgressRelay(
                socket_path=socket_path,
                relay_path=Path(__file__).resolve(),
                proxy_url=f"http://{RELAY_LISTEN_HOST}:{RELAY_LISTEN_PORT}",
                argv_prefix=(
                    "/usr/bin/python3", "/opt/validation-relay.py", "relay",
                    "--socket", "/public-egress.sock", "--",
                ),
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class _RelayHandler(socketserver.BaseRequestHandler):
    socket_path: str

    def handle(self) -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as peer:
            peer.settimeout(MAX_CONNECTION_SECONDS)
            peer.connect(self.socket_path)
            self.request.settimeout(MAX_CONNECTION_SECONDS)
            _copy_bidirectional(self.request, peer, time.monotonic() + MAX_CONNECTION_SECONDS)


class _LoopbackRelay(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    block_on_close = False


def relay(socket_path: str, command: list[str]) -> int:
    """Run an in-namespace loopback proxy around one fixed validation command."""
    if not isinstance(socket_path, str) or not socket_path.startswith("/"):
        raise PublicEgressError("relay socket path is invalid")
    if not command:
        raise PublicEgressError("relay command is missing")
    server = _LoopbackRelay((RELAY_LISTEN_HOST, RELAY_LISTEN_PORT), _RelayHandler)
    server.RequestHandlerClass.socket_path = socket_path
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
    thread.start()
    try:
        return os.spawnv(os.P_WAIT, command[0], command)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    relay_parser = commands.add_parser("relay")
    relay_parser.add_argument("--socket", required=True)
    relay_parser.add_argument("remainder", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = list(args.remainder)
    if command[:1] == ["--"]:
        command.pop(0)
    try:
        return relay(args.socket, command)
    except (OSError, PublicEgressError):
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
