from __future__ import annotations

import socket
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import validation_network as network


class PublicEgressResolutionTest(unittest.TestCase):
    def test_resolution_rejects_a_mixed_public_and_loopback_answer_set(self):
        answers = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)),
        ]
        with patch.object(network.socket, "getaddrinfo", return_value=answers):
            with self.assertRaisesRegex(network.PublicEgressError, "not public"):
                network._resolve_public("example.com", 443)

    def test_resolution_keeps_resolved_ip_socket_addresses(self):
        answers = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 443))]
        with patch.object(network.socket, "getaddrinfo", return_value=answers):
            self.assertEqual([(socket.AF_INET, ("1.1.1.1", 443))], network._resolve_public("example.com", 443))

    def test_only_https_connect_and_plain_http_are_admitted(self):
        self.assertEqual(("example.com", 443), network._connect_target("example.com:443"))
        with self.assertRaises(network.PublicEgressError):
            network._connect_target("127.0.0.1:443")
        with self.assertRaises(network.PublicEgressError):
            network._connect_target("example.com:80")
        self.assertEqual(("example.com", 80, "/a?q=1"), network._http_target("GET", "http://example.com/a?q=1"))
        with self.assertRaises(network.PublicEgressError):
            network._http_target("POST", "http://example.com/")
        with self.assertRaises(network.PublicEgressError):
            network._http_target("GET", "https://example.com/")

    def test_only_catalogue_hosts_are_allowed_for_public_egress(self):
        allowed_hosts = network._normalize_allowed_hosts({"Known.Example"})
        self.assertEqual(frozenset({"known.example"}), allowed_hosts)
        network._require_allowed_host("known.example", allowed_hosts)
        with self.assertRaisesRegex(network.PublicEgressError, "not allowed"):
            network._require_allowed_host("other.example", allowed_hosts)
        with self.assertRaises(network.PublicEgressError):
            network._normalize_allowed_hosts(set())

    def test_tunnel_treats_reset_or_broken_pipe_as_peer_closure(self):
        left = Mock()
        right = Mock()
        with patch.object(network.select, "select", return_value=([left], [], [])):
            left.recv.side_effect = ConnectionResetError()
            network._copy_bidirectional(left, right, deadline=network.time.monotonic() + 1)
        right.sendall.assert_not_called()

        left.recv.side_effect = None
        left.recv.return_value = b"payload"
        right.sendall.side_effect = BrokenPipeError()
        with patch.object(network.select, "select", return_value=([left], [], [])):
            network._copy_bidirectional(left, right, deadline=network.time.monotonic() + 1)


class PublicEgressLifecycleTest(unittest.TestCase):
    def test_proxy_socket_is_private_and_removed_after_use(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate"
            candidate.mkdir()
            with network.public_egress(candidate, 60, allowed_hosts={"example.com"}) as relay:
                self.assertTrue(relay.socket_path.is_socket())
                self.assertEqual(0o600, stat.S_IMODE(relay.socket_path.stat().st_mode))
            self.assertFalse(relay.socket_path.exists())

    def test_proxy_closes_connections_when_active_connection_limit_is_reached(self):
        with tempfile.TemporaryDirectory() as directory:
            socket_path = str(Path(directory) / "proxy.sock")
            server = network._UnixProxy(socket_path, network._ProxyHandler)
            held_slots = []
            client = peer = None
            try:
                for _ in range(network.MAX_PROXY_CONNECTIONS):
                    self.assertTrue(server._connection_slots.acquire(blocking=False))
                    held_slots.append(None)
                client, peer = socket.socketpair()
                server.process_request(client, "")
                self.assertEqual(-1, client.fileno())
            finally:
                if peer is not None:
                    peer.close()
                for _ in held_slots:
                    server._connection_slots.release()
                server.server_close()


if __name__ == "__main__":
    unittest.main()
