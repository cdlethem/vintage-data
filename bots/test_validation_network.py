from __future__ import annotations

import socket
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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


class PublicEgressLifecycleTest(unittest.TestCase):
    def test_context_creates_private_socket_and_static_relay_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate"
            candidate.mkdir()
            with network.public_egress(candidate, 60) as relay:
                self.assertTrue(relay.socket_path.is_socket())
                self.assertEqual(0o600, stat.S_IMODE(relay.socket_path.stat().st_mode))
                self.assertEqual("http://127.0.0.1:18081", relay.proxy_url)
                self.assertEqual(("/usr/bin/python3", "/opt/validation-relay.py", "relay", "--socket", "/public-egress.sock", "--"), relay.argv_prefix)
                self.assertEqual(Path(network.__file__).resolve(), relay.relay_path)
            self.assertFalse(relay.socket_path.exists())


if __name__ == "__main__":
    unittest.main()
