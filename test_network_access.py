import argparse
import io
import ipaddress
import socket
import unittest
from contextlib import redirect_stderr

from mockConsumerSingleSocket import parse_args as parse_single_consumer_args
from mockConsumerSockets import parse_args as parse_consumer_args
from network_access import (
    accept_from_allowed_network,
    create_tcp_listener,
    ipv4_network,
    tcp_port_range,
)


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeListener:
    def __init__(self, accepted):
        self.accepted = iter(accepted)

    def accept(self):
        return next(self.accepted)


class NetworkAccessTests(unittest.TestCase):
    def test_rejects_disallowed_peer_before_returning_allowed_peer(self):
        rejected = FakeConnection()
        accepted = FakeConnection()
        listener = FakeListener(
            [
                (rejected, ("203.0.113.7", 40001)),
                (accepted, ("192.0.2.23", 40002)),
            ]
        )

        connection, address = accept_from_allowed_network(
            listener, [ipaddress.ip_network("192.0.2.0/24")]
        )

        self.assertTrue(rejected.closed)
        self.assertFalse(accepted.closed)
        self.assertIs(connection, accepted)
        self.assertEqual(address, ("192.0.2.23", 40002))

    def test_accepts_multiple_ranges(self):
        connection = FakeConnection()
        listener = FakeListener([(connection, ("198.51.100.10", 40001))])

        returned, _ = accept_from_allowed_network(
            listener,
            [
                ipaddress.ip_network("192.0.2.0/24"),
                ipaddress.ip_network("198.51.100.0/24"),
            ],
        )

        self.assertIs(returned, connection)

    def test_parser_normalizes_host_bits_and_rejects_ipv6(self):
        self.assertEqual(
            ipv4_network("192.0.2.17/24"), ipaddress.ip_network("192.0.2.0/24")
        )
        with self.assertRaises(argparse.ArgumentTypeError):
            ipv4_network("2001:db8::/32")

    def test_consumers_require_an_allowed_range(self):
        for parse_args in (parse_single_consumer_args, parse_consumer_args):
            with self.subTest(parser=parse_args.__module__):
                with redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        parse_args(
                            [
                                "connection.json",
                                "output.bp",
                                "--public-key",
                                "public.key",
                            ]
                        )

    def test_port_parser_supports_start_and_bounded_range(self):
        self.assertEqual(tcp_port_range("5000"), (5000, 65535))
        self.assertEqual(tcp_port_range("5000-5009"), (5000, 5009))
        for invalid in ("0", "65536", "5001-5000", "abc", "1-2-3"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(argparse.ArgumentTypeError):
                    tcp_port_range(invalid)

    def test_consumer_port_defaults_to_none(self):
        args = parse_single_consumer_args(
            [
                "connection.json",
                "output.bp",
                "--public-key",
                "public.key",
                "--allow-ip-range",
                "127.0.0.1/32",
            ]
        )
        self.assertIsNone(args.port)

    def test_listener_defaults_to_an_ephemeral_port(self):
        listener = create_tcp_listener("127.0.0.1", None, backlog=1)
        try:
            self.assertGreater(listener.getsockname()[1], 0)
        finally:
            listener.close()

    def test_listener_increments_past_an_occupied_port(self):
        occupied = create_tcp_listener("127.0.0.1", None, backlog=1)
        start = occupied.getsockname()[1]
        if start == 65535:
            occupied.close()
            self.skipTest("ephemeral allocation returned the highest TCP port")
        listener = None
        try:
            listener = create_tcp_listener("127.0.0.1", (start, start + 1), 1)
            self.assertEqual(listener.getsockname()[1], start + 1)
        finally:
            if listener is not None:
                listener.close()
            occupied.close()

    def test_listener_port_can_be_reused_immediately(self):
        listener = create_tcp_listener("127.0.0.1", None, backlog=1)
        port = listener.getsockname()[1]
        client = socket.create_connection(("127.0.0.1", port))
        accepted, _ = listener.accept()
        accepted.close()
        client.close()
        listener.close()

        replacement = create_tcp_listener("127.0.0.1", (port, port), backlog=1)
        try:
            self.assertEqual(replacement.getsockname()[1], port)
        finally:
            replacement.close()


if __name__ == "__main__":
    unittest.main()
