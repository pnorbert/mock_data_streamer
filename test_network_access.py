import argparse
import io
import ipaddress
import unittest
from contextlib import redirect_stderr

from mockConsumerSingleSocket import parse_args as parse_single_consumer_args
from mockConsumerSockets import parse_args as parse_consumer_args
from network_access import accept_from_allowed_network, ipv4_network


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


if __name__ == "__main__":
    unittest.main()
