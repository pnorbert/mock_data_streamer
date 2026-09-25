import io
import json
import socket
import threading
import unittest
from contextlib import redirect_stderr
from pathlib import Path

import nacl.public

from connection_security import (
    CONNECTION_FILE_FORMAT,
    decrypt_connection_info,
    encrypt_connection_info,
)
from mockConsumerSingleSocket import parse_args as parse_single_consumer_args
from mockConsumerSockets import parse_args as parse_consumer_args
from mockProducer import Settings
from socket_protocol import prove_private_key, verify_producer_private_key


ROOT = Path(__file__).resolve().parent


class ConnectionSecurityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.private_key = nacl.public.PrivateKey.generate()
        cls.public_key = cls.private_key.public_key

    def test_encrypt_and_decrypt_connection_information(self):
        connection_info = {
            "id": "singlesocket",
            "consumer_id": "secret-session",
            "host": "private.example.org",
            "port": 12345,
        }

        serialized = encrypt_connection_info(connection_info, self.public_key)
        envelope = json.loads(serialized)

        self.assertEqual(envelope["format"], CONNECTION_FILE_FORMAT)
        self.assertNotIn("private.example.org", serialized)
        self.assertNotIn("secret-session", serialized)
        self.assertEqual(
            decrypt_connection_info(envelope, self.private_key), connection_info
        )

    def test_wrong_private_key_cannot_decrypt_connection_information(self):
        serialized = encrypt_connection_info({"id": "singlesocket"}, self.public_key)

        with self.assertRaisesRegex(ValueError, "Unable to decrypt"):
            decrypt_connection_info(
                json.loads(serialized), nacl.public.PrivateKey.generate()
            )

    def test_matching_keys_complete_private_key_challenge(self):
        consumer, producer = socket.socketpair()
        producer_error = []

        def answer_challenge():
            try:
                prove_private_key(producer, self.private_key)
            except Exception as exc:
                producer_error.append(exc)
            finally:
                producer.close()

        thread = threading.Thread(target=answer_challenge)
        thread.start()
        try:
            verify_producer_private_key(consumer, self.public_key)
        finally:
            consumer.close()
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(producer_error, [])

    def test_wrong_private_key_fails_challenge(self):
        consumer, producer = socket.socketpair()

        def answer_challenge():
            try:
                prove_private_key(producer, nacl.public.PrivateKey.generate())
            except RuntimeError:
                pass
            finally:
                producer.close()

        thread = threading.Thread(target=answer_challenge)
        thread.start()
        try:
            with self.assertRaises((EOFError, ConnectionResetError)):
                verify_producer_private_key(consumer, self.public_key)
        finally:
            consumer.close()
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())

    def test_key_arguments_have_no_defaults(self):
        with self.assertRaisesRegex(ValueError, "Missing required --private-key"):
            Settings(["mockProducer.py", "output.bp", "4", "4", "1"])
        for parse_args in (parse_single_consumer_args, parse_consumer_args):
            with self.subTest(parser=parse_args.__module__):
                with redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        parse_args(["connection.json", "output.bp"])


if __name__ == "__main__":
    unittest.main()
