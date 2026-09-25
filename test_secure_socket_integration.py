import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import nacl.public


ROOT = Path(__file__).resolve().parent


class SecureSocketIntegrationTests(unittest.TestCase):
    def run_case(self, directory, consumer_script):
        private_key_file = directory / f"{consumer_script}-private.key"
        public_key_file = directory / f"{consumer_script}-public.key"
        private_key = nacl.public.PrivateKey.generate()
        private_key_file.write_bytes(bytes(private_key))
        public_key_file.write_bytes(bytes(private_key.public_key))
        connection_file = directory / f"{consumer_script}-connection.json"
        consumer_output = directory / f"{consumer_script}-received.bp"
        consumer_log = directory / f"{consumer_script}-consumer.jsonl"
        producer_log = directory / f"{consumer_script}-producer.jsonl"
        consumer = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / consumer_script),
                str(connection_file),
                str(consumer_output),
                "--public-key",
                str(public_key_file),
                "--timing-log",
                str(consumer_log),
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        consumer_text = ""
        try:
            deadline = time.monotonic() + 10
            while not connection_file.exists():
                if consumer.poll() is not None:
                    consumer_text = consumer.communicate()[0]
                    self.fail(f"consumer exited before rendezvous:\n{consumer_text}")
                if time.monotonic() >= deadline:
                    self.fail("consumer did not create its encrypted connection file")
                time.sleep(0.02)

            serialized = connection_file.read_text(encoding="utf-8")
            envelope = json.loads(serialized)
            self.assertEqual(envelope["format"], "mockapp-curve25519-sealed-box-v1")
            self.assertNotIn("127.0.0.1", serialized)
            self.assertNotIn("consumer_id", serialized)

            producer = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "mockProducer.py"),
                    str(connection_file),
                    "4",
                    "4",
                    "1",
                    "--private-key",
                    str(private_key_file),
                    "--timing-log",
                    str(producer_log),
                ],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=15,
            )
            consumer_text = consumer.communicate(timeout=15)[0]
        finally:
            if consumer.poll() is None:
                consumer.terminate()
                try:
                    consumer_text += consumer.communicate(timeout=2)[0]
                except subprocess.TimeoutExpired:
                    consumer.kill()
                    consumer_text += consumer.communicate()[0]

        self.assertEqual(producer.returncode, 0, producer.stdout)
        self.assertEqual(consumer.returncode, 0, consumer_text)
        self.assertIn("Received and wrote step 0", consumer_text)
        self.assertFalse(connection_file.exists())

    def test_key_pair_secures_single_and_three_socket_transfers(self):
        with tempfile.TemporaryDirectory(prefix="mockapp-security-test-") as temp:
            directory = Path(temp)
            for consumer_script in (
                "mockConsumerSingleSocket.py",
                "mockConsumerSockets.py",
            ):
                with self.subTest(consumer=consumer_script):
                    self.run_case(directory, consumer_script)


if __name__ == "__main__":
    unittest.main()
