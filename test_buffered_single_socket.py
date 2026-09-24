import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


RUN_INTEGRATION = os.environ.get("MOCKAPP_RUN_SOCKET_INTEGRATION") == "1"
ROOT = Path(__file__).resolve().parent


def read_json_lines(path):
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


@unittest.skipUnless(
    RUN_INTEGRATION,
    "set MOCKAPP_RUN_SOCKET_INTEGRATION=1 to run socket integration tests",
)
class BufferedSingleSocketIntegrationTests(unittest.TestCase):
    def run_case(self, directory, name, buffer_seconds, expected_drop_count):
        connection_file = directory / f"{name}-connection.json"
        consumer_output = directory / f"{name}-received.bp"
        consumer_log = directory / f"{name}-consumer.jsonl"
        producer_log = directory / f"{name}-producer.jsonl"

        consumer_command = [
            sys.executable,
            str(ROOT / "mockConsumerSingleSocketBlocking.py"),
            str(connection_file),
            str(consumer_output),
            "--timing-log",
            str(consumer_log),
            "--block-min-seconds",
            "7",
            "--block-max-seconds",
            "8",
            "--max-blocks",
            "1",
            "--random-seed",
            "20260924",
        ]
        consumer = subprocess.Popen(
            consumer_command,
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
                    self.fail("consumer did not create its connection file")
                time.sleep(0.05)

            producer = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "mockProducer.py"),
                    str(connection_file),
                    "512",
                    "512",
                    "5",
                    "--buffer-seconds",
                    str(buffer_seconds),
                    "--timing-log",
                    str(producer_log),
                ],
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=45,
            )
            consumer_text = consumer.communicate(timeout=30)[0]
        finally:
            if consumer.poll() is None:
                consumer.terminate()
                try:
                    remaining_text = consumer.communicate(timeout=5)[0]
                except subprocess.TimeoutExpired:
                    consumer.kill()
                    remaining_text = consumer.communicate()[0]
                consumer_text += remaining_text

        self.assertEqual(producer.returncode, 0, producer.stdout)
        self.assertEqual(consumer.returncode, 0, consumer_text)

        producer_records = read_json_lines(producer_log)
        consumer_records = read_json_lines(consumer_log)
        drops = [
            record
            for record in producer_records
            if record["event"] == "io.buffer_drop"
        ]
        receives = [
            record
            for record in consumer_records
            if record["event"] == "socket.receive_all"
        ]
        blocks = [
            record
            for record in consumer_records
            if record["event"] == "socket.read_block"
        ]

        self.assertEqual(len(blocks), 1)
        self.assertGreaterEqual(blocks[0]["requested_seconds"], 7)
        self.assertLessEqual(blocks[0]["requested_seconds"], 8)
        self.assertEqual(len(drops), expected_drop_count, producer.stdout)
        if expected_drop_count:
            self.assertIn("Dropped output steps", producer.stdout)
            self.assertTrue(
                all(
                    isinstance(record["dropped_step"], int)
                    and isinstance(record["incoming_step"], int)
                    and record["dropped_step"] < record["incoming_step"]
                    and record["buffer_capacity_steps"] == 1
                    for record in drops
                ),
                drops,
            )
        else:
            self.assertEqual(drops, [], producer.stdout)
            self.assertNotIn("Dropped output steps", producer.stdout)
        self.assertEqual(len(receives), 5 - len(drops))

    def test_long_and_short_buffers_during_random_read_stall(self):
        with tempfile.TemporaryDirectory(prefix="mockapp-buffer-test-") as temp:
            directory = Path(temp)
            with self.subTest("long buffer preserves every step"):
                self.run_case(
                    directory, "long", buffer_seconds=9, expected_drop_count=0
                )
            with self.subTest("short buffer drops selected steps"):
                self.run_case(
                    directory, "short", buffer_seconds=3, expected_drop_count=1
                )


if __name__ == "__main__":
    unittest.main()
