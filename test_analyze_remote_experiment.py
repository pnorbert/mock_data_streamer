import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from analyze_remote_experiment import (
    parse_producer_stdout,
    render_report,
    summarize,
)


class AnalyzeRemoteExperimentTests(unittest.TestCase):
    def test_parses_producer_header(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "producer.stdout.log"
            path.write_text(
                "\n".join(
                    (
                        "Array size             : 512 x 256",
                        "Number of output steps : 100",
                        "Output interval        : 3 seconds",
                        "Output buffer          : 600 seconds",
                        "Using engine           : BP5",
                    )
                ),
                encoding="utf-8",
            )

            settings = parse_producer_stdout(path)

        self.assertEqual(settings["nx"], 512)
        self.assertEqual(settings["ny"], 256)
        self.assertEqual(settings["total_steps"], 100)
        self.assertEqual(settings["interval_seconds"], 3.0)
        self.assertEqual(settings["buffer_seconds"], 600.0)
        self.assertEqual(settings["engine"], "BP5")

    def test_summarizes_and_renders_markdown(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        producer = []
        consumer = []
        for step in range(3):
            called = start + timedelta(seconds=3 * step)
            producer.append(
                {
                    "event": "io.write",
                    "step": step,
                    "called_at": called.isoformat(),
                    "duration_seconds": 0.001,
                    "status": "ok",
                }
            )
            consumer.append(
                {
                    "event": "socket.receive_all",
                    "step": step,
                    "duration_seconds": 2.9,
                }
            )
            consumer.append(
                {
                    "event": "io.write",
                    "step": step,
                    "called_at": (called + timedelta(seconds=0.5)).isoformat(),
                    "duration_seconds": 0.01,
                    "status": "ok",
                }
            )
        settings = {
            "nx": 2,
            "ny": 3,
            "total_steps": 4,
            "interval_seconds": 3.0,
            "buffer_seconds": 6.0,
            "engine": "BP5",
        }
        summary = summarize(producer, consumer, settings)
        config = SimpleNamespace(
            host="user@example",
            timing_log="consumer.jsonl",
        )
        remote = {
            "hostname": "remote.example",
            "disk_kib": 1,
            "segments": ["one.bp"],
        }

        report = render_report(
            config,
            Path("server.conf"),
            Path("producer.jsonl"),
            Path("producer.stdout.log"),
            settings,
            summary,
            remote,
        )

        self.assertEqual(summary["produced_steps"], 3)
        self.assertEqual(summary["consumed_steps"], 3)
        self.assertEqual(summary["remaining_steps"], 1)
        self.assertAlmostEqual(summary["latency"]["median"], 0.51)
        self.assertIn("The experiment is **healthy**", report)
        self.assertIn("3 / 4 (75.00%)", report)
        self.assertIn("one multiplexed SSH connection", report)


if __name__ == "__main__":
    unittest.main()
