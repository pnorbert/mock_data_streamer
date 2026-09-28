import os
import unittest
from unittest import mock

from ntfy_notifications import NtfyNotifier


class RecordingLog:
    def __init__(self):
        self.records = []

    def record(self, event, **fields):
        self.records.append((event, fields))


class NtfyNotifierTests(unittest.TestCase):
    def test_publishes_topic_headers_message_and_bearer_token(self):
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b""
        opener = mock.Mock(return_value=response)
        timing_log = RecordingLog()

        with mock.patch.dict(os.environ, {"APP_NTFY_TOKEN": "tk_secret"}):
            notifier = NtfyNotifier(
                "warnings with spaces",
                server_url="https://ntfy.example.test/",
                token_env="APP_NTFY_TOKEN",
                timeout_seconds=2,
                timing_log=timing_log,
                opener=opener,
            )
            notifier.notify(
                "Buffer warning",
                "Buffer is filling",
                priority=4,
                tags=("warning", "computer"),
            )
            notifier.close()

        request = opener.call_args.args[0]
        self.assertEqual(request.full_url, "https://ntfy.example.test/warnings%20with%20spaces")
        self.assertEqual(request.data, b"Buffer is filling")
        self.assertEqual(request.get_header("Title"), "Buffer warning")
        self.assertEqual(request.get_header("Priority"), "4")
        self.assertEqual(request.get_header("Tags"), "warning,computer")
        self.assertEqual(request.get_header("Authorization"), "Bearer tk_secret")
        self.assertEqual(opener.call_args.kwargs["timeout"], 2)
        self.assertEqual(
            timing_log.records,
            [("notification.sent", {"title": "Buffer warning"})],
        )

    def test_publish_failure_is_logged_and_does_not_escape(self):
        timing_log = RecordingLog()
        notifier = NtfyNotifier(
            "warnings",
            timing_log=timing_log,
            opener=mock.Mock(side_effect=OSError("offline")),
        )
        notifier.notify("Disconnected", "Connection lost")
        notifier.close()

        self.assertEqual(timing_log.records[0][0], "notification.failed")
        self.assertEqual(timing_log.records[0][1]["error_type"], "OSError")
        self.assertEqual(timing_log.records[0][1]["error"], "offline")


if __name__ == "__main__":
    unittest.main()
