import json
from datetime import datetime


def timestamp():
    return datetime.now().astimezone().isoformat(timespec="microseconds")


class TimingLog:
    """Append line-delimited JSON timing records and flush each record."""

    def __init__(self, path):
        self._stream = open(path, "a", encoding="utf-8", buffering=1)

    def record(self, event, **fields):
        record = {
            "logged_at": timestamp(),
            "event": event,
            **fields,
        }
        json.dump(record, self._stream, separators=(",", ":"))
        self._stream.write("\n")

    def close(self):
        if self._stream is not None:
            self._stream.close()
            self._stream = None
