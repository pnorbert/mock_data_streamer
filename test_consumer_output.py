import pickle
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from consumer_output import RotatingConsumerOutput


class FakeClock:
    def __init__(self, value=0.0):
        self.value = value

    def __call__(self):
        return self.value


class ConsumerOutputTests(unittest.TestCase):
    def test_rotates_to_timestamped_files_at_interval(self):
        clock = FakeClock()
        timestamps = iter(
            (
                datetime(2026, 9, 25, 10, 0, 0),
                datetime(2026, 9, 25, 11, 0, 0),
            )
        )
        with tempfile.TemporaryDirectory() as temporary, mock.patch(
            "consumer_output.ADIOS2_AVAILABLE", False
        ):
            output = RotatingConsumerOutput(
                Path(temporary) / "output",
                "BP5",
                (1, 1),
                interval_seconds=10,
                clock=clock,
                timestamp_factory=lambda: next(timestamps),
            )
            output.write_data({"step": 1})
            clock.value = 9.9
            output.write_data({"step": 2})
            clock.value = 10.0
            output.write_data({"step": 3})
            output.close()

            files = sorted((Path(temporary) / "output").glob("*.pkl"))
            self.assertEqual(len(files), 2)
            self.assertTrue(files[0].name.startswith("20260925T100000.000000"))
            self.assertTrue(files[1].name.startswith("20260925T110000.000000"))
            steps = []
            for path in files:
                with path.open("rb") as stream:
                    while True:
                        try:
                            steps.append(pickle.load(stream)["step"])
                        except EOFError:
                            break
            self.assertEqual(steps, [1, 2, 3])

    def test_new_consumer_never_overwrites_same_timestamp(self):
        timestamp = datetime(2026, 9, 25, 10, 0, 0)
        with tempfile.TemporaryDirectory() as temporary, mock.patch(
            "consumer_output.ADIOS2_AVAILABLE", False
        ):
            directory = Path(temporary) / "output"
            for step in (1, 2):
                output = RotatingConsumerOutput(
                    directory,
                    "BP5",
                    (1, 1),
                    timestamp_factory=lambda: timestamp,
                )
                output.write_data({"step": step})
                output.close()

            files = sorted(directory.glob("*.pkl"))
            self.assertEqual(len(files), 2)
            self.assertTrue(any(path.name.endswith("-1.pkl") for path in files))
            values = []
            for path in files:
                with path.open("rb") as stream:
                    values.append(pickle.load(stream)["step"])
            self.assertEqual(sorted(values), [1, 2])


if __name__ == "__main__":
    unittest.main()
