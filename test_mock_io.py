import threading
import unittest

import numpy as np

from mock_io import BufferedIO


class RecordingOutput:
    def __init__(self, gate=None, error=None):
        self.gate = gate
        self.error = error
        self.started = threading.Event()
        self.values = []
        self.closed = False

    def write_data(self, data):
        self.started.set()
        if self.gate is not None:
            self.gate.wait()
        if self.error is not None:
            raise self.error
        self.values.append(data)

    def close(self):
        self.closed = True


class HeatData:
    def __init__(self, data):
        self.data = data

    def data_noghost(self):
        return self.data.copy()


class RecordingLog:
    def __init__(self):
        self.records = []

    def record(self, event, **fields):
        self.records.append((event, fields))


class BufferedIOTests(unittest.TestCase):
    def test_writes_on_worker_and_drains_on_close(self):
        output = RecordingOutput()
        io = BufferedIO(output, buffer_seconds=6, output_interval_seconds=3)

        io.write_data(1)
        io.write_data(2)
        io.close()

        self.assertEqual(output.values, [1, 2])
        self.assertTrue(output.closed)

    def test_write_snapshots_heat_data_before_returning(self):
        gate = threading.Event()
        output = RecordingOutput(gate)
        io = BufferedIO(output, buffer_seconds=6, output_interval_seconds=3)
        heat = HeatData(np.array([[2.0]]))

        io.write(heat)
        self.assertTrue(output.started.wait(timeout=1))
        heat.data[0, 0] = 100.0
        gate.set()
        io.close()

        self.assertEqual(output.values[0]["d1"][0, 0], 2.0)
        self.assertEqual(output.values[0]["d2"][0, 0], 1.0)

    def test_full_buffer_drops_one_pending_step_per_new_step(self):
        gate = threading.Event()
        output = RecordingOutput(gate)
        timing_log = RecordingLog()
        io = BufferedIO(
            output,
            buffer_seconds=12,
            output_interval_seconds=3,
            timing_log=timing_log,
        )

        io.write_data(0)
        self.assertTrue(output.started.wait(timeout=1))
        for value in range(1, 6):
            io.write_data(value)
        self.assertEqual(io.dropped_steps, 1)
        for value in range(6, 8):
            io.write_data(value)

        gate.set()
        io.close()

        self.assertEqual(output.values, [0, 2, 4, 6, 7])
        self.assertEqual(io.dropped_steps, 3)
        self.assertEqual(
            timing_log.records,
            [
                (
                    "io.buffer_drop",
                    {
                        "dropped_step": None,
                        "incoming_step": None,
                        "buffer_capacity_steps": 4,
                    },
                )
            ]
            * 3,
        )

    def test_drop_log_includes_step_numbers(self):
        gate = threading.Event()
        output = RecordingOutput(gate)
        timing_log = RecordingLog()
        io = BufferedIO(
            output,
            buffer_seconds=6,
            output_interval_seconds=3,
            timing_log=timing_log,
        )

        io.write(HeatData(np.array([[0.0]])), step=10)
        self.assertTrue(output.started.wait(timeout=1))
        io.write(HeatData(np.array([[1.0]])), step=11)
        io.write(HeatData(np.array([[2.0]])), step=12)
        io.write(HeatData(np.array([[3.0]])), step=13)
        gate.set()
        io.close()

        self.assertEqual(
            timing_log.records,
            [
                (
                    "io.buffer_drop",
                    {
                        "dropped_step": 11,
                        "incoming_step": 13,
                        "buffer_capacity_steps": 2,
                    },
                )
            ],
        )

    def test_worker_failure_is_reported_by_close(self):
        output = RecordingOutput(error=OSError("write failed"))
        io = BufferedIO(output, buffer_seconds=6, output_interval_seconds=3)
        io.write_data(1)

        with self.assertRaisesRegex(RuntimeError, "Background output writer failed"):
            io.close()
        self.assertTrue(output.closed)

    def test_capacity_is_derived_from_duration(self):
        output = RecordingOutput()
        io = BufferedIO(output, buffer_seconds=10, output_interval_seconds=3)
        self.assertEqual(io.capacity, 4)
        io.close()


if __name__ == "__main__":
    unittest.main()
