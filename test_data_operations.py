import importlib.util
import tempfile
import unittest
from pathlib import Path

import numpy as np

from data_operations import (
    Blosc2ZstdOperation,
    data_operation_from_description,
    load_data_operation,
)


BLOSC2_AVAILABLE = importlib.util.find_spec("blosc2") is not None


class DataOperationConfigurationTests(unittest.TestCase):
    def write_config(self, directory, contents):
        path = Path(directory) / "compression.conf"
        path.write_text(contents, encoding="utf-8")
        return path

    @unittest.skipUnless(BLOSC2_AVAILABLE, "blosc2 is not installed")
    def test_loads_blosc2_zstd_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = self.write_config(
                temporary,
                """\
[compression]
implementation = blosc2
codec = zstd
compression_level = 1
filter = shuffle
threads = 1
""",
            )
            operation = load_data_operation(path)

        self.assertIsInstance(operation, Blosc2ZstdOperation)
        self.assertEqual(operation.compression_level, 1)
        self.assertEqual(operation.filter_name, "shuffle")
        self.assertEqual(operation.threads, 1)

    def test_rejects_unknown_implementation(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = self.write_config(
                temporary,
                "[compression]\nimplementation = unknown\n",
            )
            with self.assertRaisesRegex(ValueError, "Unsupported compression"):
                load_data_operation(path)

    def test_rejects_invalid_blosc2_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = self.write_config(
                temporary,
                """\
[compression]
implementation = blosc2
compression_level = 10
threads = 0
""",
            )
            with self.assertRaisesRegex(ValueError, "compression_level"):
                load_data_operation(path)

    def test_decoder_description_is_validated(self):
        with self.assertRaisesRegex(ValueError, "Unsupported data operation"):
            data_operation_from_description({"name": "unknown"})


@unittest.skipUnless(BLOSC2_AVAILABLE, "blosc2 is not installed")
class Blosc2ZstdOperationTests(unittest.TestCase):
    def setUp(self):
        self.operation = Blosc2ZstdOperation(
            compression_level=1,
            filter_name="shuffle",
            threads=1,
        )

    def test_round_trips_int16_samples(self):
        expected = np.tile(np.arange(-2048, 2048, dtype=np.int16), 64)

        encoded = self.operation.encode(expected)
        self.assertIsNotNone(encoded.operation)
        actual = data_operation_from_description(encoded.operation).decode(
            encoded.payload,
            expected.dtype,
            expected.shape,
        )

        np.testing.assert_array_equal(actual, expected)

    def test_incompressible_array_falls_back_to_raw_bytes(self):
        expected = np.random.default_rng(7).integers(
            -32768,
            32767,
            size=200_000,
            dtype=np.int16,
        )

        encoded = self.operation.encode(expected)

        self.assertIsNone(encoded.operation)
        self.assertEqual(bytes(encoded.payload), expected.tobytes())

    def test_empty_array_falls_back_to_raw_bytes(self):
        expected = np.empty((0,), dtype=np.float64)

        encoded = self.operation.encode(expected)

        self.assertIsNone(encoded.operation)
        self.assertEqual(len(encoded.payload), 0)


if __name__ == "__main__":
    unittest.main()
