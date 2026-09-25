import configparser
import json
import pickle
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import nacl.public
import numpy as np

from connection_security import encrypt_connection_info
from mock_io import BufferedIO
from pickle_io import PickleIO
from remote_consumer_io import (
    RestartingSingleSocketIO,
    SSHConsumerLauncher,
    ServerConfig,
)


CONFIG = """\
[server]
host = example-host
working_directory = /srv/mock streamer
python = /srv/venv/bin/python3
script = mockConsumerSingleSocket.py
connection_file = conn.json
output = out.bp
stdout_log = consumer output.log
pid_file = consumer.pid
public_key = keys/mockkey.pub
allow_ip_range = 192.168.1.0/24, 10.0.0.0/8
port = 8501-8501
bind_host = 0.0.0.0
advertise_host = 192.168.1.7
socket_timeout_seconds = 5
startup_timeout_seconds = 2
retry_delay_seconds = 0
max_relaunch_attempts = 2
"""


class RemoteConsumerTests(unittest.TestCase):
    def make_config(self, directory):
        path = directory / "server.conf"
        path.write_text(CONFIG, encoding="utf-8")
        return ServerConfig(path)

    def test_launcher_detaches_redirects_and_returns_envelope(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = self.make_config(Path(temporary))
            envelope = {"format": "test", "ciphertext": "test"}
            completed = SimpleNamespace(
                returncode=0, stdout=json.dumps(envelope), stderr=""
            )
            with mock.patch("subprocess.run", return_value=completed) as run:
                result = SSHConsumerLauncher(config).launch()

            self.assertEqual(result, envelope)
            arguments = run.call_args.args[0]
            self.assertEqual(arguments[:2], ["ssh", "example-host"])
            remote = arguments[2]
            self.assertIn("nohup /srv/venv/bin/python3", remote)
            self.assertIn(">> 'consumer output.log' 2>&1 < /dev/null &", remote)
            self.assertIn("consumer_pid=$!", remote)
            self.assertIn("cat conn.json", remote)
            self.assertIn("/proc/$old_pid/cmdline", remote)
            self.assertIn("--allow-ip-range 192.168.1.0/24", remote)
            self.assertIn("--allow-ip-range 10.0.0.0/8", remote)
            self.assertNotIn("--append-output", remote)
            self.assertIn(
                "--append-output",
                SSHConsumerLauncher(config)._remote_command(append_output=True),
            )

    def test_server_config_rejects_missing_network(self):
        parser = configparser.ConfigParser()
        parser.read_string(CONFIG)
        parser["server"]["allow_ip_range"] = ""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "server.conf"
            with path.open("w", encoding="utf-8") as stream:
                parser.write(stream)
            with self.assertRaisesRegex(ValueError, "allow_ip_range"):
                ServerConfig(path)

    def test_disconnect_retries_failed_step_without_resending_successes(self):
        private_key = nacl.public.PrivateKey.generate()
        connection_info = {
            "id": "singlesocket",
            "protocol_version": 3,
            "consumer_id": "session",
            "host": "127.0.0.1",
            "port": 8501,
        }
        envelope = json.loads(
            encrypt_connection_info(connection_info, private_key.public_key)
        )
        launcher = mock.Mock()
        launcher.launch.return_value = envelope
        config = SimpleNamespace(
            host="example-host",
            socket_timeout_seconds=5,
            max_relaunch_attempts=2,
            retry_delay_seconds=0,
        )
        settings = SimpleNamespace(
            destination="unused",
            buffer_seconds=6,
            output_interval_seconds=3,
        )
        first = mock.Mock()
        first.write_data.side_effect = [
            None,
            None,
            None,
            TimeoutError("stalled"),
        ]
        second = mock.Mock()

        with mock.patch(
            "remote_consumer_io.SingleSocketIO", side_effect=[first, second]
        ):
            output = RestartingSingleSocketIO(
                settings, private_key, config=config, launcher=launcher
            )
            steps = [
                RestartingSingleSocketIO.data_variables_from_d1(
                    np.array([[float(value)]]), step=value
                )
                for value in range(4)
            ]
            for step in steps:
                output.write_data(step)

        self.assertEqual(launcher.launch.call_count, 2)
        self.assertEqual(
            launcher.launch.call_args_list,
            [mock.call(append_output=False), mock.call(append_output=True)],
        )
        first.abort.assert_called_once_with()
        second.write_data.assert_called_once()
        retried = second.write_data.call_args.args[0]
        np.testing.assert_array_equal(retried["d1"], steps[3]["d1"])
        self.assertEqual(retried["iteration"].item(), 3)

    def test_retried_in_flight_step_precedes_buffered_steps(self):
        private_key = nacl.public.PrivateKey.generate()
        connection_info = {
            "id": "singlesocket",
            "protocol_version": 3,
            "consumer_id": "session",
            "host": "127.0.0.1",
            "port": 8501,
        }
        envelope = json.loads(
            encrypt_connection_info(connection_info, private_key.public_key)
        )
        launcher = mock.Mock()
        launcher.launch.return_value = envelope
        config = SimpleNamespace(
            host="example-host",
            socket_timeout_seconds=5,
            max_relaunch_attempts=2,
            retry_delay_seconds=0,
        )
        settings = SimpleNamespace(destination="unused")
        blocked = threading.Event()
        release = threading.Event()
        first = mock.Mock()
        second = mock.Mock()

        def write_then_fail(data):
            if int(data["d1"][0, 0]) == 10:
                return
            blocked.set()
            release.wait(timeout=1)
            raise TimeoutError("stalled")

        first.write_data.side_effect = write_then_fail
        steps = [
            RestartingSingleSocketIO.data_variables_from_d1(
                np.array([[float(step)]]), step=step
            )
            for step in range(10, 17)
        ]

        with mock.patch(
            "remote_consumer_io.SingleSocketIO", side_effect=[first, second]
        ):
            output = RestartingSingleSocketIO(
                settings, private_key, config=config, launcher=launcher
            )
            output.write_data(steps[0])
            buffered = BufferedIO(
                output, buffer_seconds=18, output_interval_seconds=3
            )
            buffered.write_data(steps[1])
            self.assertTrue(blocked.wait(timeout=1))
            for step in steps[2:]:
                buffered.write_data(step)
            release.set()
            buffered.close()

        sent_after_relaunch = [
            int(call.args[0]["d1"][0, 0])
            for call in second.write_data.call_args_list
        ]
        self.assertEqual(sent_after_relaunch, [11, 12, 13, 14, 15, 16])
        sent_iterations = [
            call.args[0]["iteration"].item()
            for call in second.write_data.call_args_list
        ]
        self.assertEqual(sent_iterations, [11, 12, 13, 14, 15, 16])

    def test_pickle_output_appends_after_consumer_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "out.pkl"
            first = PickleIO(
                SimpleNamespace(destination=output, append_output=False)
            )
            first.write_data({"step": 10})
            first.close()

            replacement = PickleIO(
                SimpleNamespace(destination=output, append_output=True)
            )
            replacement.write_data({"step": 11})
            replacement.write_data({"step": 12})
            replacement.close()

            with output.open("rb") as stream:
                steps = [pickle.load(stream) for _ in range(3)]
                with self.assertRaises(EOFError):
                    pickle.load(stream)
            self.assertEqual([step["step"] for step in steps], [10, 11, 12])


if __name__ == "__main__":
    unittest.main()
