import configparser
import json
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
from remote_consumer_io import (
    RestartingSingleSocketIO,
    SSHConsumerLauncher,
    SSHSocketConnector,
    ServerConfig,
)


CONFIG = """\
[server]
host = example-host
working_directory = /srv/mock streamer
python = /srv/venv/bin/python3
script = mockConsumerSingleSocket.py
connection_file = conn.json
output_directory = output
file_interval_seconds = 1800
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
            self.assertIn("--file-interval-seconds 1800.0", remote)
            self.assertIn("conn.json output", remote)

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

    def test_socket_transport_defaults_to_direct_and_rejects_unknown_values(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.assertEqual(self.make_config(directory).socket_transport, "direct")

            parser = configparser.ConfigParser()
            parser.read_string(CONFIG)
            parser["server"]["socket_transport"] = "socks"
            path = directory / "invalid.conf"
            with path.open("w", encoding="utf-8") as stream:
                parser.write(stream)
            with self.assertRaisesRegex(ValueError, "socket_transport"):
                ServerConfig(path)

    def test_ssh_socket_connector_uses_stdio_forwarding(self):
        config = SimpleNamespace(
            ssh_command=["ssh", "-J", "bastion"],
            host="user@destination",
        )
        local_socket = mock.Mock()
        ssh_socket = mock.Mock()
        process = mock.Mock()
        process.wait.return_value = 0

        with (
            mock.patch(
                "remote_consumer_io.socket.socketpair",
                return_value=(local_socket, ssh_socket),
            ),
            mock.patch(
                "remote_consumer_io.subprocess.Popen", return_value=process
            ) as popen,
        ):
            connection = SSHSocketConnector(config)(("127.0.0.1", 8501), timeout=5)

        arguments = popen.call_args.args[0]
        self.assertEqual(
            arguments,
            [
                "ssh",
                "-J",
                "bastion",
                "-T",
                "-o",
                "BatchMode=yes",
                "-W",
                "127.0.0.1:8501",
                "user@destination",
            ],
        )
        self.assertIs(popen.call_args.kwargs["stdin"], ssh_socket)
        self.assertIs(popen.call_args.kwargs["stdout"], ssh_socket)
        local_socket.settimeout.assert_called_once_with(5)
        ssh_socket.close.assert_called_once_with()

        connection.close()
        local_socket.close.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=0.2)

    def test_ssh_transport_is_passed_to_single_socket_io(self):
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
            host="user@destination",
            ssh_command=["ssh"],
            socket_transport="ssh",
            socket_timeout_seconds=5,
            max_relaunch_attempts=1,
            retry_delay_seconds=0,
        )
        settings = SimpleNamespace(destination="unused")
        socket_output = mock.Mock()

        with mock.patch(
            "remote_consumer_io.SingleSocketIO", return_value=socket_output
        ) as single_socket:
            output = RestartingSingleSocketIO(
                settings, private_key, config=config, launcher=launcher
            )
            output.close()

        connector = single_socket.call_args.kwargs["connector"]
        self.assertIsInstance(connector, SSHSocketConnector)
        self.assertIs(connector.config, config)
        socket_output.close.assert_called_once_with()

    def test_ssh_launch_and_stream_share_control_connection(self):
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
        settings = SimpleNamespace(destination="unused")
        socket_output = mock.Mock()

        with tempfile.TemporaryDirectory() as temporary:
            config = self.make_config(Path(temporary))
            config.socket_transport = "ssh"
            launcher = mock.Mock()
            launcher.launch.return_value = envelope
            with (
                mock.patch(
                    "remote_consumer_io.SSHConsumerLauncher",
                    return_value=launcher,
                ) as launcher_class,
                mock.patch(
                    "remote_consumer_io.SingleSocketIO",
                    return_value=socket_output,
                ) as single_socket,
                mock.patch("remote_consumer_io.subprocess.run") as run,
            ):
                output = RestartingSingleSocketIO(
                    settings, private_key, config=config
                )
                launch_command = launcher_class.call_args.args[1]
                connector = single_socket.call_args.kwargs["connector"]
                self.assertEqual(connector.ssh_command, launch_command)
                control_options = " ".join(launch_command)
                self.assertIn("ControlMaster=auto", control_options)
                self.assertIn("ControlPath=", control_options)
                self.assertIn("ControlPersist=yes", control_options)
                output.close()

            cleanup_arguments = run.call_args.args[0]
            self.assertIn("-O", cleanup_arguments)
            self.assertIn("exit", cleanup_arguments)
            self.assertIn("-S", cleanup_arguments)

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
            [mock.call(), mock.call()],
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

if __name__ == "__main__":
    unittest.main()
