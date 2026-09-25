import json
import math
import threading
from collections import deque
from abc import ABC, abstractmethod
from pathlib import Path

from connection_security import decrypt_connection_info, load_private_key


class IO(ABC):
    """Common producer output interface."""

    @classmethod
    def create(cls, settings, timing_log=None):
        private_key = load_private_key(settings.private_key)
        from remote_consumer_io import ServerConfig

        if (
            Path(settings.destination).suffix.lower() == ".conf"
            or ServerConfig.is_server_config(settings.destination)
        ):
            from remote_consumer_io import RestartingSingleSocketIO

            output = RestartingSingleSocketIO(
                settings, private_key, timing_log=timing_log
            )
            return BufferedIO(
                output,
                buffer_seconds=getattr(settings, "buffer_seconds", 600.0),
                output_interval_seconds=getattr(
                    settings, "output_interval_seconds", 3.0
                ),
                timing_log=timing_log,
            )

        connection_info = cls._read_connection_info(settings.destination, private_key)
        io_id = connection_info.get("id", "adios") if connection_info else "adios"

        if io_id == "sockets":
            from socket_io import SocketIO

            output = SocketIO(settings, connection_info, private_key)
        elif io_id == "singlesocket":
            from single_socket_io import SingleSocketIO

            output = SingleSocketIO(settings, connection_info, private_key)
        elif io_id == "adios":
            from adios_io import AdiosIO

            output = AdiosIO(settings, connection_info)
        else:
            raise ValueError(f"Unsupported IO id in {settings.destination}: {io_id}")

        return BufferedIO(
            output,
            buffer_seconds=getattr(settings, "buffer_seconds", 600.0),
            output_interval_seconds=getattr(
                settings, "output_interval_seconds", 3.0
            ),
            timing_log=timing_log,
        )

    @staticmethod
    def _read_connection_info(destination, private_key):
        path = Path(destination)
        if not path.is_file():
            return None

        try:
            with path.open(encoding="utf-8") as stream:
                connection_info = json.load(stream)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

        if not isinstance(connection_info, dict):
            return None
        if "format" not in connection_info:
            if "id" in connection_info:
                raise ValueError("Socket connection information must be encrypted")
            return None
        return decrypt_connection_info(connection_info, private_key)

    @staticmethod
    def data_variables(ht):
        return IO.data_variables_from_d1(ht.data_noghost())

    @staticmethod
    def data_variables_from_d1(d1):
        return {
            "d1": d1,
            "d2": d1 / 2.0,
            "d3": d1 * 1.5,
            "d4": d1 / 3.0,
            "d5": d1 * 3.14159,
            "d6": d1 / 2.71828,
        }

    def write(self, ht, step=None):
        del step
        self.write_data(self.data_variables(ht))

    @abstractmethod
    def write_data(self, data):
        pass

    @abstractmethod
    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class BufferedIO(IO):
    """Queue output steps and write them from a dedicated worker thread."""

    def __init__(
        self, output, buffer_seconds, output_interval_seconds, timing_log=None
    ):
        if not math.isfinite(buffer_seconds) or buffer_seconds <= 0:
            raise ValueError("Buffer duration must be greater than zero")
        if not math.isfinite(output_interval_seconds) or output_interval_seconds <= 0:
            raise ValueError("Output interval must be greater than zero")

        self._output = output
        self._timing_log = timing_log
        self._capacity = max(1, math.ceil(buffer_seconds / output_interval_seconds))
        self._buffer = deque()
        self._condition = threading.Condition()
        self._closing = False
        self._closed = False
        self._error = None
        self._dropped_steps = 0
        self._next_drop_index = 1 if self._capacity > 1 else 0
        self._worker = threading.Thread(
            target=self._write_pending,
            name="mock-output-writer",
        )
        self._worker.start()

    @property
    def capacity(self):
        return self._capacity

    @property
    def dropped_steps(self):
        with self._condition:
            return self._dropped_steps

    def write(self, ht, step=None):
        # The simulation reuses its arrays, so take one stable snapshot before
        # returning. The remaining output-array computation happens in the worker.
        self._enqueue((ht.data_noghost(), True, step))

    def write_data(self, data):
        self._enqueue((data, False, None))

    def _enqueue(self, pending):
        dropped = None
        with self._condition:
            self._raise_if_unavailable()
            if len(self._buffer) >= self._capacity:
                dropped = self._drop_one_pending_step()
            self._buffer.append(pending)
            self._condition.notify()

        if dropped is not None and self._timing_log is not None:
            self._timing_log.record(
                "io.buffer_drop",
                dropped_step=dropped[2],
                incoming_step=pending[2],
                buffer_capacity_steps=self._capacity,
            )

    def _drop_one_pending_step(self):
        drop_index = min(self._next_drop_index, len(self._buffer) - 1)
        dropped = self._buffer[drop_index]
        del self._buffer[drop_index]
        self._dropped_steps += 1

        self._next_drop_index = drop_index + 1
        if self._next_drop_index >= self._capacity:
            self._next_drop_index = 0
        return dropped

    def _raise_if_unavailable(self):
        if self._error is not None:
            raise RuntimeError("Background output writer failed") from self._error
        if self._closing or self._closed:
            raise RuntimeError("Cannot write to closed output")

    def _write_pending(self):
        try:
            while True:
                with self._condition:
                    self._condition.wait_for(lambda: self._buffer or self._closing)
                    if not self._buffer:
                        return
                    data, is_heat_snapshot, _step = self._buffer.popleft()
                    if self._capacity > 1 and self._next_drop_index == 0:
                        self._next_drop_index = 1
                    elif self._next_drop_index > 0:
                        self._next_drop_index -= 1
                if is_heat_snapshot:
                    data = self.data_variables_from_d1(data)
                self._output.write_data(data)
        except BaseException as exc:
            with self._condition:
                self._error = exc
                self._dropped_steps += len(self._buffer)
                self._buffer.clear()
                self._closing = True
                self._condition.notify_all()

    def close(self):
        with self._condition:
            if self._closed:
                return
            self._closing = True
            self._condition.notify_all()

        self._worker.join()

        close_error = None
        try:
            self._output.close()
        except BaseException as exc:
            close_error = exc

        with self._condition:
            self._closed = True
            writer_error = self._error

        if writer_error is not None:
            raise RuntimeError("Background output writer failed") from writer_error
        if close_error is not None:
            raise close_error
