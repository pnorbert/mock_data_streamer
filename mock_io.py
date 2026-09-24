import json
from abc import ABC, abstractmethod
from pathlib import Path


class IO(ABC):
    """Common producer output interface."""

    @classmethod
    def create(cls, settings):
        connection_info = cls._read_connection_info(settings.destination)
        io_id = connection_info.get("id", "adios") if connection_info else "adios"

        if io_id == "sockets":
            from socket_io import SocketIO

            return SocketIO(settings, connection_info)
        if io_id == "singlesocket":
            from single_socket_io import SingleSocketIO

            return SingleSocketIO(settings, connection_info)
        if io_id == "adios":
            from adios_io import AdiosIO

            return AdiosIO(settings, connection_info)
        raise ValueError(f"Unsupported IO id in {settings.destination}: {io_id}")

    @staticmethod
    def _read_connection_info(destination):
        path = Path(destination)
        if not path.is_file():
            return None

        try:
            with path.open(encoding="utf-8") as stream:
                connection_info = json.load(stream)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

        if not isinstance(connection_info, dict) or "id" not in connection_info:
            return None
        return connection_info

    @staticmethod
    def data_variables(ht):
        d1 = ht.data_noghost()
        return {
            "d1": d1,
            "d2": d1 / 2.0,
            "d3": d1 * 1.5,
            "d4": d1 / 3.0,
            "d5": d1 * 3.14159,
            "d6": d1 / 2.71828,
        }

    def write(self, ht):
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
