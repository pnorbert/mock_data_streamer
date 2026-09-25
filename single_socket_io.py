import socket

from mock_io import IO
from socket_protocol import prove_private_key, send_arrays, send_end, send_hello


VARIABLES = ("d1", "d2", "d3", "d4", "d5", "d6")


class SingleSocketIO(IO):
    def __init__(self, settings, connection_info, private_key):
        del settings
        self._socket = None
        host = connection_info.get("host")
        if not isinstance(host, str) or not host:
            raise ValueError("Single-socket connection info has no valid host")
        port = connection_info.get("port")
        if not isinstance(port, int) or not 0 < port < 65536:
            raise ValueError(f"Invalid single-socket connection port: {port}")
        consumer_id = connection_info.get("consumer_id")
        if not isinstance(consumer_id, str) or not consumer_id:
            raise ValueError("Single-socket connection info has no valid consumer_id")

        connection = socket.create_connection((host, port))
        try:
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            prove_private_key(connection, private_key)
            send_hello(connection, VARIABLES, consumer_id)
        except Exception:
            connection.close()
            raise
        self._socket = connection

    def write_data(self, data):
        send_arrays(self._socket, [(name, data[name]) for name in VARIABLES])

    def close(self):
        if self._socket is not None:
            try:
                send_end(self._socket)
            except OSError:
                pass
            self._socket.close()
            self._socket = None
