import socket

from mock_io import IO
from socket_protocol import prove_private_key, send_arrays, send_end, send_hello


VARIABLE_PAIRS = (("d1", "d2"), ("d3", "d4"), ("d5", "d6"))


class SocketIO(IO):
    def __init__(self, settings, connection_info, private_key):
        del settings
        self._sockets = []
        host = connection_info.get("host")
        if not isinstance(host, str) or not host:
            raise ValueError("Socket connection info has no valid host")
        port = connection_info.get("port")
        if not isinstance(port, int) or not 0 < port < 65536:
            raise ValueError(f"Invalid socket connection port: {port}")
        consumer_id = connection_info.get("consumer_id")
        if not isinstance(consumer_id, str) or not consumer_id:
            raise ValueError("Socket connection info has no valid consumer_id")

        try:
            for pair in VARIABLE_PAIRS:
                connection = socket.create_connection((host, port))
                try:
                    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    prove_private_key(connection, private_key)
                    send_hello(connection, pair, consumer_id)
                except Exception:
                    connection.close()
                    raise
                self._sockets.append(connection)
        except Exception:
            self._close_sockets()
            raise

    def write_data(self, data):
        for connection, pair in zip(self._sockets, VARIABLE_PAIRS):
            send_arrays(
                connection,
                [(name, data[name]) for name in pair],
            )

    def close(self):
        for connection in self._sockets:
            try:
                send_end(connection)
            except OSError:
                pass
        self._close_sockets()

    def _close_sockets(self):
        for connection in self._sockets:
            try:
                connection.close()
            except OSError:
                pass
        self._sockets = []
