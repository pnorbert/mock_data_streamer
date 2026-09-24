import pickle

from mock_io import IO


class PickleIO(IO):
    """Write each data step as the next object in a pickle stream."""

    def __init__(self, settings, connection_info=None):
        connection_info = connection_info or {}
        output = connection_info.get("output", settings.destination)
        self._stream = open(output, "wb")

    def write_data(self, data):
        pickle.dump(data, self._stream, protocol=pickle.HIGHEST_PROTOCOL)
        self._stream.flush()

    def close(self):
        if self._stream is not None:
            self._stream.close()
            self._stream = None
