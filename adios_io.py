import adios2
import numpy as np

from mock_io import IO


class AdiosIO(IO):
    def __init__(self, settings, connection_info=None):
        connection_info = connection_info or {}
        output = connection_info.get("output", settings.destination)
        engine = connection_info.get("engine", settings.engine)

        self._adios = adios2.Adios()
        self._io = self._adios.declare_io("SimulationOutput")
        self._io.set_engine(engine)

        zeroes = np.zeros((settings.ndx, settings.ndy), dtype=np.float64)
        dims = [settings.ndx, settings.ndy]
        self._variables = {
            name: self._io.define_variable(
                f"T{index}", zeroes, dims, [0, 0], dims, True
            )
            for index, name in enumerate(
                ("d1", "d2", "d3", "d4", "d5", "d6"), start=1
            )
        }

        self._io.define_attribute("description", "Temperature from simulation", "T1")
        self._io.define_attribute("unit", "C", "T1")
        self._stream = adios2.Stream(self._io, output, "w")
        self._stream.engine.lock_writer_definitions()

    def write_data(self, data):
        self._stream.begin_step()
        for name, variable in self._variables.items():
            self._stream.write(variable, data[name])
        self._stream.end_step()

    def close(self):
        if self._stream is not None:
            self._stream.close()
            self._stream = None
