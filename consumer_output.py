from pathlib import Path
from types import SimpleNamespace

from adios_io import ADIOS2_AVAILABLE, AdiosIO
from pickle_io import PickleIO


def create_consumer_output(output, engine, shape):
    settings = SimpleNamespace(
        destination=output,
        engine=engine,
        ndx=shape[0],
        ndy=shape[1],
    )
    if ADIOS2_AVAILABLE:
        return AdiosIO(settings)

    pickle_output = str(Path(output).with_suffix(".pkl"))
    settings.destination = pickle_output
    print(
        f"adios2 is not available; writing pickle output to {pickle_output}",
        flush=True,
    )
    return PickleIO(settings)
