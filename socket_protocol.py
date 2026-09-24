import json
import math
import struct

import numpy as np


_HEADER_LENGTH = struct.Struct("!Q")
_MAX_HEADER_BYTES = 1024 * 1024


def _send_header(sock, header):
    encoded = json.dumps(header, separators=(",", ":")).encode("utf-8")
    sock.sendall(_HEADER_LENGTH.pack(len(encoded)))
    sock.sendall(encoded)


def send_hello(sock, variables, consumer_id):
    _send_header(
        sock,
        {
            "type": "hello",
            "variables": list(variables),
            "consumer_id": consumer_id,
        },
    )


def send_arrays(sock, variables):
    arrays = []
    descriptions = []
    for name, value in variables:
        array = np.ascontiguousarray(value)
        arrays.append(array)
        descriptions.append(
            {
                "name": name,
                "dtype": array.dtype.str,
                "shape": list(array.shape),
                "nbytes": array.nbytes,
            }
        )

    _send_header(
        sock,
        {"type": "data", "variables": descriptions},
    )
    for array in arrays:
        sock.sendall(memoryview(array).cast("B"))


def send_end(sock):
    _send_header(sock, {"type": "end"})


def _recv_exact(sock, size):
    data = bytearray(size)
    view = memoryview(data)
    received = 0
    while received < size:
        count = sock.recv_into(view[received:])
        if count == 0:
            raise EOFError("Socket closed in the middle of a message")
        received += count
    return data


def _receive_header(sock):
    header_size = _HEADER_LENGTH.unpack(_recv_exact(sock, _HEADER_LENGTH.size))[0]
    if header_size > _MAX_HEADER_BYTES:
        raise ValueError(f"Socket message header is too large: {header_size} bytes")
    return json.loads(_recv_exact(sock, header_size).decode("utf-8"))


def receive_hello(sock):
    header = _receive_header(sock)
    if header.get("type") != "hello":
        raise ValueError(f"Expected socket hello, got: {header.get('type')}")
    return tuple(header.get("variables", [])), header.get("consumer_id")


def receive_message(sock):
    header = _receive_header(sock)
    if header.get("type") == "end":
        return None
    if header.get("type") != "data":
        raise ValueError(f"Unknown socket message type: {header.get('type')}")

    variables = {}
    for description in header["variables"]:
        dtype = np.dtype(description["dtype"])
        shape = tuple(description["shape"])
        expected_size = math.prod(shape) * dtype.itemsize
        if description["nbytes"] != expected_size:
            raise ValueError(
                f"Invalid size for {description['name']}: "
                f"{description['nbytes']} != {expected_size}"
            )
        payload = _recv_exact(sock, expected_size)
        variables[description["name"]] = (
            np.frombuffer(payload, dtype=dtype).reshape(shape).copy()
        )

    return variables
