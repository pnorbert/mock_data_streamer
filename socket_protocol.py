import base64
import hmac
import json
import math
import secrets
import struct

import nacl.exceptions
import nacl.public
import numpy as np


_HEADER_LENGTH = struct.Struct("!Q")
_MAX_HEADER_BYTES = 1024 * 1024
_AUTH_CHALLENGE_BYTES = 32


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


def verify_producer_private_key(sock, public_key):
    """Challenge the peer to prove possession of the matching private key."""
    challenge = secrets.token_bytes(_AUTH_CHALLENGE_BYTES)
    encrypted = nacl.public.SealedBox(public_key).encrypt(challenge)
    _send_header(
        sock,
        {
            "type": "auth_challenge",
            "challenge": base64.b64encode(encrypted).decode("ascii"),
        },
    )
    response = _receive_header(sock)
    if response.get("type") != "auth_response":
        raise RuntimeError("Producer did not answer the private-key challenge")
    try:
        answer = base64.b64decode(response["response"], validate=True)
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("Producer returned an invalid private-key response") from exc
    if not hmac.compare_digest(answer, challenge):
        raise RuntimeError("Producer failed private-key authentication")


def prove_private_key(sock, private_key):
    """Answer a consumer challenge using a Curve25519 private key."""
    request = _receive_header(sock)
    if request.get("type") != "auth_challenge":
        raise RuntimeError("Consumer did not send a private-key challenge")
    try:
        encrypted = base64.b64decode(request["challenge"], validate=True)
        challenge = nacl.public.SealedBox(private_key).decrypt(encrypted)
    except (KeyError, TypeError, ValueError, nacl.exceptions.CryptoError) as exc:
        raise RuntimeError("Unable to answer consumer private-key challenge") from exc
    if len(challenge) != _AUTH_CHALLENGE_BYTES:
        raise RuntimeError("Consumer sent an invalid private-key challenge")
    _send_header(
        sock,
        {
            "type": "auth_response",
            "response": base64.b64encode(challenge).decode("ascii"),
        },
    )


def send_arrays(sock, variables):
    arrays = []
    descriptions = []
    for name, value in variables:
        array = np.asarray(value)
        if not array.flags.c_contiguous:
            array = np.ascontiguousarray(array)
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


def send_ack(sock):
    """Confirm that one complete data message was written by the consumer."""
    _send_header(sock, {"type": "ack"})


def receive_ack(sock):
    """Wait until the consumer confirms one complete data message."""
    header = _receive_header(sock)
    if header.get("type") != "ack":
        raise RuntimeError(f"Expected socket acknowledgement, got: {header.get('type')}")


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
