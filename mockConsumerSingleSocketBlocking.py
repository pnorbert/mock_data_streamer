#!/usr/bin/env python3
"""Single-socket test consumer that deliberately stalls before reading."""

import argparse
import math
import random
import sys
import time
import uuid
from pathlib import Path

from mockConsumerSingleSocket import (
    accept_connection,
    create_listener,
    receive_steps,
    remove_own_connection_file,
    write_connection_file,
)
from timing_log import TimingLog, timestamp


def probability(value):
    converted = float(value)
    if not 0.0 <= converted <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return converted


def nonnegative_int(value):
    converted = int(value)
    if converted < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return converted


def positive_float(value):
    converted = float(value)
    if not math.isfinite(converted) or converted <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return converted


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=(
            "Test consumer that randomly stalls for 1-100 seconds before "
            "reading from a mockProducer single-socket connection."
        )
    )
    parser.add_argument("connection_file", help="rendezvous file to create")
    parser.add_argument("output", help="ADIOS output, or pickle output fallback")
    parser.add_argument("--engine", default="BP5")
    parser.add_argument("--bind-host", default="127.0.0.1")
    parser.add_argument("--advertise-host")
    parser.add_argument(
        "--timing-log",
        default="mockConsumerSingleSocketBlocking.log",
    )
    parser.add_argument(
        "--block-min-seconds",
        type=positive_float,
        default=1.0,
        help="minimum random read stall (default: 1)",
    )
    parser.add_argument(
        "--block-max-seconds",
        type=positive_float,
        default=100.0,
        help="maximum random read stall (default: 100)",
    )
    parser.add_argument(
        "--block-probability",
        type=probability,
        default=1.0,
        help="probability of stalling before a read (default: 1)",
    )
    parser.add_argument(
        "--max-blocks",
        type=nonnegative_int,
        help="maximum stalls; omit for no limit",
    )
    parser.add_argument("--random-seed", type=int)
    args = parser.parse_args(argv)
    if args.block_max_seconds < args.block_min_seconds:
        parser.error("--block-max-seconds must be at least --block-min-seconds")
    return args


class RandomReadBlocker:
    def __init__(
        self,
        timing_log,
        minimum_seconds,
        maximum_seconds,
        block_probability,
        max_blocks,
        random_seed,
    ):
        self._timing_log = timing_log
        self._minimum_seconds = minimum_seconds
        self._maximum_seconds = maximum_seconds
        self._block_probability = block_probability
        self._max_blocks = max_blocks
        self._random = random.Random(random_seed)
        self._blocks = 0

    def __call__(self, step):
        if self._max_blocks is not None and self._blocks >= self._max_blocks:
            return
        if self._random.random() >= self._block_probability:
            return

        requested_seconds = self._random.uniform(
            self._minimum_seconds, self._maximum_seconds
        )
        started_at = timestamp()
        started = time.perf_counter()
        print(
            f"Blocking reads for {requested_seconds:.3f} seconds before step {step}",
            flush=True,
        )
        time.sleep(requested_seconds)
        self._blocks += 1
        self._timing_log.record(
            "socket.read_block",
            step=step,
            started_at=started_at,
            requested_seconds=requested_seconds,
            duration_seconds=time.perf_counter() - started,
        )


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    connection_path = Path(args.connection_file)
    advertise_host = args.advertise_host or args.bind_host
    session_id = str(uuid.uuid4())
    timing_log = TimingLog(args.timing_log)
    blocker = RandomReadBlocker(
        timing_log,
        args.block_min_seconds,
        args.block_max_seconds,
        args.block_probability,
        args.max_blocks,
        args.random_seed,
    )
    listener = None
    connection = None

    try:
        listener = create_listener(args.bind_host)
        write_connection_file(
            connection_path, advertise_host, listener, session_id
        )
        print(f"Connection info written to {connection_path}", flush=True)
        connection = accept_connection(listener, session_id)
        receive_steps(
            connection,
            args.output,
            args.engine,
            timing_log,
            before_receive=blocker,
        )
        print("Producer closed the socket channel", flush=True)
    finally:
        if connection is not None:
            connection.close()
        if listener is not None:
            listener.close()
        remove_own_connection_file(connection_path, session_id)
        timing_log.close()


if __name__ == "__main__":
    main()
