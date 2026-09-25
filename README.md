# Mock Producer and Socket Consumers

For a manually launched consumer, start it before the producer so it can create
the connection-information file. The file's `id` selects the producer's I/O
implementation. A producer can instead receive a `server.conf` file and launch
a single-socket consumer through SSH as described below. Generate a Curve25519
keypair once and keep the private key with the producer:

```bash
python3 keygen.py generate keys/mockkey.pub keys/mockkey
```

The consumers encrypt all connection information with the public key. The
producer decrypts it with the private key, then proves possession of that key
by answering a fresh encrypted challenge on every socket. Neither key argument
has a default and both must be supplied explicitly. Consumers also require at
least one producer IPv4 range in CIDR notation. Connections outside these
ranges are closed before key authentication or protocol parsing. Repeat
`--allow-ip-range` to allow more than one range.

This application filter runs immediately after the kernel accepts the TCP
connection. For an Internet- or site-facing listener, enforce the same source
range with a host firewall or infrastructure security group so unwanted TCP
handshakes and connection floods are dropped before reaching the consumer.

By default, a consumer listens on any available ephemeral port. Use
`--port 5000` to prefer port 5000 and increment until an available port is
found, `--port 5000-5000` to require exactly port 5000, or
`--port 5000-5009` to restrict selection to a firewall-approved range. The
consumer enables address reuse before binding, and the operating system closes
its socket descriptors if the process dies, allowing the chosen port to be
rebound immediately.

## Three-socket version

This version uses three TCP connections through one listening port:
`iteration/d1/d2`, `d3/d4`, and `d5/d6`. Its connection ID is `sockets`.

In the first terminal, start the consumer:

```bash
python3 mockConsumerSockets.py socket-connection.json received.bp \
    --public-key keys/mockkey.pub \
    --allow-ip-range 127.0.0.1/32 \
    --port 5000-5009 \
    --timing-log consumer-timing.jsonl
```

The consumer writes `socket-connection.json` when it is ready. In a second
terminal, run the producer with a 512x512 array and 10 output steps:

```bash
python3 mockProducer.py socket-connection.json 512 512 10 \
    --private-key keys/mockkey \
    --timing-log producer-timing.jsonl
```

The producer recognizes the `"id": "sockets"` entry in the connection file and
opens the three data connections. It writes the initial state immediately and
then writes approximately every three seconds. The consumer stores the 10
received steps in `received.bp` using ADIOS BP5.

Producer output is queued and sent by a background thread, so a slow output
does not pause the simulation. The queue holds 600 seconds of output by default;
set another duration with `--buffer-seconds SECONDS`. If the queue fills, every
other queued step is discarded, one per new step, to make room for the newest
step. Remaining queued steps are flushed when the producer exits.

The timing logs are line-delimited JSON. The producer log records each output
enqueue call and an `io.buffer_drop` event for each overflow eviction, including
the dropped and incoming step numbers. The consumer log records the three
receives, their combined duration, and each output write.

## Single-socket version

This version sends the scalar `iteration` value and all six arrays through one
TCP connection. Its connection ID is `singlesocket`.

In the first terminal, start the single-socket consumer:

```bash
python3 mockConsumerSingleSocket.py single-connection.json single-received.bp \
    --public-key keys/mockkey.pub \
    --allow-ip-range 127.0.0.1/32 \
    --port 5000-5009 \
    --timing-log single-consumer-timing.jsonl
```

In a second terminal, run the producer with a 512x512 array and 10 output steps:

```bash
python3 mockProducer.py single-connection.json 512 512 10 \
    --private-key keys/mockkey \
    --timing-log single-producer-timing.jsonl
```

The single-socket consumer acknowledges each step after its output write
finishes. This lets a producer with a socket timeout distinguish a completed
step from a stalled consumer or a connection that failed during transfer. The
connection information advertises protocol version 3, and the producer rejects
older single-socket rendezvous files that cannot provide acknowledgements. The
producer and consumer must therefore be upgraded together.

## Launching and recovering a consumer through SSH

Pass a configuration file containing a `[server]` section as the producer's
destination. The checked-in `server.conf` is configured for `ubuntupc`:

```ini
[server]
host = ubuntupc
working_directory = /home/pnorbert/Software/LAPD/mock_data_streamer
python = /home/pnorbert/Software/LAPD/.venv/bin/python3
script = mockConsumerSingleSocket.py
connection_file = conn.json
output = out.bp
stdout_log = mockConsumerSingleSocket.stdout.log
pid_file = mockConsumerSingleSocket.pid
public_key = keys/mockkey.pub
allow_ip_range = 192.168.1.0/24
port = 8501-8501
bind_host = 0.0.0.0
advertise_host = 192.168.1.7
socket_timeout_seconds = 30
startup_timeout_seconds = 30
retry_delay_seconds = 1
max_relaunch_attempts = 0
```

The SSH client must already be able to authenticate non-interactively. Paths
other than `working_directory` are interpreted on the remote host. Multiple
allowed networks can be written as a comma-separated list. `ssh_command` may
be set when SSH options or a different executable are needed.

Start the producer normally, using the configuration as its destination:

```bash
python3 mockProducer.py server.conf 512 512 10 \
    --private-key keys/mockkey \
    --buffer-seconds 600 \
    --timing-log producer-timing.jsonl
```

The remote command removes stale rendezvous information, stops the process
recorded in `pid_file`, and starts the consumer with `nohup`. Its stdin is
`/dev/null`, and both stdout and stderr are appended to `stdout_log`. Once the
new encrypted connection information is printed back to the producer, the SSH
session exits; the consumer no longer depends on that session.

`socket_timeout_seconds` bounds every socket operation, including the
per-step acknowledgement. If the consumer stalls past that timeout or the TCP
connection breaks, the producer closes the failed channel, launches a new
consumer, and retries only the interrupted in-flight snapshot. Snapshots that
were already acknowledged are not resent. The existing buffered-output queue
then drains its unsent snapshots in FIFO order before sending newer steps; the
simulation is never recomputed. The first consumer creates a fresh output; a
replacement consumer opens that output in append mode so acknowledged steps
remain present exactly once. A zero `max_relaunch_attempts` retries
indefinitely; a positive value limits each series of SSH launch attempts.
Relaunch and retry activity is recorded as `consumer.connection_lost`,
`consumer.launch`, `consumer.launch_retry`, and `io.retry` events in the
producer timing log.

### Deliberately blocking test consumer

`mockConsumerSingleSocketBlocking.py` exercises producer buffering by sleeping
for a random 1–100 seconds before socket reads. The range, probability, random
seed, and maximum number of stalls are configurable. For example:

```bash
python3 mockConsumerSingleSocketBlocking.py connection.json received.bp \
    --public-key keys/mockkey.pub \
    --allow-ip-range 127.0.0.1/32 \
    --random-seed 42 --max-blocks 1
```

Run the deterministic long-buffer/short-buffer integration test with:

```bash
MOCKAPP_RUN_SOCKET_INTEGRATION=1 \
    python3 -m unittest -v test_buffered_single_socket.py
```

## Running without ADIOS2

The consumers do not require the `adios2` Python module. When it is unavailable,
an output argument such as `received.bp` is automatically changed to
`received.pkl`. The pickle file contains one dictionary per received step, with
the scalar `iteration` and array keys `d1` through `d6`. Read all steps from the
pickle stream with:

```python
import pickle

steps = []
with open("received.pkl", "rb") as stream:
    while True:
        try:
            steps.append(pickle.load(stream))
        except EOFError:
            break
```

For a consumer accepting connections from another host, specify both the bind
address and the host name or address that the producer can reach:

```bash
python3 mockConsumerSockets.py socket-connection.json received.bp \
    --public-key keys/mockkey.pub \
    --allow-ip-range PRODUCER_NETWORK/24 \
    --port 5000-5009 \
    --bind-host 0.0.0.0 --advertise-host RECEIVER_HOST \
    --timing-log consumer-timing.jsonl
```
