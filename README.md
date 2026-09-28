# Mock Producer and Socket Consumers

For a manually launched consumer, start it before the producer so it can create
the connection-information file. The file's `id` selects the producer's I/O
implementation. A producer can instead receive a `server.conf` file and launch
a single-socket consumer through SSH as described below. Generate a Curve25519
keypair once and keep the private key with the producer:

```bash
python3 keygen.py generate keys/mockkey.pub keys/mockkey
```

Blosc2 compression is optional. Install it in both the producer and consumer
Python environments before selecting the Blosc2 operation:

```bash
python3 -m pip install 'blosc2>=4,<5'
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
python3 mockConsumerSockets.py socket-connection.json received \
    --public-key keys/mockkey.pub \
    --allow-ip-range 127.0.0.1/32 \
    --port 5000-5009 \
    --file-interval-seconds 3600 \
    --timing-log consumer-timing.jsonl
```

The consumer writes `socket-connection.json` when it is ready. In a second
terminal, run the producer with a 512x512 array and 10 output steps:

```bash
python3 mockProducer.py socket-connection.json 512 512 10 \
    --private-key keys/mockkey \
    --compression-config conf/blosc2-zstd.conf \
    --timing-log producer-timing.jsonl
```

The producer recognizes the `"id": "sockets"` entry in the connection file and
opens the three data connections. It writes the initial state immediately and
then writes approximately every three seconds. The consumer stores the 10
received steps in timestamped BP5 outputs under `received/`. A new output is
opened every 3600 seconds by default; use `--file-interval-seconds` to choose a
different positive interval. Rotation occurs before the first step received at
or after the interval expires.

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
python3 mockConsumerSingleSocket.py single-connection.json single-received \
    --public-key keys/mockkey.pub \
    --allow-ip-range 127.0.0.1/32 \
    --port 5000-5009 \
    --timing-log single-consumer-timing.jsonl
```

In a second terminal, run the producer with a 512x512 array and 10 output steps:

```bash
python3 mockProducer.py single-connection.json 512 512 10 \
    --private-key keys/mockkey \
    --compression-config conf/blosc2-zstd.conf \
    --timing-log single-producer-timing.jsonl
```

The single-socket consumer acknowledges each step after its output write
finishes. This lets a producer with a socket timeout distinguish a completed
step from a stalled consumer or a connection that failed during transfer. The
connection information advertises protocol version 4, whose array framing can
carry encoded-payload lengths and operation metadata. The producer rejects
older rendezvous files, so the producer and consumer must be upgraded together.

## Payload operations and compression

Socket payload transformations implement the generic `DataOperation`
interface in `data_operations.py`. An operation encodes a contiguous NumPy
array and supplies a self-describing wire record; the corresponding registered
decoder reconstructs the array before the consumer writes it. This boundary
can also accommodate future lossy compressors or data-refactoring libraries.

The included implementation uses Blosc2 with its Zstandard codec. Its settings
live in a separate configuration file:

```ini
[compression]
implementation = blosc2
codec = zstd
compression_level = 1
filter = shuffle
threads = 1
```

`conf/blosc2-zstd.conf` contains these recommended settings. `shuffle` uses
each array's NumPy element size automatically, so the same operation handles
the current `float64` arrays and later `int16` traces. Compression runs in the
existing output worker. If a particular array would grow after compression,
the operation sends that array uncompressed instead. The scalar iteration
value will normally take this raw fallback.

For a manually launched consumer, select the file with the producer's
`--compression-config FILE` option, as in the examples above. Omitting the
option sends every array uncompressed.

## Launching and recovering a consumer through SSH

Pass a configuration file containing a `[server]` section as the producer's
destination. The checked-in `server.conf` is configured for `ubuntupc`:

```ini
[server]
host = ubuntupc
working_directory = /home/pnorbert/Software/LAPD/mock_data_streamer
python = /home/pnorbert/Software/LAPD/.venv/bin/python3
script = mockConsumerSingleSocket.py
compression_config = conf/blosc2-zstd.conf
connection_file = conn.json
output_directory = output
file_interval_seconds = 3600
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

For a server configuration, `compression_config` is resolved relative to the
server configuration file on the producer. The remote consumer does not read
that file: the authenticated producer includes the operation description with
each encoded array, and the consumer selects the registered decoder from that
description. Omit `compression_config` to disable transformations.

The SSH client must already be able to authenticate non-interactively. Paths
other than `working_directory` are interpreted on the remote host. Multiple
allowed networks can be written as a comma-separated list. `ssh_command` may
be set when SSH options or a different executable are needed.

The data connection is direct by default. If the producer can SSH to the
consumer host but cannot connect to its listening port (for example, when SSH
uses a bastion), route the single-socket stream through the same SSH command:

```ini
socket_transport = ssh
bind_host = 127.0.0.1
advertise_host = 127.0.0.1
allow_ip_range = 127.0.0.1/32
```

This runs an OpenSSH stdio forward equivalent to `ssh -W HOST:PORT` and uses
the host, keys, `ProxyJump`, and other options from the normal SSH
configuration. No local listening port is allocated. The SSH forwarding
process is closed and re-created along with each consumer connection. Both
transport modes keep a private OpenSSH control connection alive for remote
consumer launches. In SSH transport mode the forwarding channels also share
that connection, so a load-balanced SSH destination cannot send them to
different servers. Use `socket_transport = direct` (or omit the setting) when
the producer can reach the advertised socket normally; the control connection
is retained for service restarts even though data does not travel through it.

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
new encrypted connection information is printed back to the producer, that
SSH channel exits; the shared control connection stays alive until the
producer closes, while the consumer itself does not depend on either one.

`socket_timeout_seconds` bounds every socket operation, including the
per-step acknowledgement. If the consumer stalls past that timeout or the TCP
connection breaks, the producer closes the failed channel, launches a new
consumer, and retries only the interrupted in-flight snapshot. Snapshots that
were already acknowledged are not resent. The existing buffered-output queue
then drains its unsent snapshots in FIFO order before sending newer steps; the
simulation is never recomputed. Every consumer process creates a new output
named with its current timestamp in `output_directory`. A replacement never
appends to an existing output, so a crash or interrupted write cannot cause it
to reopen a potentially damaged file. The failed in-flight snapshot is retried
into the replacement's new output; outputs from before the reconnect remain
unchanged. A zero `max_relaunch_attempts` retries indefinitely; a positive
value limits each series of SSH launch attempts. When the configured local SSH
certificate is expired, one failed launch is enough to pause further SSH
attempts. The producer checks only the local certificate while paused and
resumes launching after it detects a different certificate that is currently
valid.
Relaunch and retry activity is recorded as `consumer.connection_lost`,
`consumer.launch`, `consumer.launch_retry`, `ssh_key.wait`, `ssh_key.updated`,
and `io.retry` events in the producer timing log.

### ntfy notifications

Remote-server configurations can enable best-effort ntfy notifications:

```ini
ntfy_topic_info = my-info-topic
# SSH certificate issues and failed restarts are sent here:
ntfy_topic_alert = my-alert-topic
# OpenSSH certificate created by NERSC sshproxy:
ssh_key_certificate = ~/.ssh/nersc-cert.pub
# Optional settings shown with their defaults:
ntfy_server_url = https://ntfy.sh
ntfy_token_env = NTFY_TOKEN
ntfy_timeout_seconds = 5
ssh_key_check_interval_seconds = 60
```

Notifications are sent asynchronously to `ntfy_topic_info` when the remote
consumer starts, restarts, or disconnects. Buffer notifications at 20%, 40%,
and 60% are each sent once per producer run. While the buffer is at least 80%
full, a high-buffer notification is sent immediately and then at most once per
hour. An hourly notification reports the number of snapshots discarded since
the preceding high-buffer notification when that number is nonzero. Consumer
disconnect and buffer-loss reports remain on the info topic.

When `ssh_key_certificate` and `ntfy_topic_alert` are set, the producer starts
a separate monitor process. It obtains the certificate's actual end time
locally with `ssh-keygen -L` and sends one alert at 6, 3, 2, and 1 hour before
expiration, plus an alert if the certificate is expired, missing, invalid, or
not yet valid. The first failed remote-consumer restart attempt in each
reconnect episode is also sent to the alert topic; subsequent attempts in that
episode do not send duplicate alerts.
The monitor re-reads the file on every check, so replacing the certificate with
`sshproxy` resets the warning schedule without restarting the producer. The
monitor stops with the producer and never opens an SSH connection.

Notification failures do not interrupt output. Notifications sent by the main
producer are recorded as `notification.failed` events in its timing log. Set
the access token in the environment rather than the configuration file, for example
`export NTFY_TOKEN=tk_...` before starting the producer.

### Deliberately blocking test consumer

`mockConsumerSingleSocketBlocking.py` exercises producer buffering by sleeping
for a random 1–100 seconds before socket reads. The range, probability, random
seed, and maximum number of stalls are configurable. For example:

```bash
python3 mockConsumerSingleSocketBlocking.py connection.json received \
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
each timestamped output under a directory such as `received/` uses the `.pkl`
suffix instead of `.bp`. Each pickle file contains one dictionary per received
step, with the scalar `iteration` and array keys `d1` through `d6`. Read all
steps from one pickle stream with:

```python
import pickle

steps = []
with open("received/20260925T120000.000000-0400.pkl", "rb") as stream:
    while True:
        try:
            steps.append(pickle.load(stream))
        except EOFError:
            break
```

For a consumer accepting connections from another host, specify both the bind
address and the host name or address that the producer can reach:

```bash
python3 mockConsumerSockets.py socket-connection.json received \
    --public-key keys/mockkey.pub \
    --allow-ip-range PRODUCER_NETWORK/24 \
    --port 5000-5009 \
    --bind-host 0.0.0.0 --advertise-host RECEIVER_HOST \
    --timing-log consumer-timing.jsonl
```
