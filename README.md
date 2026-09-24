# Mock Producer and Socket Consumers

Start the selected consumer before the producer so it can create the
connection-information file. The file's `id` selects the producer's I/O
implementation.

## Three-socket version

This version uses three TCP connections through one listening port: `d1/d2`,
`d3/d4`, and `d5/d6`. Its connection ID is `sockets`.

In the first terminal, start the consumer:

```bash
python3 mockConsumerSockets.py socket-connection.json received.bp \
    --timing-log consumer-timing.jsonl
```

The consumer writes `socket-connection.json` when it is ready. In a second
terminal, run the producer with a 512x512 array and 10 output steps:

```bash
python3 mockProducer.py socket-connection.json 512 512 10 \
    --timing-log producer-timing.jsonl
```

The producer recognizes the `"id": "sockets"` entry in the connection file and
opens the three data connections. It writes the initial state immediately and
then writes approximately every three seconds. The consumer stores the 10
received steps in `received.bp` using ADIOS BP5.

The timing logs are line-delimited JSON. The producer log records each socket
write call, and the consumer log records the three receives, their combined
duration, and each output write.

## Single-socket version

This version sends all six variables through one TCP connection. Its connection
ID is `singlesocket`.

In the first terminal, start the single-socket consumer:

```bash
python3 mockConsumerSingleSocket.py single-connection.json single-received.bp \
    --timing-log single-consumer-timing.jsonl
```

In a second terminal, run the producer with a 512x512 array and 10 output steps:

```bash
python3 mockProducer.py single-connection.json 512 512 10 \
    --timing-log single-producer-timing.jsonl
```

## Running without ADIOS2

The consumers do not require the `adios2` Python module. When it is unavailable,
an output argument such as `received.bp` is automatically changed to
`received.pkl`. The pickle file contains one dictionary per received step, with
keys `d1` through `d6`. Read all steps from the pickle stream with:

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
    --bind-host 0.0.0.0 --advertise-host RECEIVER_HOST \
    --timing-log consumer-timing.jsonl
```
