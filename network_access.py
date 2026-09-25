"""Low-cost source-address filtering for consumer sockets."""

import argparse
import ipaddress


def ipv4_network(value):
    try:
        network = ipaddress.ip_network(value, strict=False)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid IP range: {value}") from exc
    if network.version != 4:
        raise argparse.ArgumentTypeError("only IPv4 ranges are supported")
    return network


def accept_from_allowed_network(listener, allowed_networks):
    """Return the next allowed connection, silently closing other peers."""
    while True:
        connection, address = listener.accept()
        try:
            peer_address = ipaddress.ip_address(address[0])
        except ValueError:
            connection.close()
            continue
        if any(peer_address in network for network in allowed_networks):
            return connection, address
        connection.close()
