"""Bounded Linux CT_GET for one held TCP tuple. No dump or mutation messages."""
import ipaddress
import secrets
import socket
import struct
import sys

# Linux UAPI: nfnetlink_conntrack.h, nfnetlink.h and nf_conntrack_tcp.h.
# The only outgoing operation is NFNL_SUBSYS_CTNETLINK/IPCTNL_MSG_CT_GET.
CT_GET = 0x101
CT_NEW_REPLY = 0x100
NLA_NESTED = 0x8000


def attribute(kind, payload):
    length = 4+len(payload)
    return struct.pack("=HH", length, kind)+payload+b"\0"*((-length)%4)


def attributes(raw):
    result = {}
    while raw:
        if len(raw) < 4:
            raise ValueError("truncated conntrack attribute")
        length, kind = struct.unpack("=HH", raw[:4])
        kind &= 0x3fff
        if length < 4 or length > len(raw) or kind in result:
            raise ValueError("ambiguous or malformed conntrack attribute")
        result[kind] = raw[4:length]
        raw = raw[(length+3)&~3:]
    return result


def tuple_attribute(source, destination, source_port, destination_port):
    addresses = attribute(1, ipaddress.IPv4Address(source).packed)+attribute(2, ipaddress.IPv4Address(destination).packed)
    ports = attribute(1, b"\x06")+attribute(2, struct.pack("!H", source_port))+attribute(3, struct.pack("!H", destination_port))
    return attribute(1|NLA_NESTED, addresses)+attribute(2|NLA_NESTED, ports)


def request(held, sequence):
    original = tuple_attribute(held.client_address, held.target_address, held.client_port, held.target_port)
    payload = struct.pack("!BBH", socket.AF_INET, 0, 0)+attribute(1|NLA_NESTED, original)
    return struct.pack("=IHHII", 16+len(payload), CT_GET, 1, sequence, 0)+payload


def parse_tuple(raw):
    value = attributes(raw)
    if value.get(3, b"\x00\x00") != b"\x00\x00":
        raise ValueError("conntrack tuple is in another zone")
    address, protocol = attributes(value[1]), attributes(value[2])
    if protocol.get(1) != b"\x06" or len(protocol.get(2, b"")) != 2 or len(protocol.get(3, b"")) != 2:
        raise ValueError("conntrack tuple is not complete TCP")
    return (str(ipaddress.IPv4Address(address[1])), str(ipaddress.IPv4Address(address[2])), struct.unpack("!H", protocol[2])[0], struct.unpack("!H", protocol[3])[0])


def parse_response(raw, sequence, held, pod_ip):
    if not 20 <= len(raw) <= 65536:
        raise ValueError("conntrack response size invalid")
    length, kind, flags, response_sequence, _ = struct.unpack("=IHHII", raw[:16])
    if response_sequence != sequence or length != len(raw) or flags & 2:
        raise ValueError("conntrack response does not match the exact query")
    if kind == 2:
        errno = struct.unpack("=i", raw[16:20])[0]
        raise OSError(-errno, "kernel CT_GET rejected the exact observation")
    if kind != CT_NEW_REPLY or raw[16:20] != struct.pack("!BBH", socket.AF_INET, 0, 0):
        raise ValueError("unexpected conntrack response type")
    value = attributes(raw[20:])
    if value.get(18, b"\x00\x00") != b"\x00\x00":
        raise ValueError("conntrack observation is in another zone")
    original, reply = parse_tuple(value[1]), parse_tuple(value[2])
    state = attributes(attributes(value[4])[1]).get(1)
    if state != b"\x03" or original != (held.client_address, held.target_address, held.client_port, held.target_port) or reply[0] != pod_ip or reply[2] != 443 or not 1 <= reply[3] <= 65535:
        raise ValueError("conntrack original/reply identity or established state differs")
    return reply[1], reply[3]


def read(held, pod_ip):
    if sys.platform != "linux" or not hasattr(socket, "AF_NETLINK"):
        raise OSError("Linux conntrack netlink observation unavailable")
    sequence = secrets.randbelow(2**32-1)+1
    message = request(held, sequence)
    with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, 12) as netlink:
        netlink.settimeout(3)
        netlink.bind((0, 0))
        netlink.sendto(message, (0, 0))
        raw, _, flags, sender = netlink.recvmsg(65536)
        if sender[0] != 0 or flags & socket.MSG_TRUNC:
            raise ValueError("conntrack response is truncated or not from kernel")
        return parse_response(raw, sequence, held, pod_ip)


if __name__ == "__main__":
    import argparse
    import json
    from types import SimpleNamespace
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=ipaddress.IPv4Address)
    parser.add_argument("--destination", required=True, type=ipaddress.IPv4Address)
    parser.add_argument("--source-port", required=True, type=int)
    parser.add_argument("--destination-port", required=True, type=int)
    parser.add_argument("--pod", required=True, type=ipaddress.IPv4Address)
    args = parser.parse_args()
    if not 1 <= args.source_port <= 65535 or not 1 <= args.destination_port <= 65535:
        raise SystemExit("bounded TCP ports required")
    held = SimpleNamespace(client_address=str(args.source), target_address=str(args.destination), client_port=args.source_port, target_port=args.destination_port)
    address, port = read(held, str(args.pod))
    print(json.dumps({"address": address, "port": port}))
