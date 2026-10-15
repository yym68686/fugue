"""Bounded DNS observations, executed inside the selected Pod network namespace.

Uses only the Python runtime already required by the updater. Exit zero means
both DNS transports returned a matching, nonempty address answer, not merely
that a resolver executable ran successfully.
"""
import ipaddress
import json
import secrets
import socket
import struct
import sys
import time


def read_name(packet, offset):
    labels, seen, end = [], set(), None
    while True:
        if offset in seen or offset >= len(packet):
            raise ValueError("invalid DNS name pointer")
        seen.add(offset)
        size = packet[offset]
        offset += 1
        if size & 0xC0 == 0xC0:
            if offset >= len(packet):
                raise ValueError("truncated DNS pointer")
            if end is None:
                end = offset + 1
            offset = ((size & 0x3F) << 8) | packet[offset]
        elif size & 0xC0:
            raise ValueError("invalid DNS label")
        elif size == 0:
            return ".".join(labels).lower(), end or offset
        else:
            if offset + size > len(packet):
                raise ValueError("truncated DNS label")
            labels.append(packet[offset:offset + size].decode("ascii"))
            offset += size
            if sum(len(label) + 1 for label in labels) > 254:
                raise ValueError("DNS name too long")


def parse_answer(packet, query_id, name, qtype, expected=""):
    if len(packet) < 12:
        raise ValueError("truncated DNS header")
    ident, flags, questions, answers, _, _ = struct.unpack("!6H", packet[:12])
    if ident != query_id or not flags & 0x8000 or flags & 0x7800:
        raise ValueError("DNS response identity mismatch")
    if flags & 0x200:
        raise ValueError("truncated DNS response")
    if flags & 15:
        raise ValueError("DNS rcode=%d" % (flags & 15))
    if questions != 1:
        raise ValueError("DNS question count mismatch")
    question, offset = read_name(packet, 12)
    actual_type, actual_class = struct.unpack_from("!HH", packet, offset)
    offset += 4
    if (question, actual_type, actual_class) != (name.rstrip(".").lower(), qtype, 1):
        raise ValueError("DNS question mismatch")
    addresses, aliases = {}, {}
    for _ in range(answers):
        owner, offset = read_name(packet, offset)
        kind, cls, _, size = struct.unpack_from("!HHIH", packet, offset)
        offset += 10
        if offset + size > len(packet):
            raise ValueError("truncated DNS record")
        if cls == 1 and kind == qtype and size == (4 if qtype == 1 else 16):
            addresses.setdefault(owner, []).append(str(ipaddress.ip_address(packet[offset:offset + size])))
        elif cls == 1 and kind == 5:
            alias, end = read_name(packet, offset)
            if end != offset + size:
                raise ValueError("invalid CNAME length")
            aliases[owner] = alias
        offset += size
    owner = question
    visited = set()
    while owner in aliases and owner not in visited and len(visited) < 16:
        visited.add(owner)
        owner = aliases[owner]
    result = addresses.get(owner, [])
    if not result:
        raise ValueError("DNS returned no matching address answers")
    if expected and str(ipaddress.ip_address(expected)) not in result:
        raise ValueError("DNS address does not match observed Service ClusterIP")
    return result


def receive_exact(sock, count, deadline):
    result = b""
    while len(result) < count:
        sock.settimeout(max(0.001, deadline - time.monotonic()))
        if time.monotonic() >= deadline:
            raise TimeoutError("DNS TCP deadline exceeded")
        part = sock.recv(count - len(result))
        if not part:
            raise ValueError("DNS TCP connection closed before response")
        result += part
    return result


def observe(server, name, expected="", timeout=2, port=53):
    address = ipaddress.ip_address(server)
    qtype = 28 if expected and ipaddress.ip_address(expected).version == 6 else 1
    ident = secrets.randbelow(65536)
    wire_name = b"".join(bytes([len(part)]) + part.encode("ascii") for part in name.rstrip(".").split(".")) + b"\0"
    query = struct.pack("!6H", ident, 0x100, 1, 0, 0, 0) + wire_name + struct.pack("!HH", qtype, 1)
    observations = {}
    for transport, socktype in (("udp", socket.SOCK_DGRAM), ("tcp", socket.SOCK_STREAM)):
        try:
            deadline = time.monotonic() + timeout
            with socket.socket(socket.AF_INET6 if address.version == 6 else socket.AF_INET, socktype) as sock:
                sock.settimeout(timeout)
                sock.connect((str(address), port))
                if transport == "udp":
                    sock.send(query)
                    packet = sock.recv(65535)
                else:
                    sock.sendall(struct.pack("!H", len(query)) + query)
                    size = struct.unpack("!H", receive_exact(sock, 2, deadline))[0]
                    packet = receive_exact(sock, size, deadline)
                observations[transport] = {"status": "pass", "answers": parse_answer(packet, ident, name, qtype, expected)}
        except (OSError, ValueError, struct.error, UnicodeError) as exc:
            observations[transport] = {"status": "fail", "error": str(exc)}
    return {"ok": all(item["status"] == "pass" for item in observations.values()), "transports": observations}


if __name__ == "__main__":
    try:
        result = observe(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "")
    except (ValueError, OSError) as exc:
        result = {"ok": False, "error": str(exc)}
    print(json.dumps(result, separators=(",", ":")))
    sys.exit(0 if result["ok"] else 1)
