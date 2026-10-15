import socket
import struct
import threading
import unittest

import node_dns_probe as probe


def response(query, mode="answer"):
    ident = struct.unpack("!H", query[:2])[0]
    question = query[12:]
    flags = 0x8180 | (3 if mode == "nxdomain" else 0)
    if mode == "truncated":
        flags |= 0x200
    if mode == "wrong_id":
        ident ^= 1
    answer = b""
    if mode not in ("empty", "nxdomain"):
        answer = b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 30, 4) + socket.inet_aton("10.42.0.9")
    return struct.pack("!6H", ident, flags, 1, int(bool(answer)), 0, 0) + question + answer


class DNSProbeTests(unittest.TestCase):
    def query(self, udp_mode="answer", tcp_mode="answer", expected="10.42.0.9"):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp, socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
            tcp.bind(("127.0.0.1", 0))
            port = tcp.getsockname()[1]
            udp.bind(("127.0.0.1", port))
            tcp.listen(1)
            tcp.settimeout(2)
            udp.settimeout(2)

            def serve_udp():
                data, peer = udp.recvfrom(4096)
                if udp_mode != "timeout":
                    udp.sendto(response(data, udp_mode), peer)

            def serve_tcp():
                conn, _ = tcp.accept()
                with conn:
                    size = struct.unpack("!H", conn.recv(2))[0]
                    data = probe.receive_exact(conn, size, probe.time.monotonic() + 1)
                    result = response(data, tcp_mode)
                    wire = struct.pack("!H", len(result)) + result
                    # Exercise fragmented TCP framing, not just a single recv.
                    for chunk in (wire[:1], wire[1:5], wire[5:]):
                        conn.sendall(chunk)

            threads = [threading.Thread(target=serve_udp), threading.Thread(target=serve_tcp)]
            for thread in threads:
                thread.start()
            try:
                return probe.observe("127.0.0.1", "service.example", expected, timeout=0.15, port=port)
            finally:
                for thread in threads:
                    thread.join(2)

    def test_both_transports_without_external_resolver_tools(self):
        result = self.query()
        self.assertTrue(result["ok"], result)
        self.assertEqual(set(result["transports"]), {"udp", "tcp"})

    def test_no_answer_errors_mismatches_and_timeouts_never_pass(self):
        for mode in ("empty", "nxdomain", "wrong_id", "truncated", "timeout"):
            with self.subTest(mode=mode):
                result = self.query(udp_mode=mode)
                self.assertFalse(result["ok"], result)
                self.assertEqual(result["transports"]["udp"]["status"], "fail")
                self.assertEqual(result["transports"]["tcp"]["status"], "pass")
        self.assertFalse(self.query(tcp_mode="empty")["ok"])
        self.assertFalse(self.query(expected="10.42.0.10")["ok"])

    def test_malformed_compression_is_bounded(self):
        for packet in (b"\xc0\x00", b"\xc0", b"\x03ab", b"\x40"):
            with self.assertRaises(ValueError):
                probe.read_name(packet, 0)


if __name__ == "__main__":
    unittest.main()
