import struct
import unittest
from types import SimpleNamespace
from scripts import read_front_conntrack as ct


class ConntrackReadTests(unittest.TestCase):
    def test_query_is_only_exact_ct_get_without_dump_or_mutation_flags(self):
        held = SimpleNamespace(client_address="192.0.2.1", target_address="192.0.2.9", client_port=43123, target_port=15443)
        message = ct.request(held, 7)
        length, kind, flags, sequence, pid = struct.unpack("=IHHII", message[:16])
        self.assertEqual((length, kind, flags, sequence, pid), (len(message), 0x101, 1, 7, 0))
        attributes = ct.attributes(message[20:])
        self.assertEqual(set(attributes), {1})
        self.assertEqual(ct.parse_tuple(attributes[1]), ("192.0.2.1", "192.0.2.9", 43123, 15443))

    def test_reply_must_match_kernel_sequence_established_flow_and_exact_candidate(self):
        held = SimpleNamespace(client_address="192.0.2.1", target_address="192.0.2.9", client_port=43123, target_port=15443)
        for bad in [None, "unicast multi flag", "sequence", "source", "candidate", "state", "zone", "duplicate", "extra message", "truncated", "permission"]:
            original = ct.tuple_attribute("192.0.2.2" if bad == "source" else held.client_address, held.target_address, held.client_port, held.target_port)
            reply = ct.tuple_attribute("10.0.0.3" if bad == "candidate" else "10.0.0.2", "10.0.1.1", 443, 43210)
            payload = b"\x02\x00\x00\x00"+ct.attribute(1|ct.NLA_NESTED, original)+ct.attribute(2|ct.NLA_NESTED, reply)+ct.attribute(4|ct.NLA_NESTED, ct.attribute(1|ct.NLA_NESTED, ct.attribute(1, b"\x04" if bad == "state" else b"\x03")))
            if bad == "zone": payload += ct.attribute(18, b"\x00\x02")
            if bad == "duplicate": payload += ct.attribute(1|ct.NLA_NESTED, original)
            if bad == "permission": payload = struct.pack("=i", -1)
            message = struct.pack("=IHHII", 16+len(payload), 2 if bad == "permission" else ct.CT_NEW_REPLY, 2 if bad == "unicast multi flag" else 0, 8 if bad == "sequence" else 7, 0)+payload
            if bad == "extra message": message += message
            if bad == "truncated": message = message[:-1]
            with self.subTest(bad=bad):
                if bad not in [None, "unicast multi flag"]:
                    with self.assertRaises((ValueError, OSError)): ct.parse_response(message, 7, held, "10.0.0.2")
                else:
                    self.assertEqual(ct.parse_response(message, 7, held, "10.0.0.2"), ("10.0.1.1", 43210))


if __name__ == "__main__": unittest.main()
