import unittest

from scripts.check_unselected_front_roll import check_sessions


HEADER = "sl local_address rem_address st"
LISTENERS = "0: 00000000:0050 00000000:0000 0A\n1: 00000000:01BB 00000000:0000 0A"
SOCKET = "2: 0100007F:01BB 0100007F:AC01 01"


class RolloutSessionTests(unittest.TestCase):
    def test_only_live_listener_side_sessions_count(self):
        tcp = f"{HEADER}\n{LISTENERS}\n{SOCKET}\n{HEADER}\n"
        outbound = "3: 0100007F:ABCD 0100007F:6F1B 01"
        time_wait = "4: 0100007F:01BB 0100007F:AC01 06"
        self.assertEqual(check_sessions(f"{tcp}{outbound}\n{time_wait}\n", 1), 1)
        with self.assertRaisesRegex(ValueError, "exceeding authorized limit"):
            check_sessions(f"{tcp}{SOCKET}\n", 1)

    def test_incomplete_inventory_never_means_zero(self):
        for tcp in (f"{HEADER}\n{LISTENERS}", f"{HEADER}\n{HEADER}"):
            with self.assertRaisesRegex(ValueError, "complete TCP inventories"):
                check_sessions(tcp, 1)
