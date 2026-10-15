import unittest
from scripts.publish_tls_issuance_policy import publish


class TLSIntentPublicationTests(unittest.TestCase):
    def test_generation_cas_and_idempotency(self):
        current = {"generation": 1, "enabled": False}
        writes = []
        def api(method, path, body=None):
            if method == "GET": return current
            writes.append(body)
            return body
        self.assertFalse(publish(api, current)["changed"])
        self.assertFalse(writes)
        with self.assertRaises(ValueError): publish(api, {"generation": 3, "enabled": True})
        self.assertFalse(writes)
        self.assertTrue(publish(api, {"generation": 2, "enabled": True})["changed"])
        self.assertEqual(len(writes), 1)
