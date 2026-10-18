import json
import unittest

from scripts.verify_k3s_transport import CONTROL_TESTS, FAULT_TESTS, REMOTE_TEST, evaluate_test_events


def events(action, tests, count=1):
    return [json.dumps({"Action": action, "Test": name}) for _ in range(count) for name in tests]


class TestTransportEvidence(unittest.TestCase):
    def baseline(self):
        return "\n".join(events("pass", CONTROL_TESTS) + events("fail", FAULT_TESTS | {REMOTE_TEST}) +
                         [json.dumps({"Action": "output", "Output": "unsafe_local_kubelet_fallback:\n"}) for _ in FAULT_TESTS])

    def test_original_failure_requires_actual_fallback_and_healthy_controls(self):
        self.assertTrue(evaluate_test_events(self.baseline(), 1, baseline=True, repetitions=1)["accepted"])
        for text, code in [("build failed", 1), (self.baseline(), 0),
                           (self.baseline().replace("unsafe_local_kubelet_fallback:", "unrelated failure:"), 1),
                           ("\n".join(events("fail", FAULT_TESTS | {REMOTE_TEST})), 1)]:
            with self.subTest(text=text[:50], code=code), self.assertRaises(ValueError):
                evaluate_test_events(text, code, baseline=True, repetitions=1)

    def test_candidate_requires_every_case_on_every_repetition(self):
        required = FAULT_TESTS | CONTROL_TESTS | {REMOTE_TEST}
        good = "\n".join(events("pass", required, 3))
        self.assertTrue(evaluate_test_events(good, 0, baseline=False, repetitions=3)["accepted"])
        for text, code in [(good, 1), ("\n".join(events("pass", required, 2)), 0),
                           (good + "\n" + events("skip", {"unexpected"})[0], 0),
                           (good + "\n" + events("fail", {"unexpected"})[0], 0)]:
            with self.subTest(code=code, length=len(text)), self.assertRaises(ValueError):
                evaluate_test_events(text, code, baseline=False, repetitions=3)


if __name__ == "__main__":
    unittest.main()
