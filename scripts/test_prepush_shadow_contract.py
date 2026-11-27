import json
import unittest

from scripts import prepush_shadow_contract


def event(package, action, test=None, output=None):
    value = {"Package": package, "Action": action}
    if test is not None:
        value["Test"] = test
    if output is not None:
        value["Output"] = output
    return json.dumps(value)


class ShadowContractTest(unittest.TestCase):
    package = "fugue/internal/imagetracking"
    test = "TestTrackingWriteCapabilityIsWiredOnlyAtComposition"

    def test_requires_exact_test_and_package_success(self):
        events = "\n".join((
            event(self.package, "pass", self.test),
            event(self.package, "pass"),
        ))
        self.assertTrue(prepush_shadow_contract.exact_outcome(events, self.package, self.test, "pass"))
        self.assertFalse(prepush_shadow_contract.exact_outcome(
            event(self.package, "pass", self.test), self.package, self.test, "pass",
        ))

    def test_regression_witness_requires_expected_failure_diagnostic(self):
        events = "\n".join((
            event(self.package, "output", self.test,
                  "acquire image tracking write capability only in its composition adapter"),
            event(self.package, "fail", self.test),
            event(self.package, "fail"),
        ))
        self.assertTrue(prepush_shadow_contract.exact_outcome(
            events, self.package, self.test, "fail",
            "acquire image tracking write capability only in its composition adapter",
        ))
        self.assertFalse(prepush_shadow_contract.exact_outcome(
            events, self.package, self.test, "fail", "an unrelated failure",
        ))
        self.assertFalse(prepush_shadow_contract.exact_outcome(
            event(self.package, "fail"), self.package, self.test, "fail",
        ))


if __name__ == "__main__":
    unittest.main()
