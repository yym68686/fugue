import json
import os
import tempfile
import unittest
from unittest import mock

from scripts import prepush


class CanonicalReceiptTest(unittest.TestCase):
    def test_command_environment_preserves_default_and_explicit_go_cache(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertNotIn("GOCACHE", prepush.command_env())
        explicit = "/tmp/fugue-explicit-go-cache"
        with mock.patch.dict(os.environ, {"GOCACHE": explicit}, clear=True):
            self.assertEqual(prepush.command_env()["GOCACHE"], explicit)

    def test_rejects_tuple_readback_for_json_array(self) -> None:
        with self.assertRaisesRegex(TypeError, "tuple"):
            prepush.validate_json_value({"images": ("api", "edge")}, "$")
        prepush.validate_json_value({"images": ["api", "edge"]}, "$")
        self.assertFalse(
            prepush.exact_json_types_equal(
                {"images": ["api", "edge"]},
                {"images": ("api", "edge")},
            )
        )

    def test_writer_round_trips_array_type_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "receipt.json")
            with mock.patch.dict(os.environ, {"PREPUSH_RECEIPT": path}):
                with self.assertRaisesRegex(TypeError, "tuple"):
                    prepush.write_receipt({"images": ("api", "edge")})
                self.assertFalse(os.path.exists(path))
                encoded = prepush.write_receipt({"images": ["api", "edge"]})
            with open(path, "rb") as handle:
                persisted = handle.read()
            self.assertEqual(encoded, persisted)
            self.assertEqual(json.loads(persisted), {"images": ["api", "edge"]})


if __name__ == "__main__":
    unittest.main()
