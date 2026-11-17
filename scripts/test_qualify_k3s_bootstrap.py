import unittest

from scripts.qualify_k3s_bootstrap import VERSIONS, memory_bytes


class BootstrapQualificationTests(unittest.TestCase):
    def test_exact_adjacent_minor_versions(self):
        self.assertEqual(VERSIONS, ("v1.35.10+k3s1", "v1.36.6+k3s1", "v1.37.2+k3s1"))
        minors = [int(version.split(".")[1]) for version in VERSIONS]
        self.assertTrue(all(right == left + 1 for left, right in zip(minors, minors[1:])))

    def test_scheduler_memory_units(self):
        for raw, expected in [("0", 0), ("128Mi", 134217728), ("1Gi", 1073741824),
                              ("1024Ki", 1048576), ("1.5Gi", 1610612736),
                              ("128M", 128000000), ("1000m", 1), ("2k", 2000)]:
            with self.subTest(raw=raw):
                self.assertEqual(memory_bytes(raw), expected)

    def test_invalid_scheduler_memory_is_not_guessed(self):
        for value in ("unknown", "128MiB", "", "-1Mi"):
            with self.subTest(value=value), self.assertRaises(AssertionError):
                memory_bytes(value)


if __name__ == "__main__":
    unittest.main()
