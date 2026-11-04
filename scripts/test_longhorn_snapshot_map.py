import unittest
from scripts.longhorn_snapshot_map import intersection


class SparseIntersectionTests(unittest.TestCase):
    def test_fragmented_intersection_and_fold_direction(self):
        parent = [(0, 4096), (8192, 16384)]
        child = [(2048, 12288), (20480, 24576)]
        overlap = intersection(parent, child)
        self.assertEqual(overlap, 6144)
        child_bytes = sum(b - a for a, b in child)
        parent_bytes = sum(b - a for a, b in parent)
        self.assertEqual(child_bytes - overlap, 8192)
        self.assertEqual(parent_bytes - overlap, 6144)
        self.assertNotEqual(child_bytes - overlap, parent_bytes - overlap)

    def test_adjacent_and_empty_ranges_do_not_overlap(self):
        self.assertEqual(intersection([], [(0, 8)]), 0)
        self.assertEqual(intersection([(0, 4)], [(4, 8)]), 0)
        self.assertEqual(intersection([(0, 8), (16, 24)], [(4, 20)]), 8)


if __name__ == '__main__':
    unittest.main()
