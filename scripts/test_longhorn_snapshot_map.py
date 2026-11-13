import unittest
from unittest.mock import mock_open, patch
from scripts.longhorn_snapshot_map import filesystem, intersection


class SparseIntersectionTests(unittest.TestCase):
    def test_mount_resolution_uses_longest_real_mount_and_read_only_flag(self):
        mounts = ('1 0 0:1 / / rw - overlay overlay rw\n'
                  '2 1 9:3 / /host rw - ext4 /dev/md3 rw\n'
                  '3 2 9:4 / /host/readonly ro - ext4 /dev/md4 ro\n')
        with patch('builtins.open', mock_open(read_data=mounts)):
            self.assertEqual(filesystem('/host/var/lib/longhorn')['device'], '9:3')
            self.assertTrue(filesystem('/host/readonly/replicas')['read_only'])
            self.assertEqual(filesystem('/host-other')['type'], 'overlay')

    def test_escaped_mountpoint_is_resolved_and_unknown_filesystem_fails_closed(self):
        with patch('builtins.open', mock_open(read_data='2 1 9:3 / /disk\\040name rw - ext4 /dev/md3 rw\n')):
            self.assertEqual(filesystem('/disk name/replicas')['device'], '9:3')
            with self.assertRaisesRegex(ValueError, 'unknown'):
                filesystem('/different')

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
