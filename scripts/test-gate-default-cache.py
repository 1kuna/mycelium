"""Regression for #1224: the default Go cache is trimmed to the gate floor, never in-use entries."""
import importlib.util
import os
import pathlib
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'default_cache', pathlib.Path(__file__).with_name('gate-default-cache.py'))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)
GIB = cache.GIB
NOW = 1_000_000.0


class DefaultCacheTests(unittest.TestCase):
    def test_gate_owned_cache_reuses_headroom_policy_and_keeps_recent_objects(self):
        spec = importlib.util.spec_from_file_location(
            'gate_cache', pathlib.Path(__file__).with_name('gate-cache.py'))
        gate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gate)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / 'ab').mkdir()
            old = root / 'ab' / ('a' * 64 + '-d')
            recent = root / 'ab' / ('b' * 64 + '-a')
            old.write_bytes(b'x' * 40)
            recent.write_bytes(b'x' * 20)
            os.utime(old, (NOW - 3 * cache.GO_MTIME_INTERVAL,) * 2)
            os.utime(recent, (NOW - 60,) * 2)
            evidence = root / 'timings.json'
            evidence.write_text('retained')
            self.assertEqual(gate.trim_cache(root, floor=50, free=100, now=NOW), 60)
            self.assertTrue(old.exists())
            self.assertEqual(gate.trim_cache(root, floor=50, free=40, now=NOW), 20)
            self.assertFalse(old.exists())
            self.assertTrue(recent.exists())
            self.assertEqual(evidence.read_text(), 'retained')

    def test_budget_restores_floor_plus_measured_hour_oldest_first(self):
        hour = cache.GO_MTIME_INTERVAL
        found = [(NOW - 3 * hour, 4 * GIB, 'old'), (NOW - 2 * hour, 4 * GIB, 'older-middle'),
                 (NOW - 1.5 * hour, 4 * GIB, 'middle'), (NOW - 600, 2 * GIB, 'recent')]
        # Ample disk: nothing goes, whatever the size.
        self.assertEqual(cache.plan(found, 100 * GIB, 45 * GIB, NOW), (14 * GIB, []))
        # Free 40 against floor 45 plus 2 GiB written this hour: evict 7, oldest first.
        budget, removals = cache.plan(found, 40 * GIB, 45 * GIB, NOW)
        self.assertEqual(budget, 7 * GIB)
        self.assertEqual([path for _, path in removals], ['old', 'older-middle'])

    def test_entries_inside_go_mtime_interval_are_never_removed(self):
        # Used five minutes ago, Go may not have refreshed the mtime for an hour.
        found = [(NOW - cache.IN_USE + 1, 4 * GIB, 'maybe-in-use'), (NOW - 60, 4 * GIB, 'new')]
        self.assertEqual(cache.plan(found, 0, 45 * GIB, NOW), (0, []))

    def test_trim_removes_only_go_objects_and_skips_gate_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            folder = root / 'ab'
            folder.mkdir()
            old = folder / ('a' * 64 + '-d')
            new = folder / ('b' * 64 + '-a')
            for path in (old, new):
                path.write_bytes(b'x' * 1024)
            (root / 'trim.txt').write_text('keep\n')
            (folder / 'notes').write_text('keep\n')
            os.utime(old, (NOW - 3 * cache.GO_MTIME_INTERVAL,) * 2)
            os.utime(new, (NOW - 60,) * 2)
            self.assertIsNone(cache.trim_default_cache(1 << 62, root, skip=(root,), cache=root, now=NOW))
            self.assertTrue(old.exists())
            budget, removed = cache.trim_default_cache(1 << 62, root, cache=root, now=NOW)
            self.assertEqual((budget, removed), (0, 1024))
            self.assertFalse(old.exists())
            for kept in (new, root / 'trim.txt', folder / 'notes'):
                self.assertTrue(kept.exists(), kept)


if __name__ == '__main__':
    unittest.main()
