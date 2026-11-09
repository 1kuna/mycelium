#!/usr/bin/env python3
"""Protect admission, disk refusal, and eviction boundaries on shared hosts."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


def load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


capacity = load('gate-capacity')
local = load('gate-local')


class LocalGateTests(unittest.TestCase):
    def test_legacy_owners_are_preserved_and_new_owner_releases_only_its_lane(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            legacy = root / 'local-go-slot'
            legacy.mkdir()
            (legacy / 'owner').write_text('another owner')
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                slot = capacity.acquire(root)
                self.assertEqual(slot.directory.name, 'local-go-slot-2')
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root)
                slot.release()
            self.assertEqual((legacy / 'owner').read_text(), 'another owner')

    def test_full_is_serial_until_measured_and_custody_failure_holds_capacity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                first = capacity.acquire(root, mode='full')
                with self.assertRaisesRegex(RuntimeError, 'capacity busy'):
                    capacity.acquire(root, mode='full')
                with contextlib.redirect_stdout(io.StringIO()):
                    first.release(stopped=False)
                self.assertTrue(first.directory.exists())
                self.assertTrue(first.full.exists())
                first.release()

    def test_release_waiter_gets_next_lane_before_normal_work(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            waiter = root / 'local-go-release-wait-other'
            waiter.touch()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity, 'available_memory', return_value=64 * capacity.GIB)):
                with self.assertRaisesRegex(RuntimeError, 'reserved for release'):
                    capacity.acquire(root)
                release = capacity.acquire(root, priority='release')
                release.release()
            self.assertTrue(waiter.exists())

    def test_disk_refusal_starts_nothing(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            disk = type('Disk', (), {'free': capacity.ENTRY_FLOOR - 1})()
            with (patch.object(capacity, 'root', return_value=root),
                  patch.object(capacity.shutil, 'disk_usage', return_value=disk)):
                with self.assertRaisesRegex(RuntimeError, 'entry floor'):
                    capacity.acquire(root)
            self.assertFalse(any(root.glob('local-go-slot*')))

    def test_trim_removes_only_old_rebuildable_go_objects(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            bucket = root / 'ab'
            bucket.mkdir()
            old = bucket / ('a' * 64 + '-d')
            new = bucket / ('b' * 64 + '-a')
            old.write_bytes(b'old cache')
            new.write_bytes(b'new')
            os.utime(old, (1, 1))
            evidence = root / 'myce-upgrade-compat'
            evidence.mkdir()
            (evidence / 'source.tar').write_text('retained')
            sentinel = bucket / 'unrelated'
            sentinel.write_text('keep')
            self.assertEqual(local.trim_cache(root, limit=3), 3)
            self.assertFalse(old.exists())
            self.assertTrue(new.exists())
            self.assertEqual(sentinel.read_text(), 'keep')
            self.assertEqual((evidence / 'source.tar').read_text(), 'retained')


if __name__ == '__main__':
    unittest.main()
