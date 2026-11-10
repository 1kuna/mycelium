#!/usr/bin/env python3
"""Native process inspection must preserve live fixtures and protected processes."""
import contextlib
import errno
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch


class HandleProtectionTests(unittest.TestCase):
    def test_protected_process_exclusions_require_system_identity_and_safe_ancestry(self):
        spec = importlib.util.spec_from_file_location('handles', Path(__file__).with_name('gate-handles.py'))
        handles = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handles)
        base = '1 0 /sbin/launchd /sbin/launchd\n'
        cases = [('409 1 /usr/libexec/UserEventAgent /usr/libexec/UserEventAgent', None),
                 ('409 1 /opt/fixture/python /opt/fixture/python', 'not a protected'),
                 ('409 800 /usr/bin/fixture /usr/bin/fixture\n800 1 /opt/python /opt/python gate-budget.py', 'descends'),
                 ('409 1 /usr/libexec/fixture /usr/libexec/fixture GATE_RUN_ID=owned', 'marker or path'),
                 ('409 1 /usr/libexec/fixture /usr/libexec/fixture /retained/source/input', 'marker or path')]
        for line, reason in cases:
            def ps(args, **kwargs):
                return line.split(None, 3)[2] if 'comm=' in args else base + line
            with self.subTest(line=line), patch.object(handles.subprocess, 'check_output', side_effect=ps), \
                    patch.object(handles.c, 'CDLL'), \
                    patch.object(handles, 'sealed_residency', return_value=dict(mount_on='/', readonly=True)), \
                    patch.object(handles, 'inventory_pid', side_effect=PermissionError(errno.EPERM, 'restricted')):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    if reason:
                        with self.assertRaisesRegex(RuntimeError, reason):
                            handles.inventory('/retained/source', [409], True, {800})
                    else:
                        handles.inventory('/retained/source', [409], True, {800})
                        row = json.loads(output.getvalue()[1:])
                        self.assertEqual(row['pid'], 409)
                        self.assertEqual(row['ancestry'], [409, 1])
                        self.assertFalse(row['gate_markers_or_paths'])
        with patch.object(handles.subprocess, 'check_output', side_effect=PermissionError('ps unavailable')):
            with self.assertRaises(PermissionError):
                handles.protected_process(409, '/retained/source', set())

    def test_protected_executable_requires_readonly_root_not_data_volume(self):
        spec = importlib.util.spec_from_file_location('mounts', Path(__file__).with_name('gate-handles.py'))
        handles = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(handles)
        with patch.object(handles.c, 'CDLL') as load:
            with self.assertRaisesRegex(RuntimeError, 'writable or auxiliary volume'):
                handles.sealed_residency('/System/Volumes/Data/fixture')
            load.assert_not_called()
            for mount, flags, accepts in ((b'/', 1, True), (b'/', 0, False),
                                           (b'/System/Volumes/Data', 1, False)):
                def observe(path, pointer):
                    info = handles.c.cast(pointer, handles.c.POINTER(handles.StatFS)).contents
                    info.mount_on, info.flags = mount, flags
                    return 0
                load.return_value.statfs64.side_effect = observe
                if accepts:
                    self.assertTrue(handles.sealed_residency('/usr/bin/fixture')['readonly'])
                else:
                    with self.assertRaisesRegex(RuntimeError, 'read-only system root'):
                        handles.sealed_residency('/usr/bin/fixture')


if __name__ == '__main__':
    unittest.main()
