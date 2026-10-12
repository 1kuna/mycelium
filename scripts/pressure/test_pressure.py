from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from pressure import Checker, digest, drm_identity, drm_inventory, drm_owners, emit, replay, validate_save


def event(kind: str, at: float = 0, **fields: object) -> dict:
    return dict(kind=kind, at=at, boot="test-boot", **fields)


def beginning(c: Checker, resumed: int = 0, attempt: int = 1) -> None:
    if attempt == 1:
        c.feed(event("submit", job="j", priority="production", units=2))
    c.feed(event("start", job="j", attempt=attempt, resumed=resumed, units=2,
                 contract="c", resumable=True, card="1"))


class EvidenceTests(unittest.TestCase):
    def test_ram_settlement_accounts_only_reclaimable_cache(self):
        from stage_ab import ram_settlement
        mib=1024**2
        receipt=ram_settlement(900*mib, {'inactive_file':700*mib,'slab_reclaimable':10*mib},100*mib)
        self.assertTrue(receipt['ram_released'])
        self.assertFalse(ram_settlement(900*mib, {'file':700*mib},100*mib)['ram_released'])
        self.assertFalse(ram_settlement(900*mib, {'inactive_file':1000*mib},100*mib)['ram_released'])

    def test_followon_success_always_cleans_scenario(self):
        import inspect, stage_ab
        source=inspect.getsource(stage_ab.Run.followons)
        self.assertIn('finally:',source)
        self.assertIn('self.scenario_cleanup()',source.split('finally:')[-1])

    def test_kernel_threads_and_non_gpu_processes_need_no_identity(self):
        from unittest.mock import patch
        import kernel_reads
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for n in ('2','17','18'):(root/n/'fd').mkdir(parents=True)
            (root/'17/fd/1').symlink_to('/dev/null')
            (root/'18/fd/1').symlink_to('/dev/null')
            original=kernel_reads._read
            def read(path,op):
                if path.name in ('stat','exe','cmdline','cgroup','fdinfo'):
                    self.fail('irrelevant process identity inspected: '+str(path))
                return original(path,op)
            with patch('kernel_reads._read',side_effect=read):
                self.assertEqual(drm_inventory(['0000:07:00.0'],root),[])

    def test_candidate_vanish_rescans_and_live_permission_fails_closed(self):
        import errno
        from unittest.mock import patch
        import kernel_reads
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);pid=root/'17';(pid/'fd').mkdir(parents=True);(pid/'fdinfo').mkdir()
            (pid/'fd/1').symlink_to('/dev/dri/renderD129')
            (pid/'fdinfo/1').write_text('drm-pdev: 0000:07:00.0\ndrm-client-id: 1\n')
            (pid/'stat').write_text('17 (test) '+' '.join(['S','1','17']+['0']*16+['42']))
            (pid/'exe').symlink_to('/usr/bin/synthetic');(pid/'cgroup').write_text('0::/test.scope')
            original=kernel_reads._read
            seen=[]
            def vanished(path,op):
                seen.append(path)
                if path==pid/'exe':
                    (pid/'fd/1').unlink()
                    raise FileNotFoundError(errno.ENOENT,'vanished')
                return original(path,op)
            with patch('kernel_reads._read',side_effect=vanished):
                self.assertEqual(drm_inventory(['0000:07:00.0'],root),[])
            self.assertGreaterEqual(seen.count(pid/'fd'),2)
            (pid/'fd/1').symlink_to('/dev/dri/renderD129')
            def denied(path,op):
                if path==pid/'exe':raise PermissionError(errno.EACCES,'denied')
                return original(path,op)
            with patch('kernel_reads._read',side_effect=denied):
                with self.assertRaises(PermissionError):drm_inventory(['0000:07:00.0'],root)

    def test_collector_allowlist_inspects_only_supplied_gpu_holders(self):
        import stage_ab
        from types import SimpleNamespace
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            receipt=Path(tmp)/'events.jsonl'
            receipt.write_text(json.dumps(dict(kind='start',pid=17,start_ticks=42))+'\n')
            run=stage_ab.Run.__new__(stage_ab.Run)
            run.receipts=[receipt];run.cgroup='0::/test';run.worker_cgroup=None
            with patch('stage_ab.process',side_effect=AssertionError('irrelevant identity read')):
                self.assertEqual(run.owned([]),[])

    def test_selected_pci_fd_filter_does_not_inspect_gpu0_identity(self):
        from unittest.mock import patch
        pid=Path('/proc/17');sysdir=Path('/sys/bus/pci/devices/0000:07:00.0/drm')
        def read(path,op='text'):
            if path==sysdir:return [sysdir/'card1',sysdir/'renderD129']
            if path==Path('/proc'):return [pid]
            if path==pid/'fd':return [pid/'fd/1']
            if path==pid/'fd/1':return '/dev/dri/renderD128'
            self.fail('GPU0 process inspected beyond fd: '+str(path))
        with patch('pressure.sys.platform','linux'),patch('pressure.kernel_read',side_effect=read):
            self.assertEqual(drm_inventory(['0000:07:00.0']),[])

    def test_collector_soak_gates_scenarios_on_zero_holds(self):
        import collector_soak
        from unittest.mock import patch,Mock
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'display-baseline.json').write_text('[]')
            (root/'diagnostic-executables.json').write_text('{}')
            readers=Mock(pending={})
            with patch('collector_soak.DiagnosticReaders',return_value=readers),patch('collector_soak.drm_owners',side_effect=PermissionError(13,'unknown candidate')):
                result=collector_soak.soak(root,duration=.01,interval=.001)
            self.assertEqual(result['holds'],1)
            self.assertTrue((root/'COLLECTOR-FAILED').exists())
            def owners(*_,**kw):
                kw['evidence'].update(holders=[],baseline_clients=[])
                return {'0000:07:00.0':[]}
            with patch('collector_soak.DiagnosticReaders',return_value=readers),patch('collector_soak.drm_owners',side_effect=owners):
                result=collector_soak.soak(root,duration=.01,interval=.001)
            self.assertEqual(result['result'],'PASS');self.assertEqual(result['holds'],0)
            self.assertGreaterEqual(result['seconds'],.01)

    def test_foreign_reader_pause_continues_only_after_exit_and_memory_release(self):
        import pressure
        with tempfile.TemporaryDirectory() as tmp:
            gate=pressure.ForeignReaderPause(Path(tmp)/'events')
            self.assertTrue(gate.update(True, False, 10))
            self.assertTrue(gate.update(False, False, 12))
            self.assertTrue(gate.update(False, False, 70))
            self.assertFalse(gate.update(False, True, 70))
            self.assertEqual(gate.interruptions, 1)
            rows=[json.loads(v) for v in (Path(tmp)/'events').read_text().splitlines()]
            self.assertEqual([v['action'] for v in rows], ['paused','continued'])
            self.assertEqual(rows[-1]['status'], 'interrupted-by-foreign-reader, continued')

    def test_foreign_reader_pause_holds_after_sixty_seconds(self):
        import pressure
        for pending,released in ((True,False),(False,False)):
            with self.subTest(pending=pending),tempfile.TemporaryDirectory() as tmp:
                gate=pressure.ForeignReaderPause(Path(tmp)/'events')
                gate.update(True, False, 0)
                self.assertTrue(gate.update(pending,released,60))
                with self.assertRaisesRegex(ValueError,'60 seconds'):
                    gate.update(pending,released,60.01)

    def test_all_readonly_diagnostics_and_no_mutating_or_file_commands(self):
        import pressure
        for args in ([b'stats',b'-d',b'0',b'-j'], [b'discovery'],
                     [b'discovery',b'-j'], [b'dump',b'-d',b'1',b'-m',b'0,18',b'-n',b'5']):
            with self.subTest(args=args):self.assertTrue(pressure.diagnostic_readonly(args))
        for args in ([b'config',b'-d',b'1'],[b'dump',b'--file',b'anything'],
                     [b'stats',b'--reset'],[b'discovery',b'-d',b'bad']):
            with self.subTest(args=args):self.assertFalse(pressure.diagnostic_readonly(args))

    def test_round4_diagnostic_reader_identity_memory_and_lifetime(self):
        from unittest.mock import patch
        import hashlib
        import pressure
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);exe=root/'xpu-smi';exe.write_bytes(b'qualified diagnostic binary')
            pid=root/'1670427';pid.mkdir();(pid/'exe').symlink_to(exe)
            fields=['S','1670164','1670427']+['0']*16+['3571851']
            (pid/'stat').write_text('1670427 (xpu-smi) '+' '.join(fields))
            (root/'uptime').write_text('35718.7 0')
            holder=dict(boot='test-boot',pid=1670427,start=3571851,ppid=1670164,
                exe=str(exe),cgroup='0::/user.slice/user-1000.slice/session-1136.scope',stats_readonly=True,
                clients=[dict(pci='0000:07:00.0',device='/dev/dri/renderD129',client=28670,
                    resident_bytes={'drm-resident-system':65536,'drm-resident-gtt':40960,
                        'drm-resident-vram0':2846720,'drm-resident-stolen':0})])
            events=root/'diagnostics.jsonl'
            readers=pressure.DiagnosticReaders({str(exe):hashlib.sha256(exe.read_bytes()).hexdigest()},events,root)
            def scan(h):
                with patch('pressure.drm_inventory',return_value=h):
                    return pressure.drm_owners(['0000:07:00.0'],allowed=[],diagnostics=readers)
            # The real round-5 allocated-system footprint is valid under calibration.
            holder['clients'][0]['allocated_bytes']={'drm-total-system':192937984}
            self.assertEqual(scan([holder]),{'0000:07:00.0':[]})
            self.assertEqual(json.loads(events.read_text().splitlines()[0])['action'],'observed')
            for change in (dict(exe='/untrusted/xpu-smi'),dict(stats_readonly=False),
                    dict(clients=[{**holder['clients'][0],'resident_bytes':{'drm-resident-vram0':readers.limits['max_memory_bytes']+1,'drm-resident-gtt':0}}]),
                    dict(clients=[{**holder['clients'][0],'allocated_bytes':{'drm-total-vram0':readers.limits['max_memory_bytes']+1}}]),
                    dict(clients=[{**holder['clients'][0],'resident_bytes':{}}])):
                with self.subTest(change=change),self.assertRaisesRegex(ValueError,'UNKNOWN'):
                    scan([{**holder,**change}])
            exe.write_bytes(b'tampered binary')
            with self.assertRaisesRegex(ValueError,'UNKNOWN'):scan([holder])
            exe.write_bytes(b'qualified diagnostic binary')
            # A live diagnostic remains accountable even after closing its DRM descriptor.
            (root/'uptime').write_text('35780 0')
            with self.assertRaisesRegex(ValueError,'UNKNOWN'):scan([])
            (pid/'stat').unlink();(pid/'exe').unlink();pid.rmdir()
            self.assertEqual(scan([]),{'0000:07:00.0':[]})
            self.assertEqual(json.loads(events.read_text().splitlines()[-1])['action'],'exited')

    def test_diagnostic_inventory_parses_short_json_and_allocated_memory(self):
        import pressure
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);pid=root/'17';(pid/'fd').mkdir(parents=True);(pid/'fdinfo').mkdir()
            (pid/'stat').write_text('17 (xpu-smi) '+' '.join(['S','1','17']+['0']*16+['42']))
            (pid/'exe').symlink_to('/usr/bin/xpu-smi');(pid/'cgroup').write_text('0::/session.scope')
            (pid/'fd/1').symlink_to('/dev/dri/renderD129')
            (pid/'fdinfo/1').write_text('drm-pdev: 0000:07:00.0\ndrm-client-id: 28670\n'
                'drm-resident-vram0: 2780 KiB\ndrm-resident-gtt: 40 KiB\ndrm-total-vram0: 8 MiB\n'
                'drm-total-cycles-rcs: 700892831130\n')
            for flag in (b'-j',b'--json'):
                (pid/'cmdline').write_bytes(b'/usr/bin/xpu-smi\0stats\0-d\0'+b'0\0'+flag+b'\0')
                h=pressure.drm_inventory(['0000:07:00.0'],root)[0]
                self.assertTrue(h['stats_readonly'])
                self.assertEqual(h['clients'][0]['allocated_bytes']['drm-total-vram0'],8*1024**2)
            (pid/'cmdline').write_bytes(b'/usr/bin/xpu-smi\0config\0-d\0'+b'0\0-j\0')
            self.assertFalse(pressure.drm_inventory(['0000:07:00.0'],root)[0]['stats_readonly'])

    def test_vanished_fd_rescans_even_when_fdinfo_lingers(self):
        import errno
        from unittest.mock import patch
        for code in (errno.ENOENT, errno.ESRCH):
            with self.subTest(errno=code), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); p = root / '1115'
                (p / 'fd').mkdir(parents=True); (p / 'fdinfo').mkdir()
                (p / 'stat').write_text('1115 (test) ' + ' '.join(['S','1','1115'] + ['0']*16 + ['42']))
                (p / 'fdinfo/11').write_text('pos: 0\n')
                scan = os.scandir
                with patch('pressure.os.scandir', wraps=scan) as rescans, patch('pressure.os.readlink', side_effect=OSError(code, 'closed')):
                    self.assertEqual(drm_owners(['0000:07:00.0'], root), {'0000:07:00.0': []})
                    self.assertIn(p / 'fd', [call.args[0] for call in rescans.call_args_list])

    def test_process_vanishing_mid_scan_is_confirmed_by_rescan(self):
        import errno
        from unittest.mock import patch
        for code in (errno.ENOENT, errno.ESRCH):
            with self.subTest(errno=code), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); p = root / '1115'; p.mkdir()
                with patch('kernel_reads._read',side_effect=[[p],OSError(code,'exited'),[]]) as rescans, patch('kernel_reads._rescan',return_value=False):
                    self.assertEqual(drm_owners(['0000:07:00.0'], root), {'0000:07:00.0': []})
                    self.assertEqual(rescans.call_count, 3)

    @unittest.skipUnless(Path('/proc/self/fdinfo').is_dir(), 'Linux procfs required')
    def test_drm_collector_does_not_race_its_own_directory_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / str(os.getpid())).symlink_to(Path('/proc') / str(os.getpid()))
            self.assertEqual(drm_owners(['0000:07:00.0'], root), {'0000:07:00.0': []})

    @unittest.skipUnless(Path('/proc/self/stat').exists(), 'Linux procfs required')
    def test_confirmed_closed_descriptor_does_not_become_unknown_owner(self):
        from unittest.mock import patch
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);pid=root/str(os.getpid());pid.mkdir();(pid/'fdinfo').mkdir();(pid/'fd').mkdir()
            (pid/'stat').write_bytes(Path('/proc/self/stat').read_bytes())
            entry=SimpleNamespace(name='99999',path=str(pid/'fdinfo/99999'))
            class Scan:
                def __enter__(self):return [entry]
                def __exit__(self,*_):pass
            scan = os.scandir
            def rescan(path):
                return Scan() if path == pid / 'fd' else scan(path)
            with patch('pressure.os.scandir',side_effect=rescan):
                self.assertEqual(drm_owners(['0000:07:00.0'],root), {'0000:07:00.0':[]})
            with patch('pressure.os.scandir',side_effect=rescan), patch('pressure.os.readlink',side_effect=PermissionError(13,'denied')):
                with self.assertRaises(PermissionError):drm_owners(['0000:07:00.0'],root)

    def test_drm_numeric_identity_and_multiple_processes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, pci = Path(tmp), "0000:07:00.0"
            def process(pid: int, group: int, start: int) -> None:
                path = root / str(pid)
                (path / "fdinfo").mkdir(parents=True)
                (path / "fd").mkdir()
                (path / "fd/9").symlink_to("/dev/dri/renderD129")
                fields = ["S", "1", str(group)] + ["0"] * 16 + [str(start)]
                (path / "stat").write_bytes(str(pid).encode() + b" (spaced ) non-utf8 \xff) " + " ".join(fields).encode())
                (path / "exe").symlink_to("/usr/bin/test-runtime")
                (path / "cgroup").write_text("0::/test.scope")
                (path / "fdinfo/9").write_text("drm-pdev:\t" + pci + "\ndrm-client-id: 88\n")
            process(100, 100, 10)
            process(101, 100, 11)
            self.assertEqual(len(drm_owners([pci], root)[pci]), 2)
            process(200, 200, 20)
            self.assertEqual(len(drm_owners([pci], root)[pci]), 3)
            (root / "100/stat").write_text("malformed")
            with self.assertRaises((IndexError, ValueError)):
                drm_owners([pci], root)

    def test_reused_non_gpu_descriptor_is_read_again(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);p=root/'1115';(p/'fd').mkdir(parents=True);(p/'fdinfo').mkdir()
            fields=['S','1','1115']+['0']*16+['42']
            (p/'stat').write_text('1115 (polkitd) '+' '.join(fields))
            (p/'fd/11').symlink_to('/dev/null');(p/'fdinfo/11').write_text('pos: 0\n')
            with patch('pressure.os.readlink',side_effect=[FileNotFoundError(2,'closed'),'/dev/null']):
                self.assertEqual(drm_owners(['0000:07:00.0'],root),{'0000:07:00.0':[]})

    def test_memory_units_are_explicit_and_unknown_units_refuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);p=root/'17';(p/'fd').mkdir(parents=True);(p/'fdinfo').mkdir()
            fields=['S','1','17']+['0']*16+['42']
            (p/'stat').write_text('17 (test) '+' '.join(fields));(p/'exe').symlink_to('/test')
            (p/'cgroup').write_text('0::/test');(p/'fd/1').symlink_to('/dev/dri/card1')
            info=p/'fdinfo/1'
            for unit,multiplier in [('B',1),('bytes',1),('KiB',1024),('MiB',1024**2),('GiB',1024**3)]:
                info.write_text('drm-pdev: 0000:07:00.0\ndrm-client-id: 1\ndrm-resident-vram0: 2 '+unit+'\n')
                self.assertEqual(drm_inventory(['0000:07:00.0'],root)[0]['clients'][0]['resident_bytes']['drm-resident-vram0'],2*multiplier)
            info.write_text('drm-pdev: 0000:07:00.0\ndrm-client-id: 1\ndrm-resident-vram0: 2 mystery\n')
            with self.assertRaisesRegex(ValueError,'unsupported DRM memory units'):
                drm_inventory(['0000:07:00.0'],root)

    def test_real_b70_shared_display_shape_and_unknown_holder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, pci = Path(tmp), "0000:07:00.0"
            for pid, start, exe, cgroup, copies in (
                (1, 14, "/usr/lib/systemd/systemd", "0::/init.scope", 1),
                (1138, 848, "/usr/lib/systemd/systemd-logind", "0::/system.slice/systemd-logind.service", 1),
                (2458, 1013, "/usr/bin/gnome-shell", "0::/user.slice/user-1000.slice/user@1000.service/session.slice/org.gnome.Shell@wayland.service", 5)):
                p = root / str(pid)
                (p / "fd").mkdir(parents=True); (p / "fdinfo").mkdir()
                fields = ["S", "1", str(pid)] + ["0"] * 16 + [str(start)]
                (p / "stat").write_text(f"{pid} (display) " + " ".join(fields))
                (p / "exe").symlink_to(exe); (p / "cgroup").write_text(cgroup)
                for fd in range(copies):
                    (p / "fd" / str(fd)).symlink_to("/dev/dri/card1")
                    (p / "fdinfo" / str(fd)).write_text(
                        f"drm-driver: xe\ndrm-pdev: {pci}\ndrm-client-id: 42\n"
                        "drm-resident-vram0: 5352 KiB\ndrm-resident-gtt: 20 KiB\n")
            baseline = [drm_identity(h) for h in drm_inventory([pci], root)]
            evidence = {}
            self.assertEqual(drm_owners([pci], root, baseline=baseline, allowed=[], evidence=evidence), {pci: []})
            self.assertEqual(len(evidence["holders"]), 3)
            self.assertEqual(len(evidence["baseline_clients"]), 1)
            self.assertEqual(evidence["baseline_clients"][0]["resident_bytes"]["drm-resident-vram0"], 5352 * 1024)
            processes = [{k:h[k] for k in ("boot", "pid", "start", "exe", "cgroup")}
                         for h in drm_inventory([pci], root)]
            self.assertEqual(drm_owners([pci],root,allowed=[],observers=processes), {pci: []})
            with self.assertRaisesRegex(ValueError,"UNKNOWN"):
                drm_owners([pci],root,allowed=[],observers=[dict(h,start=h["start"]+1) for h in processes])
            (root / "2458/fd/9").symlink_to("/dev/dri/card1")
            (root / "2458/fdinfo/9").write_text(f"drm-pdev: {pci}\ndrm-client-id: 43\n")
            with self.assertRaisesRegex(ValueError, "UNKNOWN"):
                drm_owners([pci], root, baseline=baseline, allowed=[])
            (root / "2458/fd/9").unlink(); (root / "2458/fdinfo/9").unlink()
            (root / "1138/cgroup").write_text("0::/unexpected.scope")
            with self.assertRaisesRegex(ValueError, "UNKNOWN"):
                drm_owners([pci], root, baseline=baseline, allowed=[])

    def test_complete_resume(self) -> None:
        c = Checker(30)
        beginning(c)
        c.feed(event("commit", job="j", attempt=1, unit=1))
        beginning(c, 1, 2)
        c.feed(event("commit", job="j", attempt=2, unit=2))
        c.feed(event("finish", job="j", attempt=2, unit=2, complete=True))
        c.feed(event("status", jobs=[dict(id="j", state="succeeded", attempt=2)]))
        self.assertEqual(c.finish()["violations"], [])

    def test_saved_progress_lost_and_duplicate(self) -> None:
        c = Checker(30)
        beginning(c)
        c.feed(event("commit", job="j", attempt=1, unit=1))
        beginning(c, 0, 2)
        c.feed(event("commit", job="j", attempt=2, unit=1))
        self.assertTrue({"saved_units_lost", "completed_unit_duplicated"} <= c.errors)

    def test_bound_remains_during_outage(self) -> None:
        c = Checker(30)
        c.feed(event("submit", job="j", priority="production", units=2))
        c.feed(event("outage", 31))
        self.assertIn("production_start_bound_exceeded", c.errors)

    def test_late_start_fails(self) -> None:
        c = Checker(30)
        c.feed(event("submit", job="j", priority="production", units=2))
        c.feed(event("start", 31, job="j", attempt=1, resumed=0, units=2, resumable=True))
        self.assertIn("production_start_bound_exceeded", c.errors)

    def test_double_queue_owner(self) -> None:
        c = Checker(30)
        for jid in ("a", "b"):
            c.feed(event("submit", job=jid, priority="experiment", units=2))
        c.feed(event("status", jobs=[dict(id=j, state="running", machine="same", attempt=1) for j in ("a", "b")]))
        self.assertIn("queue_double_owner", c.errors)

    def test_physical_unknown_overlap_and_stale(self) -> None:
        c = Checker(30, True)
        c.feed(event("physical", valid=True, owners={"1": ["a", "b"]}))
        c.feed(event("tick", 4))
        c.feed(event("physical", 5, valid=False, owners={}))
        self.assertTrue({"physical_double_owner", "physical_ownership_unknown", "physical_evidence_missing_or_stale"} <= c.errors)

    def test_physical_scope_and_shape(self) -> None:
        c = Checker(30, True, ("0000:07:00.0",))
        c.feed(event("physical", valid=True, owners={"0000:03:00.0": []}))
        self.assertIn("physical_scope_missing", c.errors)
        for owners in ({}, {"0000:07:00.0": "owner"}):
            with self.assertRaises(ValueError):
                c.feed(event("physical", valid=True, owners=owners))

    def test_terminal_regression_and_incomplete_success(self) -> None:
        c = Checker(30)
        beginning(c)
        c.feed(event("status", jobs=[dict(id="j", state="succeeded", attempt=1)]))
        c.feed(event("status", jobs=[dict(id="j", state="queued", attempt=2)]))
        self.assertTrue({"terminal_regressed", "success_without_complete_output"} <= c.errors)

    def test_missing_job(self) -> None:
        c = Checker(30)
        beginning(c)
        c.feed(event("status", jobs=[]))
        self.assertIn("submitted_job_missing", c.errors)
        self.assertIn("job_not_terminal", c.finish()["violations"])

    def test_clock_change_is_hold(self) -> None:
        c = Checker(30)
        c.feed(event("submit", job="j", priority="production", units=2))
        c.feed(dict(kind="tick", at=1, boot="new-boot"))
        self.assertTrue(any(e.startswith("clock_domain_changed") for e in c.errors))

    def test_memory_and_corrupt_timestamps_fail_closed(self) -> None:
        c = Checker(30)
        c.feed(event("memory", valid=True, available_gib=3, minimum_available_gib=4))
        c.feed(event("memory", valid=False))
        self.assertTrue({"host_memory_headroom_exceeded", "host_memory_unknown"} <= c.errors)
        with self.assertRaises(ValueError):
            c.feed(event("tick", float("nan")))

    def test_refuse_stop_needs_external_kill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cp = root / "checkpoint"
            cp.mkdir()
            env = {k: v for k, v in os.environ.items() if not k.startswith("MYCE_")}
            env.update(MYCE_JOB_ID="j", MYCE_ATTEMPT="1", MYCE_CHECKPOINT_DIR=str(cp))
            proc = subprocess.Popen([sys.executable, str(Path(__file__).with_name("pressure.py")), "job",
                "--events", str(root / "events.jsonl"), "--units", "1000", "--unit-seconds", ".04",
                "--host-mib", "1", "--resumable", "--refuse-stop"], env=env, stderr=subprocess.PIPE)
            try:
                deadline = time.monotonic() + 5
                while not (cp / "checkpoint.json").exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue((cp / "checkpoint.json").exists())
                proc.send_signal(signal.SIGTERM)
                time.sleep(.1)
                self.assertIsNone(proc.poll())
            finally:
                proc.kill(); proc.wait(timeout=5)
                proc.stderr.close()

    def test_checkpoint_corruption_and_changed_contract(self) -> None:
        saved = dict(job="j", contract="c", unit=1, digest=digest(["j", "c", 1]))
        self.assertEqual(validate_save(saved, "j", "c", 2), 1)
        for changed in (dict(saved, unit=2), dict(saved, job="x"), dict(saved, contract="d")):
            with self.assertRaises(ValueError):
                validate_save(changed, "j", "c", 2)

    def test_malformed_and_streamed_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            emit(path, "tick")
            c = Checker(30)
            offset = replay(path, c)
            self.assertEqual(replay(path, c, offset), offset)
            with path.open("a") as out:
                out.write("{broken}\n")
            with self.assertRaises(ValueError):
                replay(path, c, offset)


class CommandTests(unittest.TestCase):
    def command(self, root: Path, attempt: int, *extra: str) -> subprocess.Popen:
        cp = root / "checkpoint"
        cp.mkdir(exist_ok=True)
        env = {k: v for k, v in os.environ.items() if not k.startswith("MYCE_")}
        env.update(MYCE_JOB_ID="j", MYCE_ATTEMPT=str(attempt), MYCE_CHECKPOINT_DIR=str(cp))
        return subprocess.Popen([sys.executable, str(Path(__file__).with_name("pressure.py")), "job",
            "--events", str(root / "events.jsonl"), "--units", "50", "--unit-seconds", ".04",
            "--host-mib", "1", "--resumable", *extra], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def wait_checkpoint(self, root: Path) -> Path:
        cp = root / "checkpoint/checkpoint.json"
        deadline = time.monotonic() + 5
        while not cp.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(cp.exists())
        return cp

    def test_actual_sigterm_save_and_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc = self.command(root, 1)
            try:
                cp = self.wait_checkpoint(root)
                proc.send_signal(signal.SIGTERM)
                self.assertEqual(proc.wait(timeout=5), 0)
                n = json.loads(cp.read_text())["value"]["unit"]
                self.assertGreater(n, 0)
                second = self.command(root, 2)
                try:
                    self.assertEqual(second.wait(timeout=5), 0)
                finally:
                    if second.poll() is None:
                        second.kill(); second.wait()
                    second.stderr.close()
                records = [json.loads(line) for line in (root / "events.jsonl").read_text().splitlines()]
                resumed = [r for r in records if r["kind"] == "start" and r["attempt"] == 2][0]
                self.assertEqual(resumed["resumed"], n)
                self.assertEqual(json.loads(cp.read_text())["value"]["unit"], 50)
            finally:
                if proc.poll() is None:
                    proc.kill(); proc.wait()
                proc.stderr.close()

    def test_second_owner_refused_before_allocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = self.command(root, 1)
            try:
                self.wait_checkpoint(root)
                second = self.command(root, 2)
                try:
                    self.assertEqual(second.wait(timeout=5), 91)
                finally:
                    if second.poll() is None:
                        second.kill(); second.wait()
                    second.stderr.close()
            finally:
                first.send_signal(signal.SIGTERM)
                first.wait(timeout=5)
                first.stderr.close()


class NativeRunnerTests(unittest.TestCase):
    def test_attempt_roots_use_the_same_fresh_suffix(self):
        import stage_ab
        self.assertTrue(stage_ab.ROOT.name.endswith('r12-20261012'))
        self.assertTrue(stage_ab.NAS.name.endswith('r12-20261012'))

    def test_disable_confirmation_includes_descendant_population_and_unit_state(self):
        import disable_drill
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'test').mkdir();(root/'test/cgroup.procs').write_text('')
            (root/'test/cgroup.events').write_text('populated 1\n')
            state=dict(ActiveState='inactive',ControlGroup='')
            with patch.object(disable_drill,'Path',return_value=root):
                self.assertFalse(disable_drill.empty('/test',state))
                (root/'test/cgroup.events').write_text('populated 0\n')
                self.assertTrue(disable_drill.empty('/test',state))
            self.assertTrue(disable_drill.empty('',state))
            self.assertFalse(disable_drill.empty('',dict(ActiveState='active',ControlGroup='')))

    def test_submission_waits_for_foreign_reader_release(self):
        import stage_ab
        from unittest.mock import patch
        import threading
        run=object.__new__(stage_ab.Run)
        run.foreign_paused=threading.Event();run.foreign_paused.set()
        run.safety=None;run.stop=threading.Event();run.a=None;run.url='unused'
        order=[];run.physical=lambda:order.append('scan')
        def release(*_):
            order.append('paused');run.foreign_paused.clear()
        with patch.object(run.stop,'wait',side_effect=release),patch.object(stage_ab,'submit',side_effect=lambda *_:order.append('submit') or 'id'):
            self.assertEqual(run.submit('experiment',1),'id')
        self.assertEqual(order,['scan','paused','submit'])

    def test_allowlists_refresh_after_inventory(self):
        import pressure
        from unittest.mock import patch
        h=dict(boot='boot',pid=17,start=42,exe='/test',cgroup='0::/test',clients=[dict(pci='p',device='/dev/dri/card1',client=1,resident_bytes={})])
        identity={k:h[k] for k in ('boot','pid','start','exe','cgroup')}
        order=[]
        with patch.object(pressure,'drm_inventory',side_effect=lambda *_:order.append('inventory') or [h]):
            self.assertEqual(pressure.drm_owners(['p'],allowed=lambda holders:order.append('allowlist') or [identity])['p'],[pressure.digest(['boot',17,42])[:24]])
        self.assertEqual(order,['inventory','allowlist'])

    def test_measurement_observers_requires_live_owned_parent(self):
        import stage_ab
        run=object.__new__(stage_ab.Run);run.procs={}
        self.assertEqual(run.measurement_observers(),[])

    def test_first_native_failure_stops_before_the_matrix(self):
        import stage_ab
        from unittest.mock import patch
        run=object.__new__(stage_ab.Run);run.a=None;run.url='unused';run.safety=None;run.physical=lambda:None;run.gate=lambda:None
        run.finished=lambda *_:{'j':{'state':'failed'}};run.event=lambda *_:None
        with patch.object(stage_ab,'submit',return_value='j') as submission:
            with self.assertRaisesRegex(RuntimeError,'native_runtime_qualification_failed'):run.production_only()
            self.assertEqual(submission.call_count,1)

    def test_queue_observer_honors_bounded_timeout(self):
        import pressure
        from unittest.mock import patch
        with patch.dict(os.environ,MYCE_TOKEN='synthetic'), patch.object(pressure.urllib.request,'urlopen') as opened:
            opened.return_value.__enter__.return_value.read.return_value=b'{}'
            self.assertEqual(pressure.api('http://127.0.0.1:1',timeout=.25),{})
            self.assertEqual(opened.call_args.kwargs['timeout'],.25)

    def test_native_process_allowlist_requires_recorded_start_and_cgroup(self):
        import stage_ab
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            ledger=Path(tmp)/'events.jsonl'
            ledger.write_text(json.dumps(dict(kind='start',pid=17,start_ticks=42))+'\n')
            run=object.__new__(stage_ab.Run)
            run.receipts=[ledger];run.cgroup='0::/test.service';run.worker_cgroup=None
            identity=dict(pid=17,start=42,exe=os.path.realpath(stage_ab.XPU),cgroup=run.cgroup,boot='b')
            with patch.object(stage_ab,'process',return_value=identity):self.assertEqual(run.owned(),[identity])
            with patch.object(stage_ab,'process',return_value=dict(identity,start=43)):self.assertEqual(run.owned(),[])
            with patch.object(stage_ab,'process',return_value=dict(identity,cgroup='0::/foreign')):self.assertEqual(run.owned(),[])

    def test_wrong_selector_rejected_before_torch_import_or_gpu_allocation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cp=root/'cp';cp.mkdir()
            env={k:v for k,v in os.environ.items() if not k.startswith('MYCE_')}
            env.update(MYCE_JOB_ID='selector-test',MYCE_ATTEMPT='1',MYCE_CHECKPOINT_DIR=str(cp),
                       ONEAPI_DEVICE_SELECTOR='level_zero:invalid',ZE_AFFINITY_MASK='invalid')
            completed=subprocess.run([sys.executable,str(Path(__file__).with_name('pressure.py')),'job',
                '--events',str(root/'events.jsonl'),'--backend','xpu','--card','1','--host-mib','1'],
                env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            self.assertNotEqual(completed.returncode,0)
            self.assertEqual(json.loads(completed.stderr), {'error':'RuntimeError'})
            records=[json.loads(s) for s in (root/'events.jsonl').read_text().splitlines()]
            self.assertFalse(any(e['kind']=='allocated' for e in records))


if __name__ == "__main__":
    unittest.main()
