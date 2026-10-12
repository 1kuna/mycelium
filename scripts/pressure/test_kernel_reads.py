import ast
import errno
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch


class KernelReadTests(unittest.TestCase):
    def test_no_read_bypass_in_harness_including_remote_snippets(self):
        violations=[]
        for path in Path(__file__).parent.glob('*.py'):
            if path.name.startswith('test_') or path.name=='kernel_reads.py':continue
            trees=[ast.parse(path.read_text())]
            for tree in list(trees):
                for node in ast.walk(tree):
                    if isinstance(node,ast.Constant) and isinstance(node.value,str):
                        try:trees.append(ast.parse(node.value))
                        except SyntaxError:pass
            for tree in trees:
                for node in ast.walk(tree):
                    if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=='open':
                        violations.append((path.name,node.lineno,'open'))
                    if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
                        if ast.unparse(node.func.value)=='tarfile' and node.func.attr=='open' and any(k.arg=='fileobj' for k in node.keywords):continue
                        if node.func.attr=='open' and (not node.args or isinstance(node.args[0],ast.Constant) and 'r' in str(node.args[0].value)) and ast.unparse(node.func.value) not in ('f','path'):
                            violations.append((path.name,node.lineno,'open'))
                        if node.func.attr in ('stat','lstat') and ast.unparse(node.func.value) not in ('manifest','os'):
                            violations.append((path.name,node.lineno,node.func.attr))
                        if node.func.attr in ('exists','is_dir','is_file','glob','stat','lstat','open') and any(isinstance(n,ast.Constant) and isinstance(n.value,str) and n.value.startswith(('/proc','/sys')) for n in ast.walk(node.func.value)):
                            violations.append((path.name,node.lineno,node.func.attr))
                        if node.func.attr=='read' and ast.unparse(node.func.value) in ('self.rfile','res','sys.stdin.buffer'):continue
                        if node.func.attr in ('read_text','read_bytes','iterdir','readlink','scandir','read'):
                            violations.append((path.name,node.lineno,node.func.attr))
        self.assertEqual(violations,[], 'filesystem reads must use kernel_read (includes /proc and /sys)')

    def test_helper_random_vanishing_reads_rescan_and_classify(self):
        import kernel_reads as k
        rng=random.Random(238)
        for op in ('text','bytes','link','list','hash'):
            for code in (errno.ENOENT,errno.ESRCH):
                with self.subTest(op=op,errno=code),tempfile.TemporaryDirectory() as tmp:
                    path=Path(tmp)/'entry'
                    with patch('kernel_reads._read',side_effect=OSError(code,'gone')),patch('kernel_reads._rescan',return_value=True) as scan:
                        with self.assertRaises(k.Vanished):k.kernel_read(path,op)
                        scan.assert_called_once()

    def test_random_fds_and_processes_vanish_inside_inventory(self):
        import kernel_reads as k
        import pressure
        rng=random.Random(238)
        for seed in range(20):
            with self.subTest(seed=seed),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);pid=root/'17';(pid/'fd').mkdir(parents=True);(pid/'fdinfo').mkdir()
                (pid/'stat').write_text('17 (synthetic) '+' '.join(['S','1','17']+['0']*16+['42']))
                (pid/'exe').symlink_to('/usr/bin/synthetic');(pid/'cgroup').write_text('0::/test')
                for n in range(8):
                    (pid/'fd'/str(n)).symlink_to('/dev/dri/renderD129')
                    (pid/'fdinfo'/str(n)).write_text('drm-pdev: 0000:07:00.0\n'+'drm-client-id: '+str(n)+'\n')
                original=k._read
                def raced(path,op):
                    if path.parent==pid/'fd' and rng.random()<.5:
                        path.unlink(missing_ok=True)
                    if path==pid/'stat' and rng.random()<.3:
                        import shutil
                        shutil.rmtree(pid,ignore_errors=True)
                    return original(path,op)
                with patch('kernel_reads._read',side_effect=raced):
                    holders=pressure.drm_inventory(['0000:07:00.0'],root)
                self.assertLessEqual(len(holders),1)

    def test_live_permission_and_other_errors_are_never_empty(self):
        import kernel_reads as k
        for code in (errno.EACCES,errno.EPERM,errno.EIO):
            with self.subTest(errno=code),patch('kernel_reads._read',side_effect=OSError(code,'failure')),patch('kernel_reads._rescan',return_value=True):
                with self.assertRaises(OSError):k.kernel_read(Path('/proc/17/fd/8'),'link')
        with patch('kernel_reads._read',side_effect=OSError(errno.EACCES,'exited')),patch('kernel_reads._rescan',return_value=False):
            with self.assertRaises(k.Vanished):k.kernel_read(Path('/proc/17/fd/8'),'link')

    def test_cleanup_reads_only_exact_cgroup_and_descendants(self):
        import stage_ab
        import os
        root=Path('/sys/fs/cgroup/system.slice/myce-test-stage-ab-r14.service')
        def read(path,op='text'):
            if path==root/'cgroup.procs':return str(os.getpid())+' 17'
            if path==root and op=='directories':return [root/'child']
            if path==root/'child/cgroup.procs':return '18'
            if path==root/'child' and op=='directories':return []
            self.fail('cleanup crossed cgroup boundary: '+str(path))
        with patch('stage_ab.kernel_read',side_effect=read):
            self.assertEqual(stage_ab.cgroup_pids('0::/system.slice/myce-test-stage-ab-r14.service'),[17,18])

    def test_observer_survives_a_hold_until_cleanup(self):
        import stage_ab
        import threading
        from unittest.mock import Mock
        run=stage_ab.Run.__new__(stage_ab.Run)
        run.stop=threading.Event();run.recovering=threading.Event();run.observer_idle=threading.Event()
        run.active=None;run.safety=None;run.save=Mock()
        count=[]
        def broken(*_):
            count.append(1)
            if len(count)==2:run.stop.set()
            raise OSError(errno.EIO,'injected collector error')
        run.physical=broken
        run.monitor()
        self.assertEqual(len(count),2)
        self.assertIsNotNone(run.safety)
        self.assertEqual(run.save.call_count,2)

    def test_scenario_hold_continues_only_after_confirmed_cleanup(self):
        import stage_ab
        from types import SimpleNamespace
        from unittest.mock import Mock
        run=stage_ab.Run.__new__(stage_ab.Run)
        run.safety='collector fault';run.active=None
        run.result=Mock();run.begin=Mock(side_effect=RuntimeError('hold'))
        run.scenario_cleanup=Mock(side_effect=lambda:setattr(run,'safety',None))
        run.scenario('first',lambda:None)
        self.assertIsNone(run.safety)
        run.scenario_cleanup.assert_called_once()
        run.begin=Mock();run.end=Mock()
        run.scenario('second',lambda:None)
        run.end.assert_called_once_with('second')
        run.safety='fault';run.begin=Mock(side_effect=RuntimeError('hold'))
        run.scenario_cleanup=Mock(side_effect=RuntimeError('cleanup unconfirmed'))
        with self.assertRaisesRegex(RuntimeError,'cleanup unconfirmed'):run.scenario('third',lambda:None)


if __name__=='__main__':unittest.main()
