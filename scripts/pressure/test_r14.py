"""Local regressions for retry pinning and outcome custody; no fleet tools."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import build_retry
import cpu_drill
import prepare_r11


class RetryTests(unittest.TestCase):
    def test_build_exact_commit_and_hashes_in_fresh_root(self):
        archive=io.BytesIO()
        with tarfile.open(fileobj=archive,mode='w') as stream:
            for name in ('internal','internal/myce','cmd','cmd/myce'):
                entry=tarfile.TarInfo(name);entry.type=tarfile.DIRTYPE;stream.addfile(entry)
        calls=[]
        def output(args,**kw):
            calls.append(args)
            if 'archive' in args:return archive.getvalue()
            return 'exact-core\n' if args[-1]=='given^{commit}' else 'exact-harness\n'
        def run(args,**kw):
            Path(args[args.index('-o')+1]).write_bytes(b'local-artifact-'+args[1].encode())
            self.assertEqual(kw['env']['GOMAXPROCS'],'2')
            return subprocess.CompletedProcess(args,0)
        with tempfile.TemporaryDirectory() as tmp,patch.object(build_retry.subprocess,'check_output',side_effect=output),patch.object(build_retry.subprocess,'run',side_effect=run):
            root=Path(tmp)/'retry'
            receipt=build_retry.build(Path(tmp),'given',root,'linux','amd64')
            self.assertEqual(receipt['core_commit'],'exact-core')
            self.assertEqual(receipt['queue_binary'],receipt['worker_binary'])
            self.assertEqual(len(set(receipt['state'].values())),4)
            for name,digest in receipt['files'].items():self.assertEqual(build_retry.sha(root/'artifacts'/name),digest)
            self.assertTrue((root/'source/internal/myce/pressure_matrix_test.go').exists())
            self.assertIn('TestPressureForeignVersoStateMustReject',(root/'source/internal/myce/pressure_matrix_test.go').read_text())
            with self.assertRaises(FileExistsError):build_retry.build(Path(tmp),'given',root,'linux','amd64')
            self.assertTrue(any(args[-2:]==['archive','exact-core'] for args in calls))

    def test_remote_staging_refuses_existing_root_without_mutation(self):
        with tempfile.TemporaryDirectory() as tmp,patch.object(prepare_r11,'ROOT',str(Path(tmp)/'retry')):
            code=prepare_r11.staging_source()
            exec(code)
            evidence=Path(prepare_r11.ROOT)/'evidence';evidence.write_text('retained')
            with self.assertRaises(FileExistsError):exec(code)
            self.assertEqual(evidence.read_text(),'retained')

    def test_cpu_outcomes_do_not_confuse_exit_one_timeout_or_oom(self):
        self.assertEqual(cpu_drill.classify(0,{},[],'PASS'),'passed')
        self.assertEqual(cpu_drill.classify(1,{},[],'--- FAIL: test'),'assertion_failure')
        self.assertEqual(cpu_drill.classify(2,{},[],'panic: test timed out after 5m'),'timeout')
        self.assertEqual(cpu_drill.classify(None,{'Result':'timeout'},[],''),'timeout')
        self.assertEqual(cpu_drill.classify(None,{},[],'',{'type':'TimeoutError'}),'timeout')
        self.assertEqual(cpu_drill.classify(-9,{},[{'events':'oom 1\noom_kill 1\n'}],''),'oom')
        self.assertEqual(cpu_drill.classify(None,{'Result':'oom-kill'},[],''),'oom')
        self.assertEqual(cpu_drill.classify(-9,{},[{'events':'oom_kill 0\n'}],''),'execution_failure')
        self.assertIn('TestDocker',cpu_drill.FAST)
        self.assertIn('TestPressureForeignVersoStateMustReject',cpu_drill.FAST)

    def test_child_exit_and_detailed_log_preserved(self):
        child=Path(__file__).with_name('cpu_child.py')
        with tempfile.TemporaryDirectory() as tmp:
            marker=Path(tmp)/'child.exit';log=Path(tmp)/'cpu.log'
            with log.open('w') as stream:
                result=subprocess.run([sys.executable,str(child),str(marker),sys.executable,'-c',
                    'print("synthetic assertion detail", flush=True); raise SystemExit(1)'],stdout=stream,stderr=subprocess.STDOUT)
            self.assertEqual(result.returncode,1);self.assertEqual(marker.read_text().strip(),'1')
            self.assertIn('synthetic assertion detail',log.read_text())


if __name__=='__main__':unittest.main()
