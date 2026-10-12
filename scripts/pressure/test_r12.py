"""Mac-only executable harness paths; all fleet boundaries use explicit fakes."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import stage_ab
import docker_drill

class OwnershipRegression(unittest.TestCase):
    def test_docker_start_has_no_host_pid(self):
        with tempfile.TemporaryDirectory() as tmp:
            events=Path(tmp)/'events.jsonl'
            events.write_text(json.dumps(dict(kind='start',card='docker-cpu',identity='test-attempt',job='test-job',attempt=1))+'\n')
            run=stage_ab.Run.__new__(stage_ab.Run);run.receipts=[events]
            run.cgroup='0::/system.slice/myce-test.service';run.worker_cgroup=None
            self.assertEqual(run.owned([{'pid':999}]),[])
            with patch.object(stage_ab,'process') as proc:
                self.assertEqual(run.owned(),[]);proc.assert_not_called()

    def test_real_test_container_fixtures_and_cgroup_identity(self):
        fixture=Path(__file__).parent/'fixtures'
        row=json.loads((fixture/'b70-test-inspect.json').read_text())[0]
        ps=json.loads((fixture/'b70-test-ps.jsonl').read_text())
        self.assertEqual(ps['ID'],row['Id']);self.assertIn('myce.attempt=',ps['Labels'])
        ident=row['Config']['Labels']['myce.attempt']
        self.assertFalse(row['State']['Running']);self.assertEqual(row['State']['Pid'],0)
        holder=docker_drill.container_holder(row,{ident});self.assertNotIn('pid',holder)
        row['State'].update(Running=True,Pid=1234);row['HostConfig']['CgroupParent']=docker_drill.SLICE
        group='0::/myce.slice/'+docker_drill.SLICE+'/docker-'+row['Id']+'.scope'
        with patch.object(docker_drill,'kernel_read',return_value=group):
            self.assertEqual(docker_drill.container_holder(row,{ident})['cgroup'],group)
        with patch.object(docker_drill,'kernel_read',return_value='0::/system.slice/docker-foreign.scope'):
            with self.assertRaises(AssertionError):docker_drill.container_holder(row,{ident})
        with self.assertRaises(AssertionError):docker_drill.container_holder(row,{'foreign'})

if __name__=='__main__':unittest.main()

class ReceiptRegression(unittest.TestCase):
    def test_report_pending_and_digest_limit(self):
        import collect_r12 as collector
        with tempfile.TemporaryDirectory() as tmp,patch.object(collector,'HERE',Path(tmp)):
            collector.render({'launch-manifest.json':{'main':'test-main','harness':'test-harness'},'launch-receipt.json':{}})
            report=(Path(tmp)/'report-r14.md').read_text()
            self.assertIn('native outcomes pending',report)
            self.assertLessEqual(len(report.split('## DIGEST\n')[1].splitlines()),10)
            self.assertIn('Memory-ceiling-isolation: pending',report)
    def test_receipt_json_shapes_are_read_without_printing_tokens(self):
        import collect_r12 as collector
        from subprocess import CompletedProcess
        data={'results.json':{'results':[{'scenario':'Docker-grace','result':'PASS'}]},'FAILED':{}}
        with tempfile.TemporaryDirectory() as tmp,patch.object(collector,'HERE',Path(tmp)),patch.object(collector.subprocess,'run',return_value=CompletedProcess([],0,json.dumps(data),'')):
            self.assertEqual(collector.collect(),data)
            self.assertTrue((Path(tmp)/'native-r14-receipts/FAILED').exists())
    def test_new_generated_collector_and_installer_python_compiles(self):
        import collect_r12 as collector
        from remote_snippets import checked_python,python_command
        checked_python(collector.receipt_source());python_command(collector.receipt_source())
        for name in ('launch_r12.py','collect_r12.py','installed_validation.py','finalize_stage_ab.py'):
            checked_python((Path(__file__).parent/name).read_text())
