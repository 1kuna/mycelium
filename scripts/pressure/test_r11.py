import json
import unittest
from unittest.mock import MagicMock, patch
import manage_followons as t
import stage_ab
from remote_snippets import checked_python, checked_shell, python_command


class SnippetTests(unittest.TestCase):
    def tearDown(self):
        t.state=t.closed_state=t.cleanup_hold=None
        t.phase_errors.clear();t.procs.clear();t.ports.clear()

    def test_every_transport_snippet_variant(self):
        sources=[];finalized=iter([False,True])
        def remote(host, source, data=None):
            checked_python(source);checked_shell(python_command(source));sources.append(source)
            if 'unit_state' in source:return {'confirmed':True,'unit_state':'inactive'}
            if 'occupied=[]' in source:return [] if 'socket,json,sys' in source else {}
            if 'systemd-run' in source:return {'started':True}
            if 'time.monotonic()' in source:return 10
            if 'MemAvailable:' in source:return 30*1024**2
            if 'events.jsonl' in source:return []
            if 'ActiveState' in source:return {'ActiveState':'inactive','MainPID':'0','ControlGroup':''}
            if 'getsockname' in source:return 12345
            if 'final-cleanup.json' in source:return next(finalized)
            if 'kernel_reads.py' in source:return True
            return None
        with patch.object(t,'remote',side_effect=remote), patch.object(t.time,'sleep'), \
             patch.object(t,'subprocess') as processes:
            processes.Popen.return_value=MagicMock(poll=lambda:None)
            processes.run.return_value=MagicMock(returncode=0,stdout=b'null')
            t.respond({'ok':True});t.read_request();t.loan();t.stop();t.cleanup_spark();t.poll();t.serve()
            for mode in ('active','full'):
                req=dict(port=12344,mode=mode,phase=mode,token='synthetic',boot='test',request_id=mode)
                t.start(req);t.collect(dict(req,mode='stop',request_id=mode+'-stop'))
            t.state=dict(phase='failed',offset=0,uncertainty=0,boot='test')
            with patch.object(t,'poll',side_effect=ValueError):t.active_poll()
            t.collect(dict(mode='stop',phase='failed',request_id='failed-stop'))
        self.assertGreaterEqual(len(sources),25)

    def test_all_preparation_launch_qualification_and_receipt_snippets(self):
        import ast
        import prepare_r11, launch_r11, qualify_r11, collect_r11
        sources=[t.final_receipt_source(),prepare_r11.generated_0(),prepare_r11.generated_1(),prepare_r11.staging_source(),
                 launch_r11.generated_0(),qualify_r11.runtime_source(),qualify_r11.writer_source(),
                 launch_r11.receipt_writer_source(),collect_r11.receipt_source(),collect_r11.inventory_source(),
                 *[qualify_r11.proof_source(m) for m in ('write','copy','reread')]]
        for source in sources:
            checked_python(source);checked_shell(python_command(source));checked_shell(python_command(source,sudo=True))
            for node in ast.walk(ast.parse(source)):
                if isinstance(node,ast.List):
                    for i,item in enumerate(node.elts[:-1]):
                        if isinstance(item,ast.Constant) and item.value=='-c' and isinstance(node.elts[i+1],ast.Constant):
                            checked_python(node.elts[i+1].value)
        import shlex
        checked_shell(shlex.join(launch_r11.launch_args()))
        checked_shell(shlex.join(['systemctl','show',launch_r11.UNIT,'-p','ActiveState','-p','ControlGroup']))
        checked_shell('tar -xz -C '+t.ROOT)
        checked_shell('sudo -n python3 -');checked_shell('python3 - spark')

    def test_delegated_attempt_is_owned_only_below_exact_worker_unit(self):
        run=stage_ab.Run.__new__(stage_ab.Run)
        run.cgroup='0::/system.slice/myce-test.service';run.worker_cgroup='0::/user.slice/myce-worker@gpu1.service'
        self.assertTrue(run.owned_group(run.worker_cgroup+'/attempt-synthetic'))
        self.assertFalse(run.owned_group(run.worker_cgroup+'-foreign/attempt-synthetic'))
        self.assertFalse(run.owned_group('0::/user.slice/myce-worker@gpu0.service/attempt-synthetic'))

    def test_invalid_python_and_shell_never_dispatch(self):
        with patch.object(t.subprocess,'run') as run:
            with self.assertRaises(Exception):t.remote('spark','print(')
            run.assert_not_called()
        with self.assertRaises(ValueError):checked_shell('if then')

    def test_real_poll_failure_cleanup_ack_and_runner_continuation(self):
        run=stage_ab.Run.__new__(stage_ab.Run);run.safety=None;run.results=[]
        run.result=lambda name,result,**kw:run.results.append(dict(scenario=name,result=result))
        run.scenario_cleanup=MagicMock();responses=[]
        def remote(host,source,data=None):
            checked_python(source)
            if 'MemAvailable:' in source:raise ValueError('synthetic poll failure')
            if 'unit_state' in source:return {'confirmed':True,'unit_state':'inactive'}
            if 'occupied=[]' in source or 'events.jsonl' in source:return []
        def followon(name,mode):
            if mode=='active':
                t.state=dict(phase=name,offset=0,uncertainty=0,boot='test')
                t.active_poll();raise ValueError('phase failed')
            run.result(name,'PASS')
        def request(mode):
            t.service_request(dict(mode=mode,phase=run.results[-1]['scenario'],request_id='fresh-stop'))
            self.assertTrue(responses[-1]['cleanup_confirmed']);self.assertEqual(responses[-1]['request_id'],'fresh-stop')
        run.followon=followon;run.spark_request=request
        with patch.object(t,'remote',side_effect=remote),patch.object(t,'respond',side_effect=responses.append):run.followons()
        self.assertEqual([r['result'] for r in run.results],['FAIL','PASS'])
        self.assertEqual(run.scenario_cleanup.call_count,2)
        self.assertEqual(responses[0]['phase_error'],'ValueError')

    def test_unconfirmed_cleanup_never_acknowledged(self):
        t.state=dict(phase='failure');responses=[]
        with patch.object(t,'poll',side_effect=ValueError),patch.object(t,'stop',side_effect=RuntimeError), \
             patch.object(t,'remote'),patch.object(t,'respond',side_effect=responses.append):
            t.active_poll();t.service_request(dict(mode='stop',phase='failure',request_id='stop'))
        self.assertIsNotNone(t.state);self.assertFalse(responses[-1]['ok']);self.assertNotIn('cleanup_confirmed',responses[-1])


if __name__=='__main__':unittest.main()
