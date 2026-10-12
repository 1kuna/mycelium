import os
import unittest
from unittest.mock import MagicMock, patch

import manage_followons as transport
import pressure
import stage_ab


class TransportTests(unittest.TestCase):
    def tearDown(self):
        transport.state = None
        transport.procs.clear()
        transport.ports.clear()

    def test_generated_spark_start_compiles_and_quotes_device(self):
        captured = []
        def remote(host, code, data=None):
            if data is not None and 'systemd-run' in code:
                captured.append(code)
                compile(code, 'spark-start', 'exec')
                self.assertIn(repr(transport.UUID), code)
                return {'started': True}
            return 23456
        request = dict(port=23457, mode='active', phase='migration', token='synthetic', boot='test', request_id='start')
        with patch.object(transport, 'remote', side_effect=remote), patch.object(transport, 'stop'), \
             patch.object(transport, 'respond'), patch.object(transport.time, 'sleep'), \
             patch.object(transport.subprocess, 'Popen', return_value=MagicMock(poll=lambda: None)):
            transport.start(request)
        self.assertEqual(len(captured), 1)

    def test_stop_after_failed_start_confirms_cleanup_without_state(self):
        request = dict(mode='stop', phase='migration', request_id='next')
        with patch.object(transport, 'stop', return_value={'confirmed': True}) as stop, \
             patch.object(transport, 'respond') as respond, patch.object(transport, 'remote'):
            transport.collect(request)
        stop.assert_called_once()
        self.assertTrue(respond.call_args.args[0]['cleanup_confirmed'])
        self.assertEqual(respond.call_args.args[0]['request_id'], 'next')

    def test_stop_does_not_clear_state_when_cleanup_is_unconfirmed(self):
        transport.state = {'phase': 'migration'}
        with patch.object(transport, 'remote', side_effect=RuntimeError('unconfirmed')):
            with self.assertRaises(RuntimeError):
                transport.stop()
        self.assertIsNotNone(transport.state)

    def test_status_reads_use_token_without_putting_it_in_url_or_body(self):
        with patch.dict(os.environ, MYCE_TOKEN='synthetic-secret'), \
             patch.object(pressure.urllib.request, 'urlopen') as opened:
            opened.return_value.__enter__.return_value.read.return_value = b'{"jobs": []}'
            self.assertEqual(pressure.api('http://127.0.0.1:12345'), {'jobs': []})
        request = opened.call_args.args[0]
        self.assertEqual(request.get_header('Authorization'), 'Bearer synthetic-secret')
        self.assertNotIn('synthetic-secret', request.full_url)
        self.assertIsNone(request.data)


class RunnerTests(unittest.TestCase):
    def runner(self):
        run = stage_ab.Run.__new__(stage_ab.Run)
        run.safety = None
        run.results = []
        run.result = lambda name, result, **kw: run.results.append(dict(scenario=name, result=result, **kw))
        run.begin = MagicMock()
        run.end = MagicMock()
        run.scenario_cleanup = MagicMock()
        return run

    def test_scenario_failure_continues_only_after_cleanup(self):
        run = self.runner()
        def fail():
            raise RuntimeError('synthetic failure')
        run.scenario('first', fail)
        run.scenario('second', lambda: None)
        self.assertEqual(run.results[0]['result'], 'FAIL')
        self.assertEqual(run.begin.call_count, 2)
        run.scenario_cleanup.assert_called_once()

    def test_scenario_cleanup_failure_stops_runner(self):
        run = self.runner()
        run.scenario_cleanup.side_effect = RuntimeError('cleanup unconfirmed')
        with self.assertRaises(RuntimeError):
            run.scenario('first', lambda: (_ for _ in ()).throw(ValueError('synthetic')))

    def test_failed_followon_then_next_scenario_with_cross_host_cleanup(self):
        run = self.runner()
        def fail(name, mode):
            raise RuntimeError('synthetic migration failure')
        run.followon = MagicMock(side_effect=fail)
        run.spark_request = MagicMock(return_value={'cleanup_confirmed': True})
        run.followons()
        self.assertEqual(run.followon.call_count, 2)
        self.assertEqual(run.scenario_cleanup.call_count, 2)
        self.assertEqual(run.spark_request.call_count, 2)

    def test_followon_stops_when_cross_host_cleanup_is_unconfirmed(self):
        run = self.runner()
        run.followon = MagicMock(side_effect=ValueError('synthetic failure'))
        run.spark_request = MagicMock(side_effect=RuntimeError('Spark cleanup unconfirmed'))
        with self.assertRaises(RuntimeError):
            run.followons()
        self.assertEqual(run.followon.call_count, 1)


if __name__ == '__main__':
    unittest.main()
