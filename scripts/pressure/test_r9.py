import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


class DockerFenceTests(unittest.TestCase):
    def wrapper(self,root):
        path=Path(__file__).with_name('docker_wrapper.py')
        spec=importlib.util.spec_from_file_location('pressure_docker_wrapper_test',path)
        module=importlib.util.module_from_spec(spec)
        with patch.dict(os.environ,MYCE_TEST_DOCKER_PHASE='/home/user/synthetic-run/phases/test'):
            spec.loader.exec_module(module)
        module.ROOT=root;module.PHASE=root/'phases/test';module.PHASE.mkdir(parents=True)
        state=module.PHASE/'worker';state.mkdir();(state/'owned.docker.json').write_text(json.dumps(dict(identity='owned')))
        (module.PHASE/'docker-config.json').write_text(json.dumps(dict(mode='normal')))
        return module

    def test_foreign_container_never_reaches_a_signal(self):
        with tempfile.TemporaryDirectory() as temp:
            wrapper=self.wrapper(Path(temp))
            wrong=json.dumps([dict(Id='container',Config=dict(Labels={'myce.attempt':'foreign'}))]).encode()
            with patch.object(wrapper.subprocess,'check_output',return_value=wrong),patch.object(wrapper.os,'execv') as execution:
                with self.assertRaises(AssertionError):wrapper.main(['--host','unix:///var/run/docker.sock','kill','--signal=KILL','container'])
                execution.assert_not_called()

    def test_owned_create_gets_caps_and_no_network(self):
        with tempfile.TemporaryDirectory() as temp:
            wrapper=self.wrapper(Path(temp))
            with patch.object(wrapper.os,'execv') as execution:
                wrapper.main(['--host','unix:///var/run/docker.sock','create','--label','myce.attempt=owned','--env','MYCE_MEMORY_LIMIT_BYTES=134217728','--memory','134217728','--memory-swap','134217728','image'])
                args=execution.call_args.args[1]
                self.assertEqual(args[args.index('--memory')+1],'134217728')
                self.assertEqual(args.count('--memory'),1)
                self.assertIn('--cgroup-parent=myce-test-r11-docker.slice',args)
                self.assertIn('--network=none',args)
                with self.assertRaises(AssertionError):
                    wrapper.main(['--host','unix:///var/run/docker.sock','create','--label','myce.attempt=owned','--device','/dev/dri/renderD128','image'])
                self.assertEqual(execution.call_count,1)

    def test_daemon_override_fails_before_docker(self):
        with tempfile.TemporaryDirectory() as temp:
            wrapper=self.wrapper(Path(temp))
            with patch.object(wrapper.subprocess,'check_output') as read:
                with self.assertRaises(AssertionError):wrapper.main(['--host','unix:///other.sock','ps'])
                read.assert_not_called()


if __name__=='__main__':unittest.main()
