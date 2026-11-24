"""Exercise the fatal observer against synthetic proc and cgroup trees."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).with_name('qualification') / 'observe-fatal.py'
spec = importlib.util.spec_from_file_location('observe_fatal', SOURCE)
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


class FatalObserverTests(unittest.TestCase):
    def test_marked_wrapper_is_skipped_for_python_in_nested_cgroup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            proc = root / 'proc'
            cgroups = root / 'cgroups'
            group = cgroups / 'attempt'
            child = group / 'main'
            child.mkdir(parents=True)
            python = root / 'python3'
            python.touch()
            wrapper = root / 'wrapper'
            wrapper.touch()
            baseline = {name: f'{name}:[1]' for name in observer.NAMESPACES}

            for cgroup, pid, executable in ((group, '42', wrapper), (child, '43', python)):
                (cgroup / 'cgroup.procs').write_text(pid + '\n')
                process = proc / pid
                (process / 'ns').mkdir(parents=True)
                (process / 'attr').mkdir()
                (process / 'exe').symlink_to(executable)
                (process / 'cmdline').write_bytes(b'exe\0' + observer.MARKER + b'\0')
                (process / 'stat').write_text(f'{pid} (fixture) S' + ' 123' * 19)
                (process / 'attr/current').write_text('qualification (enforce)\n')
                (process / 'uid_map').write_text('0 1000 1\n')
                for name in observer.NAMESPACES:
                    (process / 'ns' / name).symlink_to(f'{name}:[2]')

            with patch.multiple(observer, PROC=proc, CGROUPS=cgroups, PYTHON=python):
                evidence = observer.sample(group, baseline)

            self.assertEqual(evidence['pid'], 43)
            self.assertEqual(evidence['cgroup'], '/attempt/main')
            self.assertEqual(evidence['payload_namespaces'], {name: f'{name}:[2]' for name in observer.NAMESPACES})
