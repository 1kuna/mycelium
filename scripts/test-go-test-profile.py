"""The profile supplies a timeout, permits overrides and preserves Go's exit."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class GoTestProfileTests(unittest.TestCase):
    def test_timeout_and_exit(self):
        script = Path(__file__).resolve().with_name('go-test-profile.sh')
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for tool in ('go', 'git', 'python3', 'uv'):
                stub = root / tool
                stub.write_text('#!/bin/sh\nexit 0\n')
                stub.chmod(0o755)
            (root / 'go').write_text('''#!/bin/sh
if [ "$1" = mod ]; then exit 0; fi
printf '%s\\n' "$@" > args
printf 'fixture output\\n'
exit 7
''')
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'])
            for args in (['./...'], ['-timeout', '22m', './...'], ['-timeout=22m', './...']):
                with self.subTest(args=args):
                    run = subprocess.run(['bash', str(script), *args], cwd=root, env=env,
                                         capture_output=True, text=True)
                    self.assertEqual(run.returncode, 7, run.stderr)
                    argv = (root / 'args').read_text().splitlines()
                    timeouts = [arg.split('=', 1)[1] if '=' in arg else argv[i+1]
                                for i, arg in enumerate(argv) if arg == '-timeout' or arg.startswith('-timeout=')]
                    self.assertEqual(timeouts[-1], '15m' if len(args) == 1 else '22m')
                    self.assertIn('fixture output', (root / 'go-test-log.jsonl').read_text())
                    self.assertEqual((root / 'go-test-result.txt').read_text(), 'go test: exit=7\n')


if __name__ == '__main__':
    unittest.main()
