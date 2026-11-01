"""The profile supplies a timeout, permits overrides and preserves Go's exit."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class GoTestProfileTests(unittest.TestCase):
    def test_gate_wrapper(self):
        script = Path(__file__).resolve().with_name('gate-profile.sh')
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for tool in ('git', 'uv'):
                (root / tool).write_text('#!/bin/sh\nexit 0\n')
                (root / tool).chmod(0o755)
            (root / 'go').write_text(
                '#!/bin/sh\nif [ "$1" = env ]; then echo go1.25.6; '
                'else printf "%s\\n" "$*" > go-args; fi\n')
            (root / 'go').chmod(0o755)
            (root / 'scripts').mkdir()
            (root / 'scripts/gate.sh').write_text(
                '#!/bin/sh\nprintf "%s\\n" "$@" > gate-args\nexit 0\n')
            (root / 'scripts/gate.sh').chmod(0o755)
            run = subprocess.run(['bash', str(script)], cwd=root,
                                 env=dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH']),
                                 capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual((root / 'go-args').read_text(), 'mod download\n')
            self.assertEqual((root / 'gate-args').read_text(), 'full\n')
            self.assertEqual((root / 'gate-result.txt').read_text(), 'gate full: exit=0\n')
            (root / 'scripts/gate.sh').write_text('#!/bin/sh\nexit 7\n')
            (root / 'gate-result.txt').unlink()
            run = subprocess.run(['bash', str(script)], cwd=root,
                                 env=dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH']),
                                 capture_output=True, text=True)
            self.assertEqual(run.returncode, 7, run.stderr)
            self.assertFalse((root / 'gate-result.txt').exists())

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
                    forwarded = [*args, '-args', 'dogfood argument with spaces']
                    run = subprocess.run(['bash', str(script), *forwarded], cwd=root, env=env,
                                         capture_output=True, text=True)
                    self.assertEqual(run.returncode, 7, run.stderr)
                    argv = (root / 'args').read_text().splitlines()
                    self.assertEqual(argv[-len(forwarded):], forwarded)
                    timeouts = [arg.split('=', 1)[1] if '=' in arg else argv[i+1]
                                for i, arg in enumerate(argv) if arg == '-timeout' or arg.startswith('-timeout=')]
                    self.assertEqual(timeouts[-1], '15m' if len(args) == 1 else '22m')
                    self.assertIn('fixture output', (root / 'go-test-log.jsonl').read_text())
                    self.assertEqual((root / 'go-test-result.txt').read_text(), 'go test: exit=7\n')


if __name__ == '__main__':
    unittest.main()
