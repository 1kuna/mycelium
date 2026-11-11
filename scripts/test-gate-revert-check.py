"""Regression for #1224: a stale tree committed onto a fresh tip is flagged."""
import importlib.util
import os
import pathlib
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'revert', pathlib.Path(__file__).with_name('gate-revert-check.py'))
revert = importlib.util.module_from_spec(spec)
spec.loader.exec_module(revert)


class RevertCheckTests(unittest.TestCase):
    def test_stale_tree_restoring_merged_files(self):
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                def git(*args):
                    return subprocess.run(['git', *args], check=True, capture_output=True,
                                          text=True).stdout.strip()

                def commit(message):
                    git('add', '-A')
                    git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                        'commit', '-qm', message)
                    return git('rev-parse', 'HEAD')

                git('init', '-q', '-b', 'main')
                pathlib.Path('a.go').write_text('package p\nvar a = 1\n')
                pathlib.Path('b.go').write_text('package p\nvar b = 1\n')
                commit('base')
                pathlib.Path('a.go').write_text('package p\nvar a = 2\n')
                pathlib.Path('added.go').write_text('package p\nvar added = 1\n')
                feature = commit('feature')
                git('checkout', '-qb', 'topic')
                # An ordinary change is not a revert.
                pathlib.Path('b.go').write_text('package p\nvar b = 2\n')
                commit('ordinary')
                self.assertEqual(revert.check('main')[1], {})
                # The stale tree: a.go back to its pre-feature blob, added.go gone.
                pathlib.Path('a.go').write_text('package p\nvar a = 1\n')
                pathlib.Path('added.go').unlink()
                commit('stale')
                _, findings = revert.check('main')
                self.assertEqual(sorted(findings), ['a.go', 'added.go'])
                self.assertEqual([c for c, *_ in findings['a.go']], [feature])
                self.assertEqual(revert.main(['main']), 1)
                self.assertEqual(revert.main(['main', '--allow-reverts']), 0)
                # Deleting a file whose adding commit is otherwise kept stays quiet.
                git('checkout', '-q', 'main')
                git('checkout', '-qb', 'cleanup')
                pathlib.Path('added.go').unlink()
                commit('remove')
                self.assertEqual(revert.check('main')[1], {})
            finally:
                os.chdir(previous)


if __name__ == '__main__':
    unittest.main()
