"""Regression for #139: unmatched prefixes select direct tests, never a package."""
import importlib.util
import os
import pathlib
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('related', pathlib.Path(__file__).with_name('gate-related.py'))
related = importlib.util.module_from_spec(spec)
spec.loader.exec_module(related)


class RelatedTests(unittest.TestCase):
    def test_selection_and_explicit_full(self):
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                def git(*args):
                    subprocess.run(['git', *args], check=True, capture_output=True)
                def write(name, text):
                    pathlib.Path(name).write_text(text)
                git('init', '-q')
                write('cache.go', 'package p\nfunc DiskUsage() int { return 1 }\n')
                write('cache_test.go', 'package p\nfunc TestCache(t *testing.T) {}\n')
                write('other_test.go', 'package p\nfunc TestRelated(t *testing.T) { DiskUsage() }\nfunc TestUnrelated(t *testing.T) {}\n')
                git('add', '.')
                git('-c', 'user.name=Fixture', '-c', 'user.email=fixture-noreply@example.invalid', 'commit', '-qm', 'base')
                write('cache.go', 'package p\nfunc DiskUsage() int { return 2 }\n')
                self.assertEqual(related.select('HEAD'), [('.', '^(TestCache|TestRelated)$')])
                # The incident: implementation filename has no matching test file.
                git('mv', 'cache.go', 'accounting.go')
                self.assertEqual(related.select('HEAD'), [('.', '^(TestCache|TestRelated)$')])
                git('add', '.')
                git('-c', 'user.name=Fixture', '-c', 'user.email=fixture-noreply@example.invalid', 'commit', '-qm', 'rename')
                write('accounting.go', 'package p\nfunc DiskUsage() int { return 3 }\n')
                self.assertEqual(related.select('HEAD'), [('.', '^(TestRelated)$')])
                git('reset', '--hard', '-q', 'HEAD')
                write('unmapped.go', 'package p\nfunc hidden() {}\n')
                with self.assertRaisesRegex(ValueError, 'no related tests'):
                    related.select('HEAD')
                pathlib.Path('unmapped.go').unlink()
                write('go.mod', 'module example.invalid/p\n')
                git('add', 'go.mod')
                with self.assertRaisesRegex(ValueError, 'module dependencies'):
                    related.select('HEAD')
            finally:
                os.chdir(previous)


if __name__ == '__main__':
    unittest.main()
