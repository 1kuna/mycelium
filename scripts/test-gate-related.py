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
    def test_example_only_changes_select_direct_readers(self):
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                def git(*args):
                    subprocess.run(['git', *args], check=True, capture_output=True)
                root = pathlib.Path('internal/myce')
                root.mkdir(parents=True)
                example = pathlib.Path('docs/examples/sim-server-entries.yaml')
                example.parent.mkdir(parents=True)
                example.write_text('allowed_machines: [placeholder]\n')
                (root / 'server_entries_test.go').write_text(
                    'package p\nfunc TestEntrySimDryExample(t *testing.T) {\n'
                    '  os.ReadFile("../../docs/examples/sim-server-entries.yaml")\n}\n'
                    'func TestUnrelatedEntry(t *testing.T) {}\n'
                    'func TestNewExample(t *testing.T) {\n'
                    '  os.ReadFile("../../docs/examples/new.yaml")\n}\n')
                git('init', '-q')
                git('add', '.')
                git('-c', 'user.name=Fixture', '-c', 'user.email=fixture-noreply@example.invalid', 'commit', '-qm', 'base')
                example.write_text('platforms: [cuda]\n')
                self.assertEqual(related.select('HEAD'), [
                    ('internal/myce', '^(TestEntrySimDryExample)$')])
                git('restore', str(example))
                # New untracked examples need the same coverage as edits.
                example.with_name('new.yaml').write_text('platforms: [cpu]\n')
                self.assertEqual(related.select('HEAD'), [
                    ('internal/myce', '^(TestNewExample)$')])
                example.with_name('new.yaml').unlink()
                # Unrelated prose must not expand to the whole package.
                pathlib.Path('docs/guide.md').write_text('Guide\n')
                self.assertEqual(related.select('HEAD'), [])
            finally:
                os.chdir(previous)

    def test_wire_schema_for_field_and_nested_type_edits(self):
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                def git(*args):
                    subprocess.run(['git', *args], check=True, capture_output=True)
                root = pathlib.Path('internal/myce')
                root.mkdir(parents=True)
                git('init', '-q')
                (root / 'types.go').write_text('package p\ntype Job struct {\n\tValue int\n}\n')
                (root / 'types_test.go').write_text('package p\nfunc TestTypes(t *testing.T) {}\n')
                (root / 'nested.go').write_text('package p\ntype Nested struct {\n\tCount int\n}\n')
                (root / 'nested_test.go').write_text('package p\nfunc TestNested(t *testing.T) {}\n')
                # The public test calls a helper; it never names Job or Nested.
                (root / 'wire_schema_test.go').write_text('package p\nfunc TestWireSchemas(t *testing.T) { generatedSchema(t) }\n')
                git('add', '.')
                git('-c', 'user.name=Fixture', '-c', 'user.email=fixture-noreply@example.invalid', 'commit', '-qm', 'base')
                (root / 'types.go').write_text('package p\ntype Job struct {\n\tValue string\n}\n')
                self.assertEqual(related.select('HEAD'), [('internal/myce', '^(TestTypes|TestWireSchemas)$')])
                git('restore', 'internal/myce/types.go')
                (root / 'nested.go').write_text('package p\ntype Nested struct {\n\tCount string\n}\n')
                self.assertEqual(related.select('HEAD'), [('internal/myce', '^(TestNested|TestWireSchemas)$')])
            finally:
                os.chdir(previous)

    def test_private_memory_writers_include_unchanged_consumers(self):
        previous = os.getcwd()
        with tempfile.TemporaryDirectory() as directory:
            os.chdir(directory)
            try:
                def git(*args):
                    subprocess.run(['git', *args], check=True, capture_output=True)
                root = pathlib.Path('internal/myce')
                root.mkdir(parents=True)
                git('init', '-q')
                # #490 changed private peak/complete; neither is an exported
                # symbol, and the consumers call helpers rather than name them.
                for source, method in [('attempt_memory', 'peak'), ('queue', 'complete')]:
                    (root / f'{source}.go').write_text(f'package p\nfunc (q *Queue) {method}() int {{ return 1 }}\n')
                    (root / f'{source}_test.go').write_text(f'package p\nfunc Test{source.title().replace("_", "")}(t *testing.T) {{}}\n')
                (root / 'process_memory_test.go').write_text('package p\nfunc TestAttemptMemoryEventCountsPersist(t *testing.T) { helper(t) }\n')
                (root / 'attempt_memory_facts_test.go').write_text('package p\nfunc TestAttemptMemoryFactsAcceptedGET(t *testing.T) { helper(t) }\n')
                (root / 'unrelated_test.go').write_text('package p\nfunc TestUnrelated(t *testing.T) {}\n')
                git('add', '.')
                git('-c', 'user.name=Fixture', '-c', 'user.email=fixture-noreply@example.invalid', 'commit', '-qm', 'base')
                for source, method in [('attempt_memory', 'peak'), ('queue', 'complete')]:
                    (root / f'{source}.go').write_text(f'package p\nfunc (q *Queue) {method}() int {{ return 2 }}\n')
                    selected = related.select('HEAD')
                    self.assertEqual(len(selected), 1)
                    self.assertIn('TestAttemptMemoryEventCountsPersist', selected[0][1])
                    self.assertIn('TestAttemptMemoryFactsAcceptedGET', selected[0][1])
                    self.assertNotIn('TestUnrelated', selected[0][1])
                    git('restore', f'internal/myce/{source}.go')
            finally:
                os.chdir(previous)

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
                # A field-only edit still selects direct field references,
                # even when those tests live in a differently named file.
                write('types.go', 'package p\ntype Snapshot struct {\n\tValue int\n}\n')
                write('status_test.go', 'package p\nfunc TestStatus(t *testing.T) { var s Snapshot; _ = s.Value }\nfunc TestOtherSnapshot(t *testing.T) { var s Snapshot }\n')
                git('add', '.')
                git('-c', 'user.name=Fixture', '-c', 'user.email=fixture-noreply@example.invalid', 'commit', '-qm', 'status')
                write('types.go', 'package p\ntype Snapshot struct {\n\tValue string\n}\n')
                self.assertEqual(related.select('HEAD'), [('.', '^(TestStatus)$')])
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
