#!/usr/bin/env python3
"""Release regressions use fake tools, never a keychain or signing key."""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('release_build', Path(__file__).with_name('release-build.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
VERSION = 'v3.1.0-test+build'
COMMIT = '0123456789abcdef0123456789abcdef01234567'
MATRIX = 'linux amd64 0\nlinux arm64 0\ndarwin arm64 1\n'


class ReleaseBuildTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.calls = []
        self.mac = patch.object(r, 'native_mac')
        self.mac.start()
        self.addCleanup(self.mac.stop)
        self.tool = patch.object(r, 'run', side_effect=self.run_tool)
        self.tool.start()
        self.addCleanup(self.tool.stop)

    def run_tool(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if args[:3] == ('go', 'version', '-m'):
            osname, arch = Path(args[3]).name.split('-')[1:]
            flags = f'-X {r.PREFIX}BuildVersion={VERSION} -X {r.PREFIX}BuildCommit={COMMIT}'
            return (f'\tbuild\t-ldflags={json.dumps(flags)}\n\tbuild\tGOOS={osname}\n'
                    f'\tbuild\tGOARCH={arch}\n\tbuild\tCGO_ENABLED={int(osname == "darwin")}\n')
        if args[:3] == ('go', 'tool', 'buildid'):
            return f'myce-release:{VERSION}:{COMMIT}\n'
        if args[:2] == ('go', 'build'):
            Path(args[args.index('-o') + 1]).write_bytes(b'fake executable')
        elif args[:2] == ('git', 'rev-parse'):
            return COMMIT + '\n'
        elif args[:2] == ('git', 'status'):
            return ''
        elif args[-2:] == ('version', '--json'):
            return json.dumps(dict(version=VERSION, commit=COMMIT, os='darwin', arch='arm64',
                                   native_sensing=True, start_gate='start-v1', attempt_protocol='attempt-v1'))
        elif args[-2:] == ('release', 'build-targets'):
            return MATRIX
        return ''

    def fixtures(self):
        linux, darwin = self.root / 'linux', self.root / 'darwin'
        for directory, kind, names in [(linux, 'linux', ['amd64', 'arm64']), (darwin, 'darwin', ['arm64'])]:
            directory.mkdir()
            records = []
            for arch in names:
                path = directory / f'myce-{kind}-{arch}'
                path.write_bytes(b'fake executable')
                records.append(r.artifact(path))
            r.write_json(directory / f'{kind}-build.json', dict(version=VERSION, commit=COMMIT, artifacts=records))
        return linux, darwin

    def test_input_validation(self):
        for version in ['', 'VERSION', 'v3.1', 'v03.1.0', 'v3.1.0-01', 'v3.1.0;echo', 'v3.1.0\n']:
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, 'semver'):
                r.validate(version, COMMIT)
        for commit in ['', 'HEAD', '1234567', COMMIT.upper(), COMMIT + '\n']:
            with self.subTest(commit=commit), self.assertRaisesRegex(ValueError, 'Git SHA'):
                r.validate(VERSION, commit)
        r.validate(VERSION, COMMIT)
        self.assertEqual(self.calls, [])

    def test_linux_build_literal_inputs_and_hashes(self):
        with patch.object(r.platform, 'system', return_value='Linux'), patch.object(r, 'targets', return_value=[tuple(x.split()) for x in MATRIX.splitlines()]), patch.object(r.Path, 'exists', return_value=False):
            r.build('linux', VERSION, COMMIT, self.root / 'built')
        receipt = json.loads((self.root / 'built/linux-build.json').read_text())
        self.assertEqual((receipt['version'], receipt['commit']), (VERSION, COMMIT))
        self.assertEqual(len(receipt['artifacts']), 2)
        for record in receipt['artifacts']:
            self.assertEqual(record, r.artifact(self.root / 'built' / record['file']))
        builds = [(a, k) for a, k in self.calls if a[:2] == ('go', 'build')]
        self.assertEqual(len(builds), 2)
        self.assertIn('BuildVersion=' + VERSION, builds[0][0][5])
        self.assertFalse(any('codesign' in a for a, _ in self.calls))

    def test_real_trimpath_cross_build_metadata(self):
        # Real Go metadata catches assumptions that mocked tool output cannot.
        self.tool.stop()
        source = self.root / 'main.go'
        source.write_text('package main\nfunc main() {}\n')
        path = self.root / 'sample'
        subprocess.run(['go', 'build', '-trimpath', '-buildvcs=false', '-ldflags',
                        f'-buildid=myce-release:{VERSION}:{COMMIT}', '-o', str(path), str(source)],
                       check=True, env=dict(os.environ, GOOS='linux', GOARCH='arm64', CGO_ENABLED='0'))
        r.metadata(path, VERSION, COMMIT, ('linux', 'arm64', '0'))
        with self.assertRaisesRegex(ValueError, 'version/commit mismatch'):
            r.metadata(path, 'v3.1.1', COMMIT, ('linux', 'arm64', '0'))

    def test_fleet_cli_submits_literal_arguments_and_collects_profile_paths(self):
        linux, _ = self.fixtures()
        def fleet_tools(*args, **kwargs):
            if args[:2] == ('myce', 'run'):
                self.assertEqual(args, ('myce', 'run', '--profile', 'release-build', '--', VERSION, COMMIT))
                return json.dumps({'job_id': 'fake-job'})
            if args[:2] == ('myce', 'result'):
                shutil.copytree(linux, Path(args[-1]) / 'release-linux')
                return ''
            return self.run_tool(*args, **kwargs)
        with patch.object(r, 'run', side_effect=fleet_tools), patch.dict(os.environ, {'MYCE_DEVELOPER_ID': 'fake-identity'}), patch.object(sys, 'argv', ['release-build.py', 'fleet', VERSION, COMMIT, str(self.root / 'fleet')]):
            r.main()
        self.assertTrue((self.root / 'fleet/release/manifest.unsigned.json').is_file())
        self.assertTrue(any(args[:2] == ('myce', 'wait') for args, _ in self.calls))
        self.assertFalse(any('manifest' in args for args, _ in self.calls))

    def test_darwin_missing_identity_stops_before_tools(self):
        with patch.dict(os.environ, {'MYCE_DEVELOPER_ID': ''}), self.assertRaisesRegex(ValueError, 'MYCE_DEVELOPER_ID'):
            r.build('darwin', VERSION, COMMIT, self.root / 'built')
        self.assertEqual(self.calls, [])

    def test_source_commit_mismatch_stops_before_build(self):
        with patch.object(r.platform, 'system', return_value='Linux'), patch.object(r.Path, 'exists', return_value=True), patch.object(r, 'run', return_value='f' * 40), self.assertRaisesRegex(ValueError, 'source commit mismatch'):
            r.build('linux', VERSION, COMMIT, self.root / 'built')
        self.assertFalse((self.root / 'built').exists())

    def test_assemble_receipt_mismatches_stop_before_tools(self):
        linux, darwin = self.fixtures()
        for directory, kind in [(linux, 'linux'), (darwin, 'darwin')]:
            for field, wrong in [('version', 'v3.1.1'), ('commit', 'f' * 40)]:
                path = directory / f'{kind}-build.json'
                original = json.loads(path.read_text())
                changed = dict(original, **{field: wrong})
                r.write_json(path, changed)
                with self.subTest(kind=kind, field=field), self.assertRaisesRegex(ValueError, field + ' mismatch'):
                    r.assemble(VERSION, COMMIT, linux, darwin, self.root / 'out', 'fake-key')
                r.write_json(path, original)
        self.assertEqual(self.calls, [])
        self.assertFalse((self.root / 'out').exists())

    def test_embedded_linux_version_and_commit_mismatch(self):
        linux, darwin = self.fixtures()
        for field, wrong in [('version', 'v3.1.1'), ('commit', 'f' * 40)]:
            def wrong_metadata(*args, **kwargs):
                result = self.run_tool(*args, **kwargs)
                if args[:3] == ('go', 'tool', 'buildid') and args[3].endswith('myce-linux-arm64'):
                    old = VERSION if field == 'version' else COMMIT
                    return result.replace(old, wrong)
                return result
            with patch.object(r, 'run', side_effect=wrong_metadata), self.assertRaisesRegex(ValueError, 'artifact .* mismatch'):
                r.assemble(VERSION, COMMIT, linux, darwin, self.root / 'out', 'fake-key')
        self.assertFalse((self.root / 'out').exists())
        self.assertFalse(any('manifest' in args for args, _ in self.calls))

    def test_profile_check_verifies_linux_receipt(self):
        linux, _ = self.fixtures()
        r.check_linux(linux)
        (linux / 'myce-linux-arm64').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'hash/size mismatch'):
            r.check_linux(linux)

    def test_changed_bytes_refused(self):
        linux, darwin = self.fixtures()
        (linux / 'myce-linux-amd64').write_bytes(b'changed after receipt')
        with self.assertRaisesRegex(ValueError, 'hash/size mismatch'):
            r.assemble(VERSION, COMMIT, linux, darwin, self.root / 'out')
        self.assertFalse((self.root / 'out').exists())

    def test_assembly_preserves_bytes_and_separates_signing(self):
        linux, darwin = self.fixtures()
        output = self.root / 'out'
        r.assemble(VERSION, COMMIT, linux, darwin, output)
        manifest = json.loads((output / 'manifest.unsigned.json').read_text())
        self.assertEqual(len(manifest['artifacts']), 3)
        self.assertEqual(len((output / 'SHA256SUMS').read_text().splitlines()), 3)
        self.assertEqual(json.loads((output / 'build-provenance.json').read_text())['commit'], COMMIT)
        self.assertFalse((output / 'release.json').exists())
        self.assertFalse(any('manifest' in args for args, _ in self.calls))
        self.assertTrue(any(args[:3] == ('codesign', '--verify', '--strict') for args, _ in self.calls))


if __name__ == '__main__':
    unittest.main()
