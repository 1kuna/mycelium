#!/usr/bin/env python3
"""Repo-owned release builds. No publishing or deployment operations."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys

SIGN_REQUIREMENT = ('anchor apple generic and certificate leaf[field.1.2.840.113635.100.6.1.13] '
                    'exists and identifier "io.github.1kuna.mycelium"')
PREFIX = 'mycelium/internal/myce.'


def run(*args, capture=False, env=None):
    return subprocess.run(args, check=True, text=True, env=env,
                          stdout=subprocess.PIPE if capture else None).stdout


def validate(version, commit):
    number = r'(?:0|[1-9][0-9]*)'
    ident = r'(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)'
    if not re.fullmatch(rf'v{number}\.{number}\.{number}(?:-{ident}(?:\.{ident})*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?', version):
        raise ValueError('version must be a full v-prefixed semver')
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('commit must be a full lowercase 40-character Git SHA')


def native_mac():
    if (platform.system(), platform.machine()) != ('Darwin', 'arm64'):
        raise ValueError('native macOS arm64 release host required')


def verify_signature(path):
    run('codesign', '--verify', '--strict', '-R=' + SIGN_REQUIREMENT, str(path))


def metadata(path, version, commit, target):
    # Read cross-platform Go build metadata; never execute a foreign binary.
    settings = {}
    for line in run('go', 'version', '-m', str(path.resolve()), capture=True).splitlines():
        if line.startswith('\tbuild\t'):
            key, _, value = line[len('\tbuild\t'):].partition('=')
            if value.startswith('"'):
                value = json.loads(value)
            settings[key] = value
    # -trimpath intentionally removes -ldflags from Go build info. Stamp the
    # exact same inputs into the build ID so foreign artifacts remain verifiable.
    build_id = run('go', 'tool', 'buildid', str(path.resolve()), capture=True).strip()
    if build_id != f'myce-release:{version}:{commit}':
        raise ValueError('artifact version/commit mismatch: ' + path.name)
    if (settings.get('GOOS'), settings.get('GOARCH'), settings.get('CGO_ENABLED')) != target:
        raise ValueError('artifact target/CGO mismatch: ' + path.name)


def artifact(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError('artifact must be a regular file: ' + path.name)
    digest = hashlib.sha256()
    with path.open('rb') as src:
        for block in iter(lambda: src.read(1024 * 1024), b''):
            digest.update(block)
    return {'file': path.name, 'sha256': digest.hexdigest(), 'size': path.stat().st_size}


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + '\n')


def targets():
    rows = run('go', 'run', './cmd/myce', 'release', 'build-targets', capture=True)
    return [tuple(row.split()) for row in rows.splitlines()]


def build(kind, version, commit, output):
    validate(version, commit)
    if kind == 'darwin':
        native_mac()
        if not os.environ.get('MYCE_DEVELOPER_ID'):
            raise ValueError('MYCE_DEVELOPER_ID is required for a native Darwin release')
    elif platform.system() != 'Linux':
        raise ValueError('Linux release profile requires a Linux worker')
    # Job source archives have no Git history. The fleet submitter fences HEAD;
    # local checkouts must independently match the requested source commit.
    if Path('.git').exists():
        if run('git', 'rev-parse', 'HEAD', capture=True).strip() != commit:
            raise ValueError('source commit mismatch')
        if run('git', 'status', '--porcelain', '--untracked-files=normal', capture=True).strip():
            raise ValueError('release source must be committed and clean')
    output.mkdir(parents=True, exist_ok=False)
    records = []
    for osname, arch, cgo in targets():
        if osname != kind:
            continue
        path = output / f'myce-{osname}-{arch}'
        env = dict(os.environ, GOOS=osname, GOARCH=arch, CGO_ENABLED=cgo,
                   GOTOOLCHAIN='local', GOFLAGS='-p=2', GOWORK='off')
        run('go', 'build', '-trimpath', '-buildvcs=false', '-ldflags',
            f'-buildid=myce-release:{version}:{commit} -X {PREFIX}BuildVersion={version} -X {PREFIX}BuildCommit={commit}',
            '-o', str(path), './cmd/myce', env=env)
        if kind == 'darwin':
            run('codesign', '--force', '--options', 'runtime', '--timestamp',
                '--identifier', 'io.github.1kuna.mycelium', '--sign',
                os.environ['MYCE_DEVELOPER_ID'], str(path))
            verify_signature(path)
            info = json.loads(run(str(path.resolve()), 'version', '--json', capture=True))
            if (info['version'], info['commit'], info['native_sensing']) != (version, commit, True):
                raise ValueError('Darwin version/commit/native sensing mismatch')
        metadata(path, version, commit, (osname, arch, cgo))
        records.append(artifact(path))
    if not records:
        raise ValueError('release matrix has no matching targets')
    write_json(output / f'{kind}-build.json', {'version': version, 'commit': commit, 'artifacts': records})


def check_linux(output):
    receipt = json.loads((output / 'linux-build.json').read_text())
    validate(receipt['version'], receipt['commit'])
    records = []
    for osname, arch, cgo in targets():
        if osname != 'linux':
            continue
        path = output / f'myce-{osname}-{arch}'
        metadata(path, receipt['version'], receipt['commit'], (osname, arch, cgo))
        records.append(artifact(path))
    if not records or records != receipt['artifacts']:
        raise ValueError('Linux artifact hash/size mismatch')


def assemble(version, commit, linux, darwin, output, signing_key=None):
    validate(version, commit)
    native_mac()
    binary = darwin / 'myce-darwin-arm64'
    # Fail mismatched receipts before invoking the candidate executable or key.
    receipts = {}
    for kind, directory in [('linux', linux), ('darwin', darwin)]:
        receipt = json.loads((directory / f'{kind}-build.json').read_text())
        if receipt['version'] != version:
            raise ValueError('build receipt version mismatch: ' + kind)
        if receipt['commit'] != commit:
            raise ValueError('build receipt commit mismatch: ' + kind)
        receipts[kind] = receipt
    verify_signature(binary)
    info = json.loads(run(str(binary.resolve()), 'version', '--json', capture=True))
    if (info['version'], info['commit'], info['os'], info['arch'], info['native_sensing']) != (version, commit, 'darwin', 'arm64', True):
        raise ValueError('Darwin version/commit/target/native sensing mismatch')
    matrix = [tuple(row.split()) for row in run(str(binary.resolve()), 'release', 'build-targets', capture=True).splitlines()]
    records = []
    for osname, arch, cgo in matrix:
        directory = linux if osname == 'linux' else darwin
        path = directory / f'myce-{osname}-{arch}'
        metadata(path, version, commit, (osname, arch, cgo))
        actual = artifact(path)
        expected = [r for r in receipts[osname]['artifacts'] if r['file'] == path.name]
        if expected != [actual]:
            raise ValueError('artifact hash/size mismatch: ' + path.name)
        records.append(dict(actual, os=osname, arch=arch, native_sensing=(osname == 'darwin')))
    output.mkdir(parents=True, exist_ok=False)
    for record in records:
        directory = linux if record['os'] == 'linux' else darwin
        shutil.copy2(directory / record['file'], output / record['file'])
        if artifact(output / record['file']) != {k: record[k] for k in ('file', 'sha256', 'size')}:
            raise ValueError('assembled artifact hash mismatch')
    manifest = {'version': version, 'start_gate': info['start_gate'],
                'attempt_protocol': info['attempt_protocol'], 'artifacts': records}
    write_json(output / 'manifest.unsigned.json', manifest)
    write_json(output / 'build-provenance.json', {'version': version, 'commit': commit, 'builds': receipts})
    (output / 'SHA256SUMS').write_text(''.join(f"{r['sha256']}  {r['file']}\n" for r in records))
    if signing_key:
        run(str((output / binary.name).resolve()), 'release', 'manifest', '--dir', str(output),
            '--version', version, '--signing-key', signing_key)


def fleet(version, commit, output, signing_key=None):
    validate(version, commit)
    native_mac()
    if not os.environ.get('MYCE_DEVELOPER_ID'):
        raise ValueError('MYCE_DEVELOPER_ID is required for a native Darwin release')
    if run('git', 'rev-parse', 'HEAD', capture=True).strip() != commit:
        raise ValueError('source commit mismatch')
    if run('git', 'status', '--porcelain', '--untracked-files=normal', capture=True).strip():
        raise ValueError('release source must be committed and clean')
    # Resolve before submission, using the committed profile without rewriting it.
    run('myce', 'spec', 'check', '--resolve', '.mycelium/profiles/release-build.yaml')
    output.mkdir(parents=True, exist_ok=False)
    receipt = json.loads(run('myce', 'run', '--profile', 'release-build', '--', version, commit, capture=True))
    write_json(output / 'linux-job.json', receipt)
    run('myce', 'wait', receipt['job_id'])
    run('myce', 'result', receipt['job_id'], '--out', str(output / 'linux'))
    build('darwin', version, commit, output / 'darwin')
    assemble(version, commit, output / 'linux' / 'release-linux', output / 'darwin', output / 'release', signing_key)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    check = sub.add_parser('check-linux')
    check.add_argument('output', type=Path)
    for action in ('linux', 'darwin', 'assemble', 'fleet'):
        p = sub.add_parser(action)
        p.add_argument('version')
        p.add_argument('commit')
        if action == 'assemble':
            p.add_argument('linux', type=Path)
            p.add_argument('darwin', type=Path)
        p.add_argument('output', type=Path, **({'nargs': '?', 'default': Path('release-linux')} if action == 'linux' else {}))
        if action in ('assemble', 'fleet'):
            p.add_argument('--signing-key', help='optional local Ed25519 key; never sent to Mycelium')
    args = vars(parser.parse_args())
    action = args.pop('action')
    if action == 'check-linux':
        check_linux(**args)
    elif action in ('linux', 'darwin'):
        build(action, **args)
    else:
        globals()[action](**args)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as exc:
        # Do not repeat argv: codesign arguments contain the private identity.
        print('release build failed: ' + (f'tool exited {exc.returncode}' if isinstance(exc, subprocess.CalledProcessError) else str(exc)), file=sys.stderr)
        sys.exit(1)
