#!/usr/bin/env python3
"""Read host-side namespace evidence for the exact detached fatal fixture."""
import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

PROC = Path('/proc')
CGROUPS = Path('/sys/fs/cgroup')
PYTHON = Path('/usr/bin/python3')
MARKER = b'myce-fatal-signal-payload'
SCOPE = re.compile(r'myce-attempt-([0-9a-f]{32})\.scope')
IDENTITY = re.compile(r'attempt-([0-9a-f]{32})')
NAMESPACES = ('user', 'mnt', 'pid', 'ipc', 'net')


def namespaces(process):
    return {name: (process / 'ns' / name).readlink().name for name in NAMESPACES}


def start_ticks(process):
    return (process / 'stat').read_text().rsplit(')', 1)[1].split()[19]


def is_fixture(process):
    command = (process / 'cmdline').read_bytes().split(b'\0')
    return command[-2:] == [MARKER, b''] and (process / 'exe').samefile(PYTHON)


def sample(group, baseline):
    # cgroup.procs is per cgroup. Include nested groups and retain the exact
    # membership path the kernel core router will report for the crashing PID.
    for members in group.rglob('cgroup.procs'):
        for pid in members.read_text().split():
            process = PROC / pid
            try:
                birth = start_ticks(process)
                # A launch wrapper can carry the same marker before Python execs.
                if not is_fixture(process):
                    continue
                observed = namespaces(process)
                if any(observed[name] == baseline[name] for name in NAMESPACES):
                    continue
                label = (process / 'attr/current').read_text().strip()
                uid_map = (process / 'uid_map').read_text().split()
                if not is_fixture(process) or start_ticks(process) != birth or pid not in members.read_text().split():
                    continue
            except (FileNotFoundError, ProcessLookupError):
                continue
            return {
                'at': datetime.now(timezone.utc).isoformat(), 'pid': int(pid),
                'cgroup': '/' + str(members.parent.relative_to(CGROUPS)),
                'start_ticks': birth, 'label': label, 'uid_map': uid_map,
                'host_namespaces': baseline, 'payload_namespaces': observed,
            }


def observe(scope, scope_id, args, baseline):
    record = args.state_dir / f'custodian-{scope_id}.state.json'
    state = json.loads(record.read_text())
    job = state.get('job') or {}
    if (job.get('id'), job.get('attempt'), job.get('session')) != (args.job, args.attempt, args.session):
        return
    group = Path(state.get('cgroup', ''))
    identity = IDENTITY.fullmatch(group.name)
    if group.parent != scope or not identity or identity[1] != state.get('identity'):
        return
    if job.get('spec', {}).get('worker_data') != 'ephemeral':
        raise SystemExit('Exact attempt state does not record ephemeral policy')
    evidence = sample(group, baseline)
    if evidence:
        return dict(evidence, job=args.job, attempt=args.attempt, session=args.session, identity=group.name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('state_dir', type=Path)
    parser.add_argument('job')
    parser.add_argument('attempt', type=int)
    parser.add_argument('session')
    parser.add_argument('--worker-cgroup', type=Path, required=True)
    parser.add_argument('--wait', type=float, required=True)
    args = parser.parse_args()
    worker_slice = args.worker_cgroup.relative_to('/').parent
    if worker_slice.suffix != '.slice':
        parser.error('worker cgroup parent must be a systemd slice')
    jobs_slice = CGROUPS / worker_slice / (worker_slice.stem + '-jobs.slice')
    baseline = namespaces(PROC / '1')
    deadline = time.monotonic() + args.wait
    while time.monotonic() < deadline:
        for scope in jobs_slice.glob('myce-attempt-*.scope'):
            match = SCOPE.fullmatch(scope.name)
            if not match:
                continue
            try:
                evidence = observe(scope, match[1], args, baseline)
            except FileNotFoundError:
                continue
            if evidence:
                print(json.dumps(evidence, sort_keys=True))
                return
        time.sleep(0.25)
    raise SystemExit('No matching fatal Python payload observed before deadline')


if __name__ == '__main__':
    main()
