#!/usr/bin/env python3
"""Run both native Darwin and local Linux gates, without queue availability."""
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location('gate_capacity', Path(__file__).with_name('gate-capacity.py'))
capacity = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(capacity)


_cache_spec = importlib.util.spec_from_file_location('gate_cache', Path(__file__).with_name('gate-cache.py'))
cache_tools = importlib.util.module_from_spec(_cache_spec)
_cache_spec.loader.exec_module(cache_tools)
trim_cache = cache_tools.trim_cache


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else 'quick'
    # Use a dedicated, shared-across-checkouts cache so automatic eviction
    # cannot remove other projects' active Go build output.
    cache = Path.home() / 'Library/Caches/mycelium/gates/go-build'
    cache.mkdir(parents=True, exist_ok=True)
    slot = capacity.acquire(REPO, mode=mode, cores=4)
    env = dict(os.environ, GOCACHE=str(cache), GOMAXPROCS='4',
               GATE_TARGET='native', GATE_LOCAL_CAPACITY_HELD='1')
    report = REPO / '.reports/gate-local.jsonl'
    status = 0
    stopped = True
    try:
        # Trim only when no second heavy owner exists. Hold admission while
        # checking/evicting, preventing a new gate from racing the idle test.
        with capacity.admission_lock(capacity.root(REPO)):
            others = [p for p in capacity.root(REPO).glob('local-go-slot*')
                      if p.is_dir() and p != slot.directory]
            if not others:
                trim_cache(cache)
        sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
        with tempfile.TemporaryDirectory(prefix='local-', dir=REPO / '.gate') as folder:
            selection = Path(folder) / 'selection.tsv'
            if mode == 'quick':
                if len(sys.argv) > 2 and sys.argv[2] == '--selection':
                    shutil.copyfile(sys.argv[3], selection)
                else:
                    base = sys.argv[2] if len(sys.argv) > 2 else 'origin/main'
                    with selection.open('w') as output:
                        subprocess.run([sys.executable, str(REPO / 'scripts/gate-related.py'), base],
                                       stdout=output, cwd=REPO, check=True)
            # Preparation is local and may download dependencies once; the
            # inner gate remains offline and uses its existing fixture custody.
            subprocess.run(['go', 'mod', 'download'], cwd=REPO, env=env, check=True)
            if mode == 'full':
                subprocess.run([sys.executable, 'scripts/upgrade-compat.py', 'prepare'],
                               cwd=REPO, env=env, check=True)
            native_args = [mode] if mode == 'full' else ['quick', '--selection', str(selection)]
            measured = [sys.executable, 'scripts/gate-measure.py', '--report', str(report),
                        '--source-sha', sha, '--mode', mode, '--', 'bash', 'scripts/gate.sh']
            stopped = False
            native = subprocess.run([*measured, *native_args], cwd=REPO, env=env)
            status = native.returncode
            if status == 125:
                return status  # Safety/custody refusal stops dependent work.
            linux_args = [mode] if mode == 'full' else ['quick', '--selection', 'gate-selection.tsv']
            linux = [sys.executable, 'scripts/gate-linux.py', '--report', str(report)]
            if mode == 'quick':
                linux += ['--input', str(selection) + '=gate-selection.tsv']
            linux += ['--', 'python3', 'scripts/gate-measure.py', '--source-sha', sha,
                      '--mode', mode, '--', 'bash', 'scripts/gate.sh', *linux_args]
            result = subprocess.run(linux, cwd=REPO, env=env)
            status = status or result.returncode
            stopped = result.returncode != 125 and native.returncode != 125
        return status
    finally:
        # gate-budget marks incomplete fixture cleanup; retain local capacity
        # on any interruption/unknown result rather than assuming process exit.
        slot.release(stopped=stopped)


if __name__ == '__main__':
    try:
        (REPO / '.gate').mkdir(exist_ok=True)
        sys.exit(main())
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print('STOP local gate:', error, file=sys.stderr)
        sys.exit(125)
