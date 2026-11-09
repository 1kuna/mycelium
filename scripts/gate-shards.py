#!/usr/bin/env python3
"""Partition the native full-test inventory; timing hints never select coverage."""
import json
import math
from pathlib import Path
import re
import subprocess
import sys


def partition(names: list[str], durations: dict[str, float], count: int) -> list[list[str]]:
    """Greedily balance measured work, keeping each current root exactly once."""
    if len(names) != len(set(names)):
        raise ValueError('duplicate roots in native test inventory')
    if count < 1:
        raise ValueError('shard count must be positive')
    # Unmeasured or cheap roots still consume a slot and are spread evenly.
    def weight(name: str) -> float:
        value = durations.get(name, 0.1)
        if not math.isfinite(value) or value < 0:
            raise ValueError('invalid duration for ' + name)
        return max(0.1, value)

    bins: list[list[str]] = [[] for _ in range(min(count, len(names)))]
    loads = [0.0] * len(bins)
    for name in sorted(names, key=lambda name: (-weight(name), name)):
        index = min(range(len(bins)), key=lambda i: (loads[i], len(bins[i]), i))
        bins[index].append(name)
        loads[index] += weight(name)
    return [sorted(group) for group in bins]


def native_roots(package: str) -> list[str]:
    """Use Go's compiled inventory under the same race tags and fixture custody."""
    progress = Path(__file__).with_name('gate-progress.py')
    command = [sys.executable, str(progress), 'inventory:' + package,
               'go', 'test', '-race', '-vet=off', '-json', '-p', '1',
               '-list', '.', '-run', '^$', package]
    # The watchdog owns HOME/temp and joins TestMain's helpers before returning.
    child = subprocess.Popen(command, stdout=subprocess.PIPE)
    try:
        output, _ = child.communicate()
    except (KeyboardInterrupt, SystemExit):
        # Let the inventory watchdog finish its custody cleanup; do not let
        # subprocess.run kill it after the short KeyboardInterrupt grace period.
        child.terminate()
        output, _ = child.communicate()
        sys.stdout.buffer.write(output)
        sys.stdout.buffer.flush()
        if child.returncode < 0 or child.returncode in (124, 125):
            raise SystemExit(125 if child.returncode < 0 else child.returncode)
        raise
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()
    if child.returncode:
        raise SystemExit(125 if child.returncode < 0 else child.returncode)
    names = []
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            continue
        if not isinstance(event, dict) or event.get('Package') != package:
            continue
        # Go -list reports runnable examples and fuzz seeds as well as Tests.
        if event.get('Action') == 'output':
            name = event.get('Output', '').strip()
            if re.fullmatch(r'(?:Test|Example|Fuzz)\w*', name):
                names.append(name)
    return names


def plan(entries: list[str], cores: int) -> list[str]:
    """Expand measured dominant packages; retain other package selections intact."""
    hints = json.loads(Path(__file__).with_name('gate-test-durations.json').read_text())['packages']
    expanded = []
    # At most eight core processes plus CLI: nine busy cores on an 18-core host.
    # Smaller allocations leave one process slot for CLI/other packages.
    count = max(1, min(8, cores - 1))
    for entry in entries:
        package, expression = entry.split('\t', 1)
        if package not in hints or expression != '.*' or count == 1:
            expanded.append(entry)
            continue
        names = native_roots(package)
        if not names:
            raise ValueError('empty native inventory for sharded package ' + package)
        groups = partition(names, hints[package], count)
        for group in groups:
            # Slash-free anchored roots select every descendant in that root.
            expression = '^(' + '|'.join(re.escape(name) for name in group) + ')$'
            expanded.append(package + '\t' + expression)
        print(f'gate shards {package}: {len(names)} roots in {len(groups)} processes', flush=True)
    return expanded
