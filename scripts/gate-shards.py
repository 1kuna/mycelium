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
    """Balance CLI/core chunks together so a finished process can refill."""
    if cores < 1:
        raise ValueError('core allocation must be positive')
    if cores == 1:
        return entries
    hints = json.loads(Path(__file__).with_name('gate-test-durations.json').read_text())['packages']
    inventories = {}
    for entry in entries:
        package, expression = entry.split('\t', 1)
        if package not in hints or expression != '.*':
            continue
        names = native_roots(package)
        if not names:
            raise ValueError('empty native inventory for sharded package ' + package)
        inventories[package] = names

    # Three chunks per slot leave refill work when timing estimates differ
    # from this host. Keep the total a multiple of the slots: independently
    # rounding each package up can add a nearly empty final scheduling wave.
    weights = {package: {name: max(.1, hints[package].get(name, .1)) for name in names}
               for package, names in inventories.items()}
    totals = {package: sum(values.values()) for package, values in weights.items()}
    counts = {package: 1 for package in inventories}
    budget = min(3 * min(9, cores), sum(len(names) for names in inventories.values()))
    for _ in range(budget - len(counts)):
        candidates = [package for package in counts if counts[package] < len(inventories[package])]
        package = max(candidates, key=lambda package: totals[package] / counts[package])
        counts[package] += 1
    expanded = []
    for entry in entries:
        package, expression = entry.split('\t', 1)
        if package not in inventories or expression != '.*':
            expanded.append((0., entry))
            continue
        names = inventories[package]
        groups = partition(names, hints[package], counts[package])
        for group in groups:
            # Slash-free anchored roots select every descendant in that root.
            expression = '^(' + '|'.join(re.escape(name) for name in group) + ')$'
            expanded.append((sum(weights[package][name] for name in group), package + '\t' + expression))
        print(f'gate shards {package}: {len(names)} roots in {len(groups)} processes', flush=True)
    # Longest chunks first, across packages. Package ordering would otherwise
    # strand the CLI behind the whole core workload (or the reverse).
    return [entry for _, entry in sorted(expanded, key=lambda row: -row[0])]
