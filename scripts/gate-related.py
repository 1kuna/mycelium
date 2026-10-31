#!/usr/bin/env python3
"""Conventional test files plus direct textual references, not dependency analysis."""
import os
import pathlib
import re
import subprocess
import sys


# Private learning writers have consumers in differently named files (#493).
# Keep this explicit and bounded; this is not a transitive call graph.
MEMORY_CONSUMERS = ('process_memory_test.go', 'attempt_memory_facts_test.go')
SUPPLEMENTAL_CONSUMERS = {
    f'internal/myce/{source}.go': MEMORY_CONSUMERS
    for source in ('attempt_memory', 'memory_learning', 'memory_envelope',
                   'gpu_host_memory', 'capability_placement', 'work_observation', 'queue')
}


def git(*args):
    return subprocess.check_output(['git', *args], text=True)


def select(base):
    if git('diff', '--name-only', base, '--', 'go.mod', 'go.sum').strip():
        raise ValueError('module dependencies changed')
    changed = sorted(set((git('diff', '--name-only', base) +
                          git('ls-files', '--others', '--exclude-standard')).splitlines()))
    packages = {}
    for name in changed:
        if not name.endswith('.go'):
            continue
        path = pathlib.Path(name)
        tests = set()
        stem = path.stem.removesuffix('_test')
        for test_file in path.parent.glob(stem + '*_test.go'):
            tests.update(re.findall(r'^func (Test\w+)\(', test_file.read_text(), re.M))
        for consumer in SUPPLEMENTAL_CONSUMERS.get(name, ()):
            test_file = path.parent / consumer
            if test_file.exists():
                tests.update(re.findall(r'^func (Test\w+)\(', test_file.read_text(), re.M))
        # Hunk headers identify the enclosing function; +/- declarations also
        # cover renamed functions and exported types/variables/constants.
        diff = git('diff', '--unified=0', base, '--', name)
        symbols = set(re.findall(r'^(?:@@.*?@@ |[+-])func (?:\([^)]*\) )?([A-Z]\w*)', diff, re.M))
        symbols.update(re.findall(r'^[+-](?:type|var|const) ([A-Z]\w*)', diff, re.M))
        # Field-only struct edits need their direct tests even after test moves.
        if not tests and not symbols:
            symbols.update(re.findall(r'^[+-]\t([A-Z]\w*)\s+', diff, re.M))
        if symbols:
            reference = re.compile(r'\b(?:' + '|'.join(sorted(symbols)) + r')\b')
            for test_file in path.parent.glob('*_test.go'):
                # Split at top-level functions, so a reference selects its test
                # rather than every unrelated test in the same file.
                for block in re.split(r'(?=^func )', test_file.read_text(), flags=re.M):
                    test = re.match(r'func (Test\w+)\(', block)
                    if test and reference.search(block):
                        tests.add(test[1])
        if not tests:
            raise ValueError(f'no related tests for {name}')
        # The reflected Job schema includes nested types across this package.
        # Textual references miss field-only edits and indirect definitions (#413).
        if path.parent == pathlib.Path('internal/myce') and not name.endswith('_test.go'):
            tests.add('TestWireSchemas')
        packages.setdefault(path.parent, set()).update(tests)
    # Fixture paths are relative to the Go package's working directory. Match
    # direct literals in test bodies, including docs/examples-only changes.
    fixtures = [name for name in changed if not name.endswith('.go')]
    if fixtures:
        for name in git('ls-files', '--', '*_test.go').splitlines():
            path = pathlib.Path(name)
            if not path.is_file():
                continue
            references = ['"' + os.path.relpath(fixture, path.parent) + '"'
                          for fixture in fixtures]
            for block in re.split(r'(?=^func )', path.read_text(), flags=re.M):
                test = re.match(r'func (Test\w+)\(', block)
                if test and any(reference in block for reference in references):
                    packages.setdefault(path.parent, set()).add(test[1])
    return [(str(directory), '^(' + '|'.join(sorted(tests)) + ')$')
            for directory, tests in sorted(packages.items())]


if __name__ == '__main__':
    try:
        for directory, pattern in select(sys.argv[1]):
            print(f'{directory}\t{pattern}')
    except ValueError as error:
        print(f'quick: {error}; run ./scripts/gate.sh full', file=sys.stderr)
        sys.exit(2)
