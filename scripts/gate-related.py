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

# Platform clone helpers are exercised through model snapshot staging.
for source in ('shared_clone_darwin', 'shared_clone_linux', 'shared_clone_other'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/{source}.go'] = ('model_snapshots_test.go',)

# The monitor's tests predate the source split; it has no matching test filename.
SUPPLEMENTAL_CONSUMERS['internal/myce/engine_health.go'] = (
    'engine_recovery_test.go', 'engine_recovery_round3_test.go')

# Copy-runtime authorization is exercised through its lifecycle/maintenance
# consumers; the implementation predates the matching test filename convention.
SUPPLEMENTAL_CONSUMERS['internal/myce/worker_sync.go'] = (
    'sync_test.go', 'host_maintenance_work_test.go', 'server_entries_round4_test.go')

# Adaptive private budgets are exercised through their observable consumers.
SUPPLEMENTAL_CONSUMERS['internal/myce/observation_cache_budget.go'] = (
    'status_read_test.go',)
SUPPLEMENTAL_CONSUMERS['internal/myce/telemetry_budget.go'] = (
    'job_get_volume_test.go', 'request_timing_test.go', 'queue_mutex_test.go')
SUPPLEMENTAL_CONSUMERS['internal/myce/diagnostic_budget.go'] = (
    'evidence_budget_test.go', 'evidence_readiness_test.go')

# The non-race diagnostic benchmark has its correctness regressions here.
SUPPLEMENTAL_CONSUMERS['internal/myce/checkin_persistence_benchmark_test.go'] = (
    'queue_persistence_test.go',)
SUPPLEMENTAL_CONSUMERS['internal/myce/machine_conversion_install.go'] = (
    'machine_migration_test.go', 'machine_verified_empty_test.go',
    'machine_migration_spool_test.go')

# Platform socket helpers implement transfer progress underneath file clients.
for source in ('file_socket_unix', 'file_socket_other'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/{source}.go'] = (
        'client_adaptive_test.go', 'file_transfer_test.go')

# The non-Linux Xe probe withholds telemetry; Linux exercises the read-only PMU
# implementation; portable device and admission tests exercise missing evidence.
SUPPLEMENTAL_CONSUMERS['internal/myce/xe_activity_other.go'] = (
    'xe_activity_linux_test.go', 'device_telemetry_test.go', 'discrete_fanout_sharing_test.go')


# Shared atomic-file primitives and the tus datastore are exercised through real
# HTTP, subprocess-death and lost-barrier fixtures rather than filename twins.
for source in ('durable_files', 'durable_files_darwin', 'durable_files_other',
               'upload_durable'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/{source}.go'] = (
        'file_store_fault_test.go',)


def changed_symbols(diff):
    symbols = set()
    for receiver, method in re.findall(
            r'^(?:@@.*?@@ |[+-])func (?:\(([^)]*)\) )?([A-Z]\w*)', diff, re.M):
        # Read/Close/etc on private interface implementations are not references
        # to every unrelated reader/closer in the package. Match the concrete
        # receiver instead; conventional tests still cover helper-only callers.
        types = re.findall(r'[A-Za-z_]\w*', receiver)
        if types and types[-1][0].islower():
            symbols.add(types[-1])
        else:
            symbols.add(method)
    symbols.update(re.findall(r'^[+-](?:type|var|const) ([A-Z]\w*)', diff, re.M))
    return symbols


def git(*args):
    return subprocess.check_output(['git', *args], text=True)


def select(base):
    # A moving main may contain changes this PR never made. Compare from the
    # common ancestor even when the caller explicitly supplies origin/main;
    # ancestor SHAs still select the requested wider batch.
    base = git('merge-base', 'HEAD', base).strip()
    if git('diff', '--name-only', base, '--', 'go.mod', 'go.sum').strip():
        raise ValueError('module dependencies changed')
    changed = sorted(set((git('diff', '--name-only', base) +
                          git('ls-files', '--others', '--exclude-standard')).splitlines()))
    packages = {}
    for name in changed:
        if not name.endswith('.go'):
            continue
        path = pathlib.Path(name)
        # Tagged worker defaults run in the standalone CLI smoke fixture, not
        # the core package's in-process synthetic host.
        if name == 'internal/myce/worker_probes_fake.go':
            packages.setdefault(pathlib.Path('cmd/myce'), set()).add('TestDevStandaloneSmoke')
            continue
        # The shared fixture lifecycle is exercised through both real TestMains.
        if name == 'internal/testenv/lifecycle.go':
            for directory in ('internal/myce', 'cmd/myce'):
                packages.setdefault(pathlib.Path(directory), set()).add('TestPackageFixtureLifecycle')
            continue
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
        symbols = changed_symbols(diff)
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
            # Removed implementations/tests have nothing left to execute. Keep
            # surviving symbol consumers above, and still reject unmapped live code.
            if not path.exists():
                continue
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
