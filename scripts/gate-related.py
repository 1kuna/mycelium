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

# Platform signal capture helpers are covered by the shared native runner tests.
SUPPLEMENTAL_CONSUMERS['internal/myce/native_namespace_other.go'] = (
    'native_group_test.go', 'fault_evidence_test.go')
SUPPLEMENTAL_CONSUMERS['internal/myce/signal_evidence.go'] = (
    'fault_evidence_test.go',)
# The build-tagged fleet stability window has no tests of its own; the rollout
# tests wait it out (the synthetic end-to-end builds use the short one).
for _variant in ('fake', 'real'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/fleet_stability_{_variant}.go'] = (
        'fleet_upgrade_test.go',)

# The queue's own fleet runtime loop is exercised with the worker runtime (#1393).
SUPPLEMENTAL_CONSUMERS['internal/myce/fleet_queue_runtime.go'] = (
    'fleet_runtime_test.go', 'fleet_upgrade_test.go')

# The shared CLI image helper has no test roots of its own. Exercise its real
# child-process launch and synthetic-probe boundary, including in quick mode.
SUPPLEMENTAL_CONSUMERS['cmd/myce/binary_test.go'] = ('dev_probes_test.go',)

# Observer teardown helpers are exercised by the real adapter journey.
SUPPLEMENTAL_CONSUMERS['cmd/myce/observer_fixture_test.go'] = ('adapter_test.go',)

# `result recover` is dispatched through resultCommand; its dev regressions live there.
SUPPLEMENTAL_CONSUMERS['cmd/myce/result_recover.go'] = ('result_test.go',)

# PCI facts feed onboarding, worker inventory and backend descriptor dispatch.
SUPPLEMENTAL_CONSUMERS['internal/myce/pci_inventory.go'] = (
    'device_detection_test.go', 'device_inventory_family_test.go',
    'probe_consolidation_test.go', 'accelerator_backend_test.go')

# Actual check-in admission decisions are exercised by the admission wait tests.
SUPPLEMENTAL_CONSUMERS['internal/myce/admission_decision.go'] = ('admission_wait_test.go',)
# The Linux owner probe and its constructor are exercised by the owner-signal tests.
for source in ('owner_signals_linux', 'owner_signals_linux_probe'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/{source}.go'] = ('owner_signals_test.go',)
# Cold-load readiness alerts are reviewed through the readiness regressions.
SUPPLEMENTAL_CONSUMERS['internal/myce/readiness_health.go'] = ('readiness_review_test.go',)
# The one memory rule's charge, room and release regressions (#990).
SUPPLEMENTAL_CONSUMERS['internal/myce/memory_room.go'] = (
    'used_memory_charge_test.go', 'host_memory_growth_test.go', 'stale_memory_charge_test.go',
    'declared_host_memory_test.go', 'learned_memory_guard_test.go', 'host_release_search_test.go',
    'memory_admission_test.go', 'unified_launch_memory_test.go', 'metal_footprint_charge_test.go')

# Backend accounting policy feeds these existing memory/placement regressions.
SUPPLEMENTAL_CONSUMERS['internal/myce/memory_sizing_facts.go'] = (
    'attempt_memory_facts_test.go', 'process_memory_test.go',
    'memory_envelope_test.go', 'memory_observations_test.go')
SUPPLEMENTAL_CONSUMERS['internal/myce/model_readiness_resources.go'] = (
    'models_readiness_test.go', 'readiness_review_test.go', 'engine_probe_memory_test.go')

# Device-use ranking extends the existing placement regressions.
SUPPLEMENTAL_CONSUMERS['internal/myce/device_use.go'] = (
    'machine_placement_test.go', 'benchmark_test.go')

# Platform clone helpers are exercised through model snapshot staging.
for source in ('shared_clone_darwin', 'shared_clone_linux', 'shared_clone_other'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/{source}.go'] = ('model_snapshots_test.go',)
for platform in ('darwin', 'linux', 'other'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/model_snapshots_clone_{platform}_test.go'] = (
        'model_snapshots_test.go',)

# Local-volume platform probes are exercised through real installer preflight.
for source in ('model_root_local', 'model_root_local_darwin', 'model_root_local_linux', 'model_root_local_other'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/{source}.go'] = ('onboarding_storage_test.go',)

# The shared ledger's pressure tests exercise these private cache adapters.
for source in ('cache_references', 'worker_compiler_cache', 'worker_disk_candidates'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/{source}.go'] = ('worker_disk_budget_test.go',)

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
# The host-load benchmark expands the shared candidate-cost planner fixture.
SUPPLEMENTAL_CONSUMERS['internal/myce/host_backfill_cost_test.go'] = (
    'checkin_candidate_cost_test.go',)
# Platform socket helpers implement transfer progress underneath file clients.
for source in ('file_socket_unix', 'file_socket_other'):
    SUPPLEMENTAL_CONSUMERS[f'internal/myce/{source}.go'] = (
        'client_adaptive_test.go', 'file_transfer_test.go')

# The non-Linux Xe probe withholds telemetry; Linux exercises the read-only PMU
# implementation; portable device and admission tests exercise missing evidence.
SUPPLEMENTAL_CONSUMERS['internal/myce/xe_activity_other.go'] = (
    'xe_activity_linux_test.go', 'device_telemetry_test.go', 'discrete_fanout_sharing_test.go')


PYTHON_CLIENT = ('@python-client', 'test_client.DevTests.test_consumer_skill_examples')


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
            # A benchmark-only test file has no tests to run; vet compiles it.
            if name.endswith('_test.go') and not re.search(r'^func Test\w+\(', path.read_text(), re.M):
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
    selection = [(str(directory), '^(' + '|'.join(sorted(tests)) + ')$')
                 for directory, tests in sorted(packages.items())]
    # The consumer skill's runnable examples execute in the Python client suite,
    # which no Go test covers. Quick runs only that contract test (#1224).
    if any(name.startswith(('skills/', 'clients/python/')) for name in changed):
        selection.append(PYTHON_CLIENT)
    return selection


# Always-run canaries (#1224): cheap whole-output checks that textual selection
# missed on 2026-11-11 (planning digest, status rendering, previous-release open).
# About 12 s of race test time. A canary renamed away fails test-gate-related.py.
CANARIES = {
    'internal/myce': ('TestCheckinPlanningOutcomeEquivalence', 'TestStatusReadPlanningEquivalence',
                      'TestStatusCompactRetainedEvidence', 'TestStatusPageRendersSnapshot',
                      'TestQueuePersistenceOpensPreviousReleaseDatabase'),
    'cmd/myce': ('TestCompactStatusIsSmallAndKeepsWhatMatters', 'TestStatusFullIDsAndOneBasedAttempts',
                 'TestStatusShowsProfileQualificationAndEveryWaitingReason'),
}


def with_canaries(selection, canaries=CANARIES):
    """Merge canaries into a selection; keep the standalone smoke for no-Go changes."""
    # Non-Go entries (@name) pass through; they never replace the Go smoke.
    extra = [entry for entry in selection if entry[0].startswith('@')]
    packages = {directory: set(pattern[2:-2].split('|'))
                for directory, pattern in selection if not directory.startswith('@')}
    if not packages:
        # gate.sh runs this smoke for an empty selection; canaries must not hide it.
        packages['cmd/myce'] = {'TestDevStandaloneSmoke'}
    for directory, tests in canaries.items():
        if pathlib.Path(directory).is_dir():
            packages.setdefault(directory, set()).update(tests)
    return [(directory, '^(' + '|'.join(sorted(tests)) + ')$')
            for directory, tests in sorted(packages.items())] + extra


if __name__ == '__main__':
    try:
        for directory, pattern in with_canaries(select(sys.argv[1])):
            print(f'{directory}\t{pattern}')
    except ValueError as error:
        print(f'quick: {error}; run ./scripts/gate.sh full', file=sys.stderr)
        sys.exit(2)
