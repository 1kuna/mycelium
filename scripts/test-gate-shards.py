#!/usr/bin/env python3
"""Coverage and balance contracts for full-gate process partitions."""
import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('shards', Path(__file__).with_name('gate-shards.py'))
shards = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shards)
spec = importlib.util.spec_from_file_location('scheduler', Path(__file__).with_name('gate-scheduler.py'))
scheduler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scheduler)


class ShardTests(unittest.TestCase):
    def test_balances_measured_work_and_retains_new_and_zero_duration_roots(self):
        names = ['TestHeavy', 'TestMedium', 'TestSmall', 'TestNew', 'FuzzSeeds', 'Example', 'TestZero']
        durations = {'TestHeavy': 10, 'TestMedium': 6, 'TestSmall': 4, 'TestZero': 0}
        groups = shards.partition(names, durations, 2)
        self.assertCountEqual([name for group in groups for name in group], names)
        self.assertEqual(groups, shards.partition(list(reversed(names)), durations, 2))
        self.assertLessEqual(max(sum(durations.get(n, .1) for n in group) for group in groups), 10.3)
        self.assertEqual(shards.partition([], {}, 8), [])
        self.assertEqual(shards.partition(['TestOnly'], {}, 8), [['TestOnly']])

    def test_invalid_inventory_or_durations_cannot_silently_drop_coverage(self):
        for names, durations, count in [(['TestA', 'TestA'], {}, 2), (['TestA'], {}, 0),
                                         (['TestA'], {'TestA': float('nan')}, 2)]:
            with self.subTest(names=names, count=count), self.assertRaises(ValueError):
                shards.partition(names, durations, count)

    def test_full_plan_has_exactly_once_root_and_descendant_selection(self):
        names = ['TestA', 'TestAB', 'TestNew', 'FuzzSeed', 'Example']
        with patch.object(shards, 'native_roots', return_value=names):
            entries = shards.plan(['mycelium/cmd/myce\t.*', 'mycelium/internal/myce\t.*'], 18)
        import re
        for package in ('mycelium/cmd/myce', 'mycelium/internal/myce'):
            expressions = [line.split('\t')[1] for line in entries if line.split('\t')[0] == package]
            for name in names:
                self.assertEqual(sum(bool(re.fullmatch(expression, name)) for expression in expressions), 1)
            self.assertFalse(any(re.fullmatch(expression, 'TestAbsent') for expression in expressions))
            # Go applies -run component-wise: no slash in root expression constrains children.
            self.assertTrue(all('/' not in expression for expression in expressions))

    def test_small_capacity_and_quick_expressions_remain_unsharded(self):
        entries = ['mycelium/cmd/myce\t.*', 'mycelium/internal/myce\t.*']
        with patch.object(shards, 'native_roots', side_effect=AssertionError('no discovery expected')):
            self.assertEqual(shards.plan(entries, 1), entries)
            exact = ['mycelium/internal/myce\t^TestExact$/case', 'unmeasured/package\t.*']
            self.assertEqual(shards.plan(exact, 4), exact)

    def test_chunks_refill_across_packages_in_longest_first_order(self):
        import re
        hints = {'core': {f'TestCore{i}': 10 for i in range(60)},
                 'cli': {'TestCLI1': 120, 'TestCLI2': 80, 'TestCLI3': 70, 'TestCLI4': 10}}
        with patch.object(shards.Path, 'read_text', return_value=json.dumps({'packages': hints})), \
             patch.object(shards, 'native_roots', side_effect=lambda package: list(hints[package])):
            entries = shards.plan(['core\t.*', 'cli\t.*'], 4)
        loads = []
        for entry in entries:
            package, expression = entry.split('\t')
            loads.append(sum(weight for name, weight in hints[package].items() if re.fullmatch(expression, name)))
        self.assertEqual(loads, sorted(loads, reverse=True))
        self.assertLessEqual(max(loads), 120.1)  # The largest root is the lower bound.
        self.assertGreater(len(entries), 4)
        # CLI work is admitted before the last core chunk, even though core
        # came first in the package list. Each gets refills, not one long slot.
        packages = [entry.split('\t')[0] for entry in entries]
        self.assertLess(packages.index('cli'), max(i for i, package in enumerate(packages) if package == 'core'))

    def test_measured_tail_is_split_without_splitting_queue_history_root(self):
        import re
        from collections import Counter
        # Fresh history timing dominates one old, coarsely packed chunk. Keep
        # that root whole while providing enough smaller jobs to refill slots.
        names = ['TestQueueHistoryBurst'] + [f'TestShort{i}' for i in range(120)]
        hints = dict(packages={'core': {name: 1 for name in names}},
                     platform_packages={'darwin': {'core': {
                         name: 84 if name == 'TestQueueHistoryBurst' else 10 for name in names}}})
        with patch.object(shards.Path, 'read_text', return_value=json.dumps(hints)), \
             patch.object(shards, 'native_roots', return_value=names), \
             patch.object(shards.sys, 'platform', 'darwin'):
            entries = shards.plan(['core\t.*'], 4)
        selected = Counter()
        loads = []
        for entry in entries:
            expression = entry.split('\t')[1]
            chosen = [name for name in names if re.fullmatch(expression, name)]
            selected.update(chosen)
            loads.append(sum(hints['platform_packages']['darwin']['core'][name] for name in chosen))
            if 'TestQueueHistoryBurst' in chosen:
                self.assertEqual(chosen, ['TestQueueHistoryBurst'])
        self.assertEqual(selected, Counter(names))
        self.assertGreater(len(entries), 3 * 4)
        self.assertLessEqual(max(loads), 90)
        self.assertEqual(loads, sorted(loads, reverse=True))

    def test_native_inventory_includes_examples_and_seeds_but_no_benchmarks(self):
        package = 'mycelium/internal/myce'
        events = [dict(Package=package, Action='output', Output=name + '\n')
                  for name in ['TestOne', 'Example', 'FuzzSeed', 'BenchmarkNotRun']]
        data = ('\n'.join(json.dumps(e) for e in events) + '\n').encode()
        child = Mock(returncode=0)
        child.communicate.return_value = (data, None)
        with patch.object(shards.subprocess, 'Popen', return_value=child) as run:
            self.assertEqual(shards.native_roots(package), ['TestOne', 'Example', 'FuzzSeed'])
        command = run.call_args.args[0]
        self.assertIn('-race', command)
        self.assertEqual(command[2], 'inventory:' + package)
        self.assertEqual(command[-1], package)

    def test_inventory_custody_failure_stops_plan(self):
        child = Mock(returncode=125)
        child.communicate.return_value = (b'', None)
        with patch.object(shards.subprocess, 'Popen', return_value=child):
            with self.assertRaises(SystemExit) as stopped:
                shards.plan(['mycelium/internal/myce\t.*'], 18)
        self.assertEqual(stopped.exception.code, 125)

    def test_inventory_interrupt_waits_for_watchdog_custody(self):
        child = Mock(returncode=125)
        child.communicate.side_effect = [SystemExit(143), (b'', None)]
        with patch.object(shards.subprocess, 'Popen', return_value=child):
            with self.assertRaises(SystemExit) as stopped:
                shards.native_roots('mycelium/internal/myce')
        self.assertEqual(stopped.exception.code, 125)
        child.terminate.assert_called_once()
        self.assertEqual(child.communicate.call_count, 2)

    def test_full_nested_builds_inherit_shard_budget_and_quick_keeps_allocation(self):
        from tempfile import TemporaryDirectory
        import sys
        with TemporaryDirectory() as directory:
            selection = Path(directory) / 'selection'
            selection.write_text('mycelium/internal/view\t.*\n')
            with patch.dict(os.environ, GOFLAGS='-p=18', GOMAXPROCS='18'):
                with patch.object(sys, 'argv', ['scheduler', 'full', '18', str(selection)]), \
                     patch.object(scheduler, 'run', return_value=0) as run:
                    self.assertEqual(scheduler.main(), 0)
                entries, slots, command, environment = run.call_args.args
                self.assertEqual(slots, 9)
                self.assertEqual(environment['GOFLAGS'], '-p=1')
                self.assertEqual(environment['GOMAXPROCS'], '1')
                self.assertIn('-parallel', command(entries[0])[1])
                with patch.object(sys, 'argv', ['scheduler', 'quick', '18', str(selection)]), \
                     patch.object(scheduler, 'run', return_value=0) as run:
                    self.assertEqual(scheduler.main(), 0)
                self.assertEqual(run.call_args.args[1], 18)
                self.assertIsNone(run.call_args.args[3])
                self.assertEqual(os.environ['GOFLAGS'], '-p=18')


if __name__ == '__main__':
    unittest.main()
