#!/usr/bin/env python3
"""Real fake-child checks for package admission and typed watchdog results."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


SCHEDULER = Path(__file__).with_name('gate-scheduler.py')
HARNESS = '''
import importlib.util, json, signal, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location('scheduler', sys.argv[1])
scheduler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scheduler)
for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
    signal.signal(sig, scheduler.interrupted)
root = Path(sys.argv[2])
entries = json.loads((root / 'entries').read_text())
def command(entry):
    with (root / 'admitted').open('a') as out:
        out.write(entry[0] + '\\n')
    return entry[0], [sys.executable, str(root / 'child.py'), str(root), *entry]
sys.exit(scheduler.run(entries, 2, command))
'''
CHILD = '''
import json, os, signal, sys, time
from pathlib import Path
root, name, behavior = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
def event(kind):
    fd = os.open(root / 'events', os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.write(fd, (json.dumps([name, kind]) + '\\n').encode())
    os.close(fd)
def stop(sig, frame):
    event('stopped')
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
event('start')
(root / (name + '-started')).touch()
if behavior == 'blocked':
    while not (root / 'release').exists():
        time.sleep(.01)
elif behavior == 'release':
    (root / 'release').touch()
elif behavior == '125':
    while not (root / 'oldest-started').exists():
        time.sleep(.01)
elif behavior == 'killed':
    os.kill(os.getpid(), signal.SIGKILL)
event('finish')
sys.exit(int(behavior) if behavior.isdigit() else 0)
'''


class SchedulerTests(unittest.TestCase):
    def launch(self, root, entries):
        (root / 'child.py').write_text(CHILD)
        (root / 'entries').write_text(json.dumps(entries))
        return subprocess.Popen([sys.executable, '-c', HARNESS, str(SCHEDULER), str(root)],
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)

    def finish(self, child):
        try:
            output, _ = child.communicate(timeout=8)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            output, _ = child.communicate()
            self.fail('scheduler did not finish: ' + output)
        return child.returncode, output

    def test_completed_second_slot_admits_next_before_oldest_exits(self):
        # The old FIFO waits forever: only the third package releases oldest.
        entries = [('oldest', 'blocked'), ('bridge', '0'), ('core', 'release'), ('tail', '0')]
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            status, output = self.finish(self.launch(root, entries))
            self.assertEqual(status, 0, output)
            self.assertEqual((root / 'admitted').read_text().splitlines(), [x[0] for x in entries])
            events = [json.loads(line) for line in (root / 'events').read_text().splitlines()]
            self.assertLess(events.index(['core', 'start']), events.index(['oldest', 'finish']))
            active = set()
            for name, kind in events:
                if kind == 'start':
                    self.assertNotIn(name, active)
                    active.add(name)
                    self.assertLessEqual(len(active), 2)
                else:
                    active.remove(name)
            self.assertFalse(active)

    def test_actual_failure_and_killed_watchdog_cannot_be_masked(self):
        for behavior in ('7', 'killed'):
            with self.subTest(behavior=behavior), tempfile.TemporaryDirectory() as folder:
                status, output = self.finish(self.launch(Path(folder), [('failure', behavior), ('ok', '0')]))
                self.assertEqual(status, 1, output)

    def test_stall_dominates_ordinary_failure_in_final_drain(self):
        with tempfile.TemporaryDirectory() as folder:
            status, output = self.finish(self.launch(Path(folder), [('failure', '7'), ('stall', '124')]))
            self.assertEqual(status, 124, output)

    def test_custody_refusal_stops_before_refill_and_unwinds_sibling(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            status, output = self.finish(self.launch(root, [('oldest', 'blocked'), ('custody', '125'),
                                                           ('forbidden', 'release')]))
            self.assertEqual(status, 125, output)
            self.assertEqual((root / 'admitted').read_text().splitlines(), ['oldest', 'custody'])
            self.assertIn(['oldest', 'stopped'], [json.loads(x) for x in (root / 'events').read_text().splitlines()])

    def test_signal_unwinds_active_watchdog(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            child = self.launch(root, [('oldest', 'blocked')])
            deadline = time.monotonic() + 5
            while not (root / 'events').exists() and time.monotonic() < deadline:
                time.sleep(.01)
            child.send_signal(signal.SIGTERM)
            status, output = self.finish(child)
            self.assertEqual(status, 143, output)
            self.assertIn(['oldest', 'stopped'], [json.loads(x) for x in (root / 'events').read_text().splitlines()])


if __name__ == '__main__':
    unittest.main()
