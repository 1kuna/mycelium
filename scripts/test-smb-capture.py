#!/usr/bin/env python3
"""Laptop regressions for the optional SMB capture, with no SMB dependency."""
import errno
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

from smb_capture import Capture, capture_command


class CaptureTests(unittest.TestCase):
    def test_read_and_replace_keep_original_errors_and_sanitize_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = Capture(Path(temporary) / "capture", "/example-mount")
            with patch("smb_capture.capture_command", return_value={"status": "ok", "returncode": 0}) as run:
                capture.baseline()
                for operation, path_class, number in [("read", "nas_input", errno.EAGAIN), ("replace", "checkpoint", errno.EBUSY)]:
                    failure = OSError(number, "private message", "/private/example-file")

                    def fail():
                        raise failure

                    with self.assertRaises(OSError) as caught:
                        capture.observe(operation, path_class, fail)
                    self.assertIs(caught.exception, failure)
                # Two baseline commands plus ONE incident snapshot, never a
                # repeated SMB query per failure in a failing consumer loop.
                self.assertEqual(run.call_count, 5)
            data = (capture.output / "events.jsonl").read_text()
            self.assertNotIn("example-file", data)
            self.assertNotIn("private message", data)
            self.assertNotIn("example-mount", data)
            events = [json.loads(line) for line in data.splitlines()]
            self.assertEqual([e["errno"] for e in events], [errno.EAGAIN, errno.EBUSY])
            self.assertTrue(all(e["elapsed_seconds"] >= 0 for e in events))
            self.assertTrue(all(e["started_at"].endswith("+00:00") for e in events))
            self.assertEqual(stat.S_IMODE(capture.output.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE((capture.output / "events.jsonl").stat().st_mode), 0o600)

    def test_partial_read_is_recorded_without_payload_or_repair(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = Capture(Path(temporary) / "capture", "/example-mount")
            result = b"private payload"
            with patch("smb_capture.capture_command", return_value={"status": "ok", "returncode": 0}):
                self.assertIs(capture.observe("read", "nas_input", lambda: result), result)
                self.assertFalse((capture.output / "events.jsonl").exists())  # Normal read/EOF.
                self.assertIs(capture.observe("read", "nas_input", lambda: result, expected_bytes=40), result)
            event = json.loads((capture.output / "events.jsonl").read_text())
            self.assertEqual((event["requested_bytes"], event["returned_bytes"], event["errno"]), (40, len(result), None))
            self.assertNotIn("private payload", json.dumps(event))

    def test_typed_memoryview_counts_bytes_not_elements(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = Capture(Path(temporary) / "capture", "/example-mount")
            value = memoryview(b"1234").cast("I")
            self.assertIs(capture.observe("read", "nas_input", lambda: value, expected_bytes=4), value)
            self.assertFalse((capture.output / "events.jsonl").exists())
            with patch("smb_capture.capture_command", return_value={"status": "ok", "returncode": 0}):
                capture.observe("read", "nas_input", lambda: value, expected_bytes=8)
            self.assertEqual(json.loads((capture.output / "events.jsonl").read_text())["returned_bytes"], 4)

    def test_refusal_stops_collection_and_capture_failure_keeps_payload_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = Capture(Path(temporary) / "capture", "/example-mount")
            with patch("smb_capture.capture_command", return_value={"status": "ok", "returncode": 1}) as run:
                capture.baseline()
                failure = OSError(errno.EBUSY, "busy")
                with self.assertRaises(OSError) as caught:
                    capture.observe("replace", "checkpoint", lambda: (_ for _ in ()).throw(failure))
                self.assertIs(caught.exception, failure)
                self.assertEqual(run.call_count, 1)
                self.assertEqual(capture.last_error, "command_refused")
            with patch.object(capture, "incident", side_effect=OSError(errno.ENOSPC, "capture unavailable")):
                with self.assertRaises(OSError) as caught:
                    capture.observe("read", "nas_input", lambda: (_ for _ in ()).throw(failure))
                self.assertIs(caught.exception, failure)
                self.assertEqual(capture.last_error, "OSError")

    def test_command_timeout_stops_its_process_and_never_inherits_fleet_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "command.txt"
            result = capture_command([sys.executable, "-c", "import time; time.sleep(60)"], output, 0.05)
            self.assertEqual(result["status"], "timeout")
            self.assertEqual(result["returncode"], -9)
            # Inspect variable NAMES only, never a fleet credential value.
            output = Path(temporary) / "environment.txt"
            result = capture_command([sys.executable, "-c", "import os; print('MYCE_TOKEN' in os.environ)"], output, 5)
            self.assertEqual(result["returncode"], 0)
            self.assertEqual(output.read_text().strip(), "False")
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)

    def test_output_write_failure_stops_live_command_and_retains_pid(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch("smb_capture.os.write", side_effect=OSError(errno.ENOSPC, "full")):
                result = capture_command([
                    sys.executable, "-c", "import time; print('ready', flush=True); time.sleep(60)"
                ], Path(temporary) / "command.txt", 5)
            self.assertEqual(result["status"], "capture_io_error")
            self.assertEqual(result["errno"], errno.ENOSPC)
            self.assertEqual(result["returncode"], -9)
            with self.assertRaises(ProcessLookupError):
                os.killpg(result["pid"], 0)

    def test_unresolved_custody_survives_a_failed_status_write(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = Capture(Path(temporary) / "capture", "/example-mount")
            with patch("smb_capture.capture_command", return_value={"status": "cleanup_unconfirmed", "pid": 123}):
                with patch.object(capture, "_write", side_effect=OSError(errno.ENOSPC, "full")):
                    with self.assertRaises(OSError):
                        capture.baseline()
            self.assertEqual(capture.unresolved_pids, [123])
            self.assertEqual(capture.last_error, "cleanup_unconfirmed")
            with patch("smb_capture.capture_command") as run:
                capture.baseline()
                self.assertEqual(run.call_count, 0)


if __name__ == "__main__":
    unittest.main()
