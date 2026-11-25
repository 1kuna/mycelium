import json
import os
import socket
import struct
import threading
import unittest
from unittest import mock

from mycelium.warm_pieces import WarmPieces


class WorkerSide:
    """Answer main's framed operations with scripted replies."""

    def __init__(self, replies):
        self.channel, main = socket.socketpair()
        self.operations = []
        self.main_fd = main.detach()
        self.thread = threading.Thread(target=self._serve, args=(list(replies),), daemon=True)
        self.thread.start()

    def _serve(self, replies):
        with self.channel:
            for reply in replies:
                header = self.channel.recv(5, socket.MSG_WAITALL)
                if len(header) < 5:
                    return
                self.operations.append(header[4:])
                data = json.dumps(reply).encode()
                self.channel.sendall(struct.pack("!I", len(data)) + data)


class WarmPiecesTests(unittest.TestCase):
    item = {"key": "a", "args": [], "inputs": {}, "output_dir": "/out", "checkpoint_dir": "/save"}

    def open(self, replies):
        worker = WorkerSide(replies)
        with mock.patch.dict(os.environ, {"MYCE_WARM_PIECES_FD": str(worker.main_fd)}):
            pieces = WarmPieces()
        self.addCleanup(pieces.__exit__)
        return worker, pieces

    def test_stop_refusing_settle_ends_work_without_raising(self):
        for settle in ("success", "failure"):
            with self.subTest(settle=settle):
                worker, pieces = self.open([{"item": self.item}, {"end": True}])
                self.assertEqual(pieces.next().key, "a")
                self.assertFalse(getattr(pieces, settle)())
                self.assertIsNone(pieces.next())
                pieces.__exit__()
                worker.thread.join(5)
                self.assertEqual(worker.operations, [b"N", settle[:1].upper().encode()])

    def test_pending_checkpoint_keeps_item_active(self):
        worker, pieces = self.open([{"item": self.item}, {"pending": True}, {}, {}])
        pieces.next()
        self.assertFalse(pieces.checkpoint())
        self.assertTrue(pieces.checkpoint())
        self.assertTrue(pieces.success())
        pieces.__exit__()
        worker.thread.join(5)
        self.assertEqual(worker.operations, [b"N", b"C", b"C", b"S"])


if __name__ == "__main__":
    unittest.main()
