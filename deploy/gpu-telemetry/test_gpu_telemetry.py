"""Laptop checks: python3 -B -m unittest discover -s deploy/gpu-telemetry -v."""

import datetime as dt
import importlib.machinery
import importlib.util
import json
import multiprocessing
from pathlib import Path
import socket
import tempfile
import time
import unittest

loader = importlib.machinery.SourceFileLoader(
    'telemetry', str(Path(__file__).with_name('myce-gpu-telemetry-broker')))
spec = importlib.util.spec_from_loader(loader.name, loader)
telemetry = importlib.util.module_from_spec(spec)
loader.exec_module(telemetry)
CARDS = ['0000:01:00.0', '0000:02:00.0']
RAW = '  use_type: 1\n  use_tt: 1\n  size: 8180507\n  usage: 28672\n'


class TelemetryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for card in CARDS:
            (self.root / card).mkdir()
            (self.root / card / 'gtt_mm').write_text(RAW)

    def connection(self) -> tuple[socket.socket, multiprocessing.Process]:
        client, server = socket.socketpair()
        accepted = time.monotonic()

        def run() -> None:
            client.close()
            with server:
                telemetry.respond(server, accepted, CARDS, self.root)

        process = multiprocessing.get_context('fork').Process(target=run)
        process.start()
        server.close()
        client.settimeout(1)
        self.addCleanup(client.close)
        self.addCleanup(process.join, 1)
        return client, process

    def receive(self, client: socket.socket) -> bytes:
        data = b''
        while True:
            chunk = client.recv(65537)
            if not chunk:
                return data
            data += chunk

    def test_reads_both_cards_only_after_request_eof(self) -> None:
        client, process = self.connection()
        # No request EOF yet; changing a card now must affect the reply.
        client.settimeout(0.015)
        with self.assertRaises(socket.timeout):
            client.recv(1)
        changed = RAW.replace('28672', '57344')
        (self.root / CARDS[1] / 'gtt_mm').write_text(changed)
        started = dt.datetime.now(dt.timezone.utc)
        client.shutdown(socket.SHUT_WR)
        client.settimeout(1)
        data = self.receive(client)
        finished = dt.datetime.now(dt.timezone.utc)
        sample = json.loads(data)
        sampled = dt.datetime.fromisoformat(sample['sampled_at'].replace('Z', '+00:00'))
        self.assertGreaterEqual(sampled, started)
        self.assertLessEqual(sampled, finished)
        self.assertEqual(sample['gtt_mm'], {CARDS[0]: RAW, CARDS[1]: changed})
        self.assertLessEqual(len(data), telemetry.MAX_BYTES)
        process.join(1)
        self.assertEqual(process.exitcode, 0)

    def test_rejects_any_request_byte(self) -> None:
        client, _ = self.connection()
        client.sendall(b'x')
        client.shutdown(socket.SHUT_WR)
        self.assertEqual(self.receive(client), b'')

    def test_request_without_eof_expires(self) -> None:
        client, _ = self.connection()
        begin = time.monotonic()
        self.assertEqual(self.receive(client), b'')
        self.assertLess(time.monotonic() - begin, 0.200)

    def test_blocked_read_shares_connection_deadline(self) -> None:
        # A FIFO emulates a blocking driver read. No reader survives its budget.
        import os
        path = self.root / CARDS[1] / 'gtt_mm'
        path.unlink()
        os.mkfifo(path)
        client, process = self.connection()
        begin = time.monotonic()
        client.shutdown(socket.SHUT_WR)
        self.assertEqual(self.receive(client), b'')
        process.join(1)
        self.assertEqual(process.exitcode, 0)
        self.assertLess(time.monotonic() - begin, 0.200)

    def test_missing_malformed_and_oversized_reply_are_empty(self) -> None:
        for text in ('use_type: 1\nuse_tt: 1\n', RAW.replace('28672', '-1'),
                     RAW.replace('use_tt: 1', 'use_tt: 0'),
                     RAW + ('x' * telemetry.MAX_BYTES)):
            with self.subTest(text=text[:60]):
                (self.root / CARDS[1] / 'gtt_mm').write_text(text)
                client, _ = self.connection()
                client.shutdown(socket.SHUT_WR)
                self.assertEqual(self.receive(client), b'')
        (self.root / CARDS[1] / 'gtt_mm').unlink()
        client, _ = self.connection()
        client.shutdown(socket.SHUT_WR)
        self.assertEqual(self.receive(client), b'')

    def test_total_json_limit_applies_across_cards(self) -> None:
        for card in CARDS:
            (self.root / card / 'gtt_mm').write_text(RAW + 'padding: ' + 'x' * 33000)
        client, _ = self.connection()
        client.shutdown(socket.SHUT_WR)
        self.assertEqual(self.receive(client), b'')

    def test_inventory_must_be_unique_and_canonical(self) -> None:
        telemetry.validate_cards(CARDS)
        for cards in ([], [CARDS[0], CARDS[0]], ['01:00.0'], ['0000:AB:00.0'],
                      ['../../bad'], ['0000:01:ff.0']):
            with self.subTest(cards=cards), self.assertRaises(ValueError):
                telemetry.validate_cards(cards)


if __name__ == '__main__':
    unittest.main()
