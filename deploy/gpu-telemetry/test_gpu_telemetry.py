"""Laptop checks: python3 -B -m unittest discover -s deploy/gpu-telemetry -v."""

import configparser
import ctypes
import datetime as dt
import importlib.machinery
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

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

    def test_vram_query_is_read_only_and_excludes_host_storage(self) -> None:
        # UAPI fixture: system region plus one VRAM region. Only the latter
        # supplies device headroom; no process/vendor tool is involved.
        raw = struct.pack('=II', 2, 0)
        for kind, instance, size, used in ((0, 0, 64 << 30, 10 << 30),
                                           (1, 1, 8 << 30, 2 << 30)):
            raw += struct.pack('=HHIQQQQ6Q', kind, instance, 4096,
                               size, used, 0, 0, *([0] * 6))
        calls = []

        def ioctl(fd, request, query, mutate):
            calls.append((fd, request))
            self.assertEqual(struct.unpack_from('=I', query, 8)[0], 1)
            address = struct.unpack_from('=Q', query, 16)[0]
            if address:
                ctypes.memmove(address, raw, len(raw))
            struct.pack_into('=I', query, 12, len(raw))

        with mock.patch.object(Path, 'read_text', return_value='CapEff:\t0000004000000000\n'), \
                mock.patch.object(Path, 'glob', return_value=[Path('renderD128')]), \
                mock.patch.object(os, 'open', return_value=10) as opened, \
                mock.patch.object(os, 'close') as closed, \
                mock.patch.object(telemetry.fcntl, 'ioctl', side_effect=ioctl):
            self.assertEqual(telemetry.collect_vram(CARDS[0]),
                             {'total_bytes': 8 << 30, 'used_bytes': 2 << 30})
            opened.assert_called_once_with(Path('/dev/dri/renderD128'), os.O_RDONLY | os.O_CLOEXEC)
            closed.assert_called_once_with(10)
        self.assertEqual(calls, [(10, 0xc0286440)] * 2)
        with self.assertRaises(ValueError):
            telemetry.vram_regions(raw[:-1])

    def test_censored_usage_never_becomes_free_vram(self) -> None:
        with mock.patch.object(Path, 'read_text', return_value='CapEff:\t0000000000000000\n'), \
                mock.patch.object(os, 'open') as opened:
            with self.assertRaises(ValueError):
                telemetry.collect_vram(CARDS[0])
            opened.assert_not_called()

    def test_vram_is_optional_and_preserves_gtt_contract(self) -> None:
        with mock.patch.object(telemetry, 'collect_vram', return_value={'total_bytes': 8192, 'used_bytes': 2048}):
            sample = json.loads(telemetry.collect(CARDS, self.root))
        self.assertEqual(sample['vram'][CARDS[1]], {'total_bytes': 8192, 'used_bytes': 2048})
        self.assertEqual(sample['gtt_mm'], {card: RAW for card in CARDS})
        with mock.patch.object(telemetry, 'collect_vram', side_effect=ValueError('unsupported')):
            sample = json.loads(telemetry.collect(CARDS, self.root))
        self.assertEqual(sample['vram'], {})
        self.assertEqual(sample['gtt_mm'], {card: RAW for card in CARDS})

    def test_wrapper_clears_environment_and_ignores_caller_arguments(self) -> None:
        # Substitute only the installed binary location; execute the real shell
        # wrapper against a harmless client that reports its environment/argv.
        client = self.root / 'myce'
        client.write_text(f'#!{sys.executable}\n'
                          'import json, os, sys\n'
                          'print(json.dumps([sys.argv[1:], dict(os.environ)]))\n')
        client.chmod(0o755)
        wrapper = Path(__file__).with_name('myce-gpu-telemetry-helper').read_text()
        wrapper = wrapper.replace('/usr/local/libexec/mycelium-telemetry/myce', str(client))
        result = subprocess.run(['/bin/sh', '-c', wrapper, 'helper', '--socket', '/untrusted'],
                                env={'PATH': '/untrusted', 'MYCE_TOKEN': 'test-only'},
                                check=True, capture_output=True, text=True)
        args, env = json.loads(result.stdout)
        self.assertEqual(args, ['gpu-telemetry-client'])
        # Interpreter/platform startup may add locale/encoding metadata.
        runtime_keys = {'LC_CTYPE'}
        if sys.platform == 'darwin':
            runtime_keys.add('__CF_USER_TEXT_ENCODING')
        self.assertLessEqual(set(env), {'PATH', 'LANG', 'LC_ALL'} | runtime_keys)
        self.assertEqual(env['PATH'], '/usr/bin:/bin')
        self.assertEqual(env['LANG'], 'C')
        self.assertEqual(env['LC_ALL'], 'C')




class ClientGroupTemplateTest(unittest.TestCase):
    def test_rendered_client_group_permissions(self):
        root = Path(__file__).parent
        # Run the documented sed rendering for both a primary-group style name
        # and an already-held dedicated supplementary group.
        for group in ('worker-primary', 'myce-telemetry'):
            with self.subTest(group=group):
                def render(name):
                    return subprocess.check_output(
                        ['sed', f's/REPLACE_CLIENT_GROUP/{group}/g', str(root / name)],
                        text=True)
                unit = configparser.ConfigParser()
                unit.read_string(render('myce-gpu-telemetry.socket'))
                socket_unit = unit['Socket']
                self.assertEqual(socket_unit['SocketGroup'], group)
                self.assertEqual(socket_unit['SocketUser'], 'root')
                self.assertEqual(socket_unit['SocketMode'], '0660')
                self.assertEqual(socket_unit['DirectoryMode'], '0750')
                rule = next(line.split() for line in render('myce-gpu-telemetry.conf').splitlines()
                            if line and not line.startswith('#'))
                self.assertEqual(rule, ['d', '/run/mycelium', '0750', 'root', group, '-', '-'])
                self.assertEqual(socket_unit['ListenStream'], rule[1] + '/gpu-telemetry.sock')


if __name__ == '__main__':
    unittest.main()
