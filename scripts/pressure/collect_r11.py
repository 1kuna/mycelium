from kernel_reads import kernel_read
"""Detached receipt collection and fail-closed final inventory; no AI polling."""
from pathlib import Path
import datetime
import json
import subprocess
import time
from remote_snippets import python_command, checked_python, checked_shell
HERE = Path('/Users/zach/Documents/Git/Verso/.private/missions/mycelium-stage-ab-20261011')
ROOT = '/home/kuna/myce-test-stage-ab-r12-20261012'
NAMES = ['launch-manifest.json', 'launch-receipt.json', 'collector-soak.json', 'results.json', 'cleanup.json', 'final-cleanup.json', 'spark-cleanup.json', 'DONE', 'FAILED']
SCENARIOS = ['C-migrate-to-Spark', 'C-Spark-full-return-B70', *['Docker-' + m for m in ('grace', 'force', 'restart', 'loss', 'late', 'slow', 'disable')], 'Memory-ceiling-isolation', 'CPU-fast-matrix', 'B-installed-disable-enable']

def inventory_source():
    return kernel_read(Path(__file__).with_name('kernel_reads.py'))+'\n'+kernel_read(Path(__file__).with_name('inventory_r11.py')).replace('from kernel_reads import kernel_read\n','')


def receipt_source():
    return kernel_read(Path(__file__).with_name('kernel_reads.py'))+'\n'+'from pathlib import Path; import json; root=Path('+repr(ROOT)+'); names='+repr(NAMES)+'; print(json.dumps({n:json.loads(kernel_read(root/n)) for n in names if (root/n).exists()}))'


def collect():
    p = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', 'b70', python_command(receipt_source())], capture_output=True, text=True, timeout=20)
    assert p.returncode == 0, 'receipt transport failed'
    values = json.loads(p.stdout)
    folder = HERE / 'native-r11-receipts'
    folder.mkdir(exist_ok=True)
    for (name, value) in values.items():
        (folder / name).write_text(json.dumps(value, indent=2))
    return values

def final_inventory():
    source = checked_python(inventory_source())
    summary = {}
    for host in ('b70', 'spark'):
        p = subprocess.run(['ssh', host, checked_shell(('sudo -n ' if host == 'b70' else '') + 'python3 - ' + host)], input=source, capture_output=True, text=True, timeout=45)
        assert p.returncode == 0, 'inventory unavailable'
        data = json.loads(p.stdout)
        (HERE / ('r11-final-' + host + '-inventory.json')).write_text(json.dumps(data, indent=2))
        assert not any((data[k] for k in ('test_pids', 'scan_errors', 'test_cgroups', 'disabled_flags', 'test_slices')))
        assert not any((c['running'] for c in data['test_containers']))
        summary[host] = dict(live_test_pids=0, live_test_cgroups=0, running_test_containers=0, disabled_flags=0, retained_stopped_containers=len(data['test_containers']))
        if host == 'b70':
            assert json.loads(data['health']['stdout']) == {'status': 'ok', 'worker_connected': True}
            assert not data['pause_marker_exists']
            assert len(data['protected_containers']) == 4 and all((c['running'] and c['health'] in (None, 'healthy') for c in data['protected_containers']))
    (HERE / 'r11-final-inventory-summary.json').write_text(json.dumps(summary, indent=2))
    return summary

def render(values, error=None):
    status = 'FAILED' if 'FAILED' in values else 'DONE' if 'DONE' in values else 'RUNNING — outcomes pending' if 'launch-receipt.json' in values else 'PREPARED'
    manifest = values.get('launch-manifest.json', {})
    rows = values.get('results.json', {}).get('results', [])
    final = values.get('final-cleanup.json')
    spark = values.get('spark-cleanup.json')
    lines = ['# Mycelium Stage A/B round 11 — 2026-10-11', '', status + '. Native acceptance and final cleanup remain pending until verified receipts.', '', 'Issue #238. Mycelium checkout is read-only. Core source is unchanged from current origin/main ' + manifest.get('main', '968c15d5d3e39060e997b1b3ba7f6450db881269') + '. Queue and both workers are built from that one pin. Harness ' + manifest.get('harness', 'pending') + ' is on feat/verso-pressure-harness-r11; git ls-remote verifies its pushed SHA. Source and binary hashes are in launch-manifest.json.', '', '## Repair and verification', '', 'The active Spark memory-poll parenthesis is fixed. Every SSH Python payload passes local py_compile; every generated remote shell command passes bash -n. Tests generate start/full/poll/read/respond/stop/collect/final-cleanup, preparation, launch, receipt, and qualification snippets. A failing-first reproduction is retained in r11-failing-first.log.', '', 'After a phase failure the transport preserves the failure, confirms exact unit/cgroup and owned tunnel cleanup, and remains available for fresh request-specific cleanup responses. A cleanup error retains custody, refuses later starts and prevents runner continuation. Real transport/runner regressions cover confirmed and unconfirmed cleanup. A phase failure stays FAIL.', '', 'Mac tests and B70 tests are recorded in r11-local-tests.log and remote harness-tests.log. Two Mac tests require Linux. Focused Go checks, native device visibility and fresh cross-host durability have separate receipts. The full Go suite and native scenario acceptance are not inferred from these checks. No model calls occur.', '', 'The initial r11 collector was stopped before any scenario to fix cleanup after a start failed before clock mapping. Its FAILED marker and both-host cleanup receipts are retained in native-r11-initial-abort-receipts. A second collector-only start was stopped before scenarios to make final cleanup reuse the tested missing-unit handler; its receipts are in native-r11-retry-b-abort-receipts. Both aborted starts were independently cleaned. This run uses the fresh r12 root.', '', '## Run contract', '', 'The completed spelling loan is inactive with PID zero and empty cgroup. Its 14 GiB test reservation is removed; Atlas retains 24 GiB. Spark active inventory is capped at 26 GiB before that reserve (2 GiB available); full inventory is capped at 24 GiB (zero available), with no memory-filling payload. The loan and all atlas units are read-only.', '', 'Fresh local databases, spools and host-control state; per-phase shared checkpoints. Order: migration to Spark; visibly queued Spark-full return to B70 after at least 180 idle seconds; real CPU Docker grace/force/restart/loss/late/slow/disable; isolated native memory-ceiling OOM; isolated local-ext4 CPU matrix; installed GPU1 targeted disable/enable. A fresh 30-minute zero-hold collector precedes the sequence.', '', 'Docker uses the already-local pinned image, exact myce.attempt ownership, MYCE_MEMORY_LIMIT_BYTES for --memory and no-swap --memory-swap, no GPU mappings and no network. Slowness/delay is only in our wrapper. Unknown or unlabeled containers are never signaled; stopped owned objects remain recoverable evidence.', '', 'Memory-ceiling scenario: a delegated CPU/fake worker has a 32 MiB native attempt ceiling. A touched 128 MiB allocation must be kernel OOM-killed once, fail with the exact ceiling reason and zero infrastructure losses, while a concurrent separate job succeeds without restart. Local checkpoint root; no GPU device access.', '', 'B70 Mycelium GPU workers see physical GPU1 PCI 0000:07:00.0 only, logical selector 0 inside its private namespace. GPU0 is never exposed. Installed drill invokes install worker --instance gpu1 --device 0 inside that qualified environment; the installer detect check must pass before enable/start. It then exercises disable/enable --unit myce-worker@gpu1.service. Enforced attempt ceilings are enabled by Delegate=memory on the installed unit.', '', '564 remains disabled; the legacy queue is held during this attempt and its exact pause is released only after independent final resource proof. Production containers, GPU controller, display service and Atlas are protected. No reboot or Mac office application. B70 aggregate test cap 8 GiB, no swap, 18 GiB minimum host headroom; device envelope 4 GiB.', '', '## Detached paths', '', ROOT + '/DONE or ' + ROOT + '/FAILED (B70).', ROOT + '/results.json; final-cleanup.json; spark-cleanup.json; phases/.', 'Spark test root: ' + ROOT + '; its final contents move to recoverable Trash after unit/tunnel cleanup.', 'NAS: Verso/experiments/exp-238-mycelium-stage-ab-r12-20261012/.', 'Mac: spark-controller-r11.log/.pid, report-sync-r11.log/.pid, report-sync-r11-DONE or report-sync-r11-FAILED.', '', '## Native outcomes', '']
    soak = values.get('collector-soak.json')
    lines.append('Collector: ' + json.dumps({k: soak.get(k) for k in ('result', 'seconds', 'holds', 'failures')}) if soak else 'Collector gate pending.')
    for name in SCENARIOS:
        matched = [r for r in rows if r['scenario'] == name]
        row = matched[-1] if matched else None
        lines.append(name + ': ' + (row['result'] if row else 'NOT RUN' if 'FAILED' in values else 'pending') + '.')
    if final:
        lines += ['', 'B70 final: ' + json.dumps({k: final.get(k) for k in ('physical', 'queue_pause', 'remaining_pids', 'occupied_test_ports', 'docker_error', 'cpu_cleanup_error', 'runtime_unit_error', 'installed_worker')})]
    if spark:
        lines += ['', 'Spark final: ' + json.dumps({k: spark.get(k) for k in ('error', 'phase_errors', 'loan_identity_unchanged', 'tunnels_stopped', 'cleanup')})]
    summary = HERE / 'r11-final-inventory-summary.json'
    if summary.exists():
        lines += ['', 'Independent final inventory: ' + kernel_read(summary, 'text')]
    if error:
        lines += ['', 'Collector error: ' + error + '. No completion inferred.']
    lines += ['', '## DIGEST', 'The round 10 failure was our malformed Spark poll and dead cleanup transport.', 'The snippet is fixed; all generated remote Python and shell variants are syntax checked locally.', 'The transport survives phase failures and only acknowledges proven, request-specific cleanup.', 'Mycelium queue and workers share current main 968c15d; core source is unchanged.', 'The harness is pushed and its remote SHA is recorded in the launch manifest.', 'The completed loan reservation is dropped; Atlas retains 24 GiB.', 'GPU1 alone is exposed; GPU0, production, Atlas, 564 and protected services are guarded.', 'Migration, full-capacity wait, Docker, OOM isolation, CPU and installed-worker drills run in order.', 'Native state: ' + status + '. Cleanup ' + ('receipts collected; independent inventory required' if final and spark else 'pending') + '.', 'No reboots; launch and local tests are not native acceptance.']
    assert len(lines[lines.index('## DIGEST') + 1:]) <= 10
    (HERE / 'report-r11.md').write_text('\n'.join(lines) + '\n')
if __name__ == '__main__':
    deadline = time.monotonic() + 4 * 3600
    last = {}
    errors = 0
    while time.monotonic() < deadline:
        try:
            last = collect()
            if 'final-cleanup.json' in last and 'spark-cleanup.json' in last:
                final_inventory()
                render(last)
                marker = 'report-sync-r11-FAILED' if 'FAILED' in last else 'report-sync-r11-DONE'
                (HERE / marker).write_text(json.dumps(dict(native_terminal='FAILED' if 'FAILED' in last else 'DONE', when=datetime.datetime.now(datetime.timezone.utc).isoformat())))
                break
            render(last)
            errors = 0
        except Exception as exc:
            errors += 1
            render(last, type(exc).__name__)
            if errors >= 6:
                (HERE / 'report-sync-r11-FAILED').write_text(json.dumps(dict(reason='receipt/final inventory failure', native_terminal='unknown')))
                break
        time.sleep(10)
    else:
        render(last, 'receipt deadline')
        (HERE / 'report-sync-r11-FAILED').write_text(json.dumps(dict(reason='receipt deadline', native_terminal='unknown')))
