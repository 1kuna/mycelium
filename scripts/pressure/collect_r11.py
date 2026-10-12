from kernel_reads import kernel_read
"""Detached receipt collection and fail-closed final inventory; no AI polling."""
from pathlib import Path
import datetime
import json
import subprocess
import time
from remote_snippets import python_command, checked_python, checked_shell
HERE = Path('/Users/user/Documents/Git/Verso/.private/missions/mycelium-stage-ab-20261011')
ROOT = '/home/user/synthetic-run'
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
    lines = ['# Example operational report', '', 'Operational details are retained in private maintainer evidence.']
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
