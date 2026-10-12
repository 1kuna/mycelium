from kernel_reads import kernel_read
import datetime, json, os, pathlib, re, subprocess, sys
host = sys.argv[1]
home = pathlib.Path('/home/kuna')
root = home / 'myce-test-stage-ab-r11c-20261011'

def run(args):
    p = subprocess.run(args, capture_output=True, text=True, timeout=25)
    return {'rc': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr}
out = {'host': host, 'when': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'test_pids': [], 'scan_errors': [], 'test_cgroups': []}
for p in kernel_read(pathlib.Path('/proc'), 'list'):
    if not p.name.isdecimal() or int(p.name) == os.getpid():
        continue
    try:
        args = kernel_read(p.joinpath('cmdline'), 'bytes').split(b'\x00')
        cg = kernel_read(p.joinpath('cgroup'), 'text')
        match = any((b'myce-test' in a or b'cpu-matrix.test' in a for a in args)) or 'myce-test' in cg or 'myce-worker' in cg
        if match:
            out['test_pids'].append({'pid': int(p.name), 'cgroup': cg.strip(), 'rss_kib': next((l.split()[1] for l in kernel_read(p.joinpath('status'), 'text').splitlines() if l.startswith('VmRSS:')), None)})
    except (FileNotFoundError, ProcessLookupError):
        pass
    except PermissionError:
        out['scan_errors'].append({'pid': int(p.name), 'error': 'PermissionError'})
for p in pathlib.Path('/sys/fs/cgroup').rglob('*'):
    if p.is_dir() and ('myce-test' in str(p) or 'myce-worker' in str(p)):
        out['test_cgroups'].append({'path': str(p), 'pids': kernel_read(p.joinpath('cgroup.procs'), 'text').split(), 'memory_current': kernel_read(p.joinpath('memory.current'), 'text').strip()})
for mode in ['system', 'user']:
    prefix = (['runuser', '-u', 'kuna', '--', 'env', 'XDG_RUNTIME_DIR=/run/user/1000', 'systemctl', '--user'] if os.getuid() == 0 else ['systemctl', '--user']) if mode == 'user' else ['systemctl']
    for kind in ['list-units', 'list-unit-files']:
        out[mode + '_' + kind] = run(prefix + [kind, '--all', '--plain', '--no-pager', 'myce*'])
containers = run(['docker', 'ps', '-aq'])
assert containers['rc'] == 0
ids = containers['stdout'].split()
items = json.loads(run(['docker', 'inspect', *ids])['stdout']) if ids else []
out['test_containers'] = []
for d in items:
    labels = d['Config'].get('Labels') or {}
    name = d['Name']
    if 'myce' in name.lower() or any(('myce' in k.lower() or 'myce-test' in str(v).lower() for (k, v) in labels.items())):
        out['test_containers'].append({'id': d['Id'], 'name': name, 'running': d['State']['Running'], 'pid': d['State']['Pid'], 'label_keys': sorted(labels)})
protected = ['verso-api', 'verso-worker', 'verso-frontend', 'docker-render-1']
out['protected_containers'] = [{'name': d['Name'].lstrip('/'), 'running': d['State']['Running'], 'health': d['State'].get('Health', {}).get('Status')} for d in items if d['Name'].lstrip('/') in protected]
bases = list(home.glob('myce-test*')) + [home / '.config/myce', home / '.local/share/myce', home / '.local/share/mycelium', home / '.myce']
out['disabled_flags'] = [str(p) for b in bases if b.exists() for p in b.rglob('DISABLED')]
ports = [int(x) for x in sys.argv[2].split(',')] if len(sys.argv) > 2 else []
ss = run(['ss', '-H', '-lntu'])
assert ss['rc'] == 0
occupied = set()
for line in ss['stdout'].splitlines():
    fields = line.split()
    if len(fields) > 4:
        try:
            occupied.add(int(fields[4].rsplit(':', 1)[1]))
        except ValueError:
            pass
out['checked_ports'] = ports
out['occupied_test_ports'] = sorted(set(ports) & occupied)
mi = {l.split(':')[0]: int(l.split()[1]) for l in kernel_read(pathlib.Path('/proc/meminfo'), 'text').splitlines() if l.startswith(('MemAvailable:', 'MemFree:', 'MemTotal:'))}
out['host_memory_kib'] = mi
if host == 'b70':
    assert pathlib.Path('/sys/class/drm/card1/device').resolve().name == '0000:07:00.0'
    out['health'] = run(['docker', 'exec', 'verso-api', 'python', '-c', 'import urllib.request,json;d=json.load(urllib.request.urlopen("http://localhost:4000/health",timeout=10));print(json.dumps({k:d.get(k) for k in ["status","worker_connected"]}))'])
    out['pause_marker_exists'] = (home / 'exp42-queue' / 'PAUSED').exists()
else:
    out['loan'] = run((['runuser', '-u', 'kuna', '--', 'env', 'XDG_RUNTIME_DIR=/run/user/1000'] if os.getuid() == 0 else []) + ['systemctl', '--user', 'show', 'verso-loan-233-next-queue.service', '-p', 'ActiveState', '-p', 'MainPID', '-p', 'ControlGroup'])
out['test_slices'] = [str(p) for p in [p for p in kernel_read('/sys/fs/cgroup','directories') if p.name.startswith('myce-test')]]
print(json.dumps(out, indent=2))
