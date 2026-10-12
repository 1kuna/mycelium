from kernel_reads import kernel_read
from pathlib import Path
import hashlib, io, json, subprocess, tarfile, shlex, datetime, os
from remote_snippets import checked_python, checked_shell, python_command
HERE = Path('/Users/user/Documents/Git/Verso/.private/missions/mycelium-stage-ab-20261011')
WORK = Path('/Users/user/Developer/worktrees/mycelium-verso-pressure-r11')
ROOT = '/home/user/synthetic-run'
MAIN = '968c15d5d3e39060e997b1b3ba7f6450db881269'
SHA = 'unprepared'

def generated_0():
    return 'from pathlib import Path\nimport json,sys,os,hashlib,subprocess,datetime\nroot=Path("' + ROOT + '");sys.path.insert(0,str(root/\'source/scripts/pressure\'))\nfrom pressure import DiagnosticReaders,drm_owners\nfrom kernel_reads import kernel_read\nbaseline=json.loads(kernel_read(\'/home/user/synthetic-run/display-baseline.json\'))\nallow=json.loads(kernel_read(root/\'diagnostic-executables.json\'))\nfor exe,sha in allow.items():assert hashlib.sha256(kernel_read(exe,\'bytes\')).hexdigest()==sha\nreaders=DiagnosticReaders(allow,root/\'diagnostics.jsonl\');e={}\nowners=drm_owners([\'0000:07:00.0\'],baseline=baseline,allowed=[],diagnostics=readers,evidence=e)\nassert owners=={\'0000:07:00.0\':[]}\nassert Path(\'/mnt/nas\').is_mount()\nassert not Path(\'/mnt/nas/example/experiments/synthetic-run\').exists()\nassert not Path(\'/home/user/exp42-queue/PAUSED\').exists()\nassert {p.name for p in kernel_read(\'/sys/bus/pci/devices/0000:07:00.0/drm\',\'list\')} >= {\'card1\',\'renderD129\'}\nheadroom=int(next(r.split()[1] for r in kernel_read(\'/proc/meminfo\').splitlines() if r.startswith(\'MemAvailable:\')))/1024**2\nassert headroom>=18\nq=Path(\'/home/user/exp42-queue\');d=next(q.glob(\'564-*\'))\nassert json.loads(kernel_read(d/\'meta.json\'))[\'enabled\'] is False\nassert json.loads(kernel_read(d/\'status\'))[\'state\']==\'failed\'\nfor d in kernel_read(q,\'directories\'):\n try:s=json.loads(kernel_read(d/\'status\'))\n except (ValueError,OSError):continue\n assert not (s.get(\'state\') in (\'running\',\'stopping\',\'preempting\') and s.get(\'assigned_xpu\')==1)\nassert not subprocess.check_output([\'/usr/bin/docker\',\'ps\',\'-aq\',\'--filter\',\'label=myce.attempt\']).strip()\nhealth=json.loads(subprocess.check_output([\'/usr/bin/docker\',\'exec\',\'verso-api\',\'python\',\'-c\',\'import urllib.request,json;d=json.load(urllib.request.urlopen("http://localhost:4000/health",timeout=10));print(json.dumps({k:d.get(k) for k in ["status","worker_connected"]}))\']))\nassert health==dict(status=\'ok\',worker_connected=True)\n(root/\'display-baseline.json\').write_text(json.dumps(baseline,indent=2))\n(root/\'preflight.json\').write_text(json.dumps(dict(when=datetime.datetime.now(datetime.timezone.utc).isoformat(),owners=owners,display=e[\'baseline_clients\'],nas_mounted=True,pci=\'0000:07:00.0\',headroom_gib=headroom,production=health,legacy_gpu1_off=True),indent=2))\nfiles={str(p.relative_to(root)):hashlib.sha256(kernel_read(p,\'bytes\')).hexdigest() for p in [root/\'myce\',root/\'cpu-matrix.test\',root/\'finalize.py\',root/\'diagnostic-executables.json\',*list((root/\'source/scripts/pressure\').glob(\'*.py\')),*list((root/\'source/scripts/pressure\').glob(\'*.go\'))]}\n(root/\'launch-manifest.json\').write_text(json.dumps(dict(issue=238,main="' + MAIN + '",harness="' + SHA + '",pci=\'0000:07:00.0\',fenced_index=0,host_cap_gib=8,host_headroom_gib=18,device_envelope_gib=4,spark_reserve_gib=24,spark_cap_gib=2,collector_seconds=1800,files=files),indent=2))\nfor p in (root/\'display-baseline.json\',root/\'preflight.json\',root/\'launch-manifest.json\'):os.chown(p,1000,1000)\np=subprocess.run([\'python3\',\'-m\',\'unittest\',\'discover\',\'-s\',str(root/\'source/scripts/pressure\')],capture_output=True,text=True)\n(root/\'harness-tests.log\').write_text(p.stdout+p.stderr);assert p.returncode==0\nprint(json.dumps(dict(qualified=True,owners=owners,headroom_gib=headroom,tests=72,sha="' + SHA + '")))\n'

def generated_1():
    return 'from pathlib import Path\nimport sys,json,subprocess\nroot=Path("' + ROOT + '");sys.path.insert(0,str(root/\'source/scripts/pressure\'))\nfrom kernel_reads import kernel_read\nv={k:subprocess.check_output([\'systemctl\',\'--user\',\'show\',\'verso-loan-233-next-queue.service\',\'-p\',k,\'--value\'],text=True).strip() for k in (\'ActiveState\',\'MainPID\',\'ControlGroup\')}\nloan=v.copy()\nv[\'loan\']=loan\nv[\'memory_current\']=int(kernel_read(Path(\'/sys/fs/cgroup\')/v[\'ControlGroup\'].lstrip(\'/\')/\'memory.current\')) if v[\'ControlGroup\'] else 0\nv[\'MemAvailable_KiB\']=int(next(r.split()[1] for r in kernel_read(\'/proc/meminfo\').splitlines() if r.startswith(\'MemAvailable:\')))\nassert v[\'ActiveState\']==\'inactive\' and v[\'MainPID\']==\'0\' and not v[\'ControlGroup\'] and v[\'memory_current\']==0 and v[\'MemAvailable_KiB\']>=26*1024**2\nassert Path(\'/mnt/nas\').is_mount()\nassert subprocess.check_output([\'nvidia-smi\',\'--query-gpu=uuid\',\'--format=csv,noheader\'],text=True).strip()==\'GPU-00000000-0000-0000-0000-000000000000\'\n(root/\'preflight.json\').write_text(json.dumps(v,indent=2));print(json.dumps(v))\n'

def staging_source():
    return 'from pathlib import Path; import time; root=Path(' + repr(ROOT) + '); root.mkdir(exist_ok=True); assert not any((root/n).exists() for n in ("owned-pause","logs","phases","DONE","FAILED","launch-receipt.json")); suffix=".prepared-retired-"+str(time.time_ns()); [(root/n).rename(root/(n+suffix)) for n in ("source","myce","cpu-matrix.test","finalize.py","diagnostic-executables.json") if (root/n).exists()]'
if __name__ == '__main__':
    SHA = subprocess.check_output(['git', '-C', str(WORK), 'rev-parse', 'HEAD'], text=True).strip()
    assert subprocess.check_output(['git', '-C', str(WORK), 'ls-remote', 'origin', 'refs/heads/feat/verso-pressure-harness-r11'], text=True).split()[0] == SHA
    assert subprocess.check_output(['git', '-C', str(WORK), 'merge-base', '--is-ancestor', MAIN, 'HEAD']) == b''
    assert not subprocess.check_output(['git', '-C', str(WORK), 'status', '--porcelain'])
    assert not subprocess.check_output(['git', '-C', str(WORK), 'diff', MAIN, '--', 'internal', 'cmd', 'clients'])
    source = subprocess.check_output(['git', '-C', str(WORK), 'archive', 'HEAD'])

    def remote(host, code, sudo=False):
        r = subprocess.run(['ssh', host, checked_shell(('sudo -n ' if sudo else '') + 'python3 -')], input=checked_python(code), capture_output=True, text=True, timeout=45)
        (HERE / ('r11-prepare-' + host + '.log')).write_text(r.stdout + r.stderr)
        assert r.returncode == 0, host + ' preflight failed; inspect private log'
        return json.loads(r.stdout)

    def stage(host, binary):
        b = io.BytesIO()
        with tarfile.open(fileobj=b, mode='w:gz') as a:
            with tarfile.open(fileobj=io.BytesIO(source)) as src:
                for m in src.getmembers():
                    f = src.extractfile(m) if m.isfile() else None
                    m.name = 'source/' + m.name
                    a.addfile(m, f)
            files = [(binary, 'myce')]
            if host == 'b70':
                files += [(Path('/private/tmp/myce-test-r11-cpu-matrix.test'), 'cpu-matrix.test'), (WORK / 'scripts/pressure/finalize_stage_ab.py', 'finalize.py'), (WORK / 'scripts/pressure/diagnostic-executables.json', 'diagnostic-executables.json')]
            for (path, name) in files:
                a.add(path, arcname=name)
        prep = staging_source()
        r = subprocess.run(['ssh', host, python_command(prep)], capture_output=True)
        assert r.returncode == 0
        r = subprocess.run(['ssh', host, checked_shell('tar -xz -C ' + ROOT)], input=b.getvalue(), capture_output=True)
        assert r.returncode == 0, host + ' staging failure'
        print(json.dumps(dict(staged=host, sha=SHA)))
    stage('b70', Path('/private/tmp/myce-test-r11-b70'))
    stage('spark', Path('/private/tmp/myce-test-r11-spark'))
    code = generated_0()
    r = remote('b70', code, True)
    print(json.dumps(r))
    code = generated_1()
    r = remote('spark', code)
    (HERE / 'r11-spark-preflight.json').write_text(json.dumps(r, indent=2))
    print(json.dumps(dict(spark_qualified=True, headroom_gib=r['MemAvailable_KiB'] / 1024 ** 2)))
