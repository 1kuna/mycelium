#!/usr/bin/env python3
"""Detached, GPU1-only synthetic Stage A/B runner; never changes Mycelium core."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import random
import signal
import socket
import sqlite3
import shutil
import subprocess
import threading
import time
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.request

from pressure import (Checker, api, clock_domain, digest, drm_identity, drm_inventory,
                      drm_owners, emit, observe, replay, spec, submit)

PCI = '0000:07:00.0'
ROOT = Path('/home/user/synthetic-run')
NAS = Path('/mnt/nas/example/experiments/synthetic-run')
XPU = '/home/user/verso-labs/runtimes/experiment-xpu-venv/bin/python'
QUEUE = Path('/home/user/exp42-queue')
BASE = Path(__file__).parent


def atomic(path, value):
    tmp = path.with_name(path.name + f'.{threading.get_ident()}.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, indent=2); f.flush(); os.fsync(f.fileno())
    tmp.replace(path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def process(pid):
    p = Path('/proc') / str(pid)
    raw = (p / 'stat').read_bytes()
    return dict(boot=clock_domain(), pid=pid,
                start=int(raw.rsplit(b')', 1)[1].split()[19]),
                exe=os.readlink(p / 'exe'), cgroup=(p / 'cgroup').read_text().strip())


class Run:
    def __init__(self):
        assert os.getuid() == 0, 'privileged read-only proc collector required'
        self.cgroup = process(os.getpid())['cgroup']
        assert self.cgroup == '0::/system.slice/myce-test-stage-ab-r2b.service'
        self.baseline = json.loads((ROOT / 'display-baseline.json').read_text())
        self.env = {k:v for k,v in os.environ.items() if not k.startswith('MYCE_')}
        self.env['MYCE_TOKEN'] = os.urandom(32).hex()
        os.environ['MYCE_TOKEN'] = self.env['MYCE_TOKEN']
        self.results, self.procs, self.files, self.receipts = [], {}, [], []
        self.safety = None
        self.pause = None
        self.active = None
        self.stop = threading.Event()
        self.block = threading.Event()
        self.proxy = None
        self.ack_armed = False
        self.total_peak = dict(device_gib=0, host_used_gib=0, cgroup_bytes=0)
        self.output = ROOT / 'results.json'
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        self.observer = threading.Thread(target=self.monitor, daemon=True)
        self.counter = threading.Thread(target=self.counters, daemon=True)
        self.passive = []
        self.inventory_lock = threading.RLock()
        self.device_peak = 0
        self.device_current = 0
        self.device_at = 0
        self.ports = []

    def result(self, name, state, **evidence):
        self.results.append(dict(scenario=name, result=state, **evidence))
        self.save()

    def save(self):
        atomic(self.output, dict(results=self.results, peaks=self.total_peak,
            safety_hold=self.safety, cleanup='pending', boot=clock_domain(),
            issue=238, source_sha256=digest((BASE / 'pressure.py').read_text())))

    def nas_directory(self,path):
        # Create as the same UID that will save/copy checkpoints; never chmod/chown a share.
        groups=[int(v) for v in subprocess.check_output(['id','-G','1000']).split()]
        subprocess.run(['/usr/bin/mkdir','-m','700',str(path)],check=True,
                       user=1000,group=1000,extra_groups=groups)

    def spawn(self, key, args, env=None):
        log = (ROOT / 'logs' / (key + '.log')).open('a')
        self.files.append(log)
        # Commands stay in this exact device-fenced/memory-capped unit, under UID1000.
        groups = [int(v) for v in subprocess.check_output(['id','-G','1000']).split()]
        p = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT,
            env=env or self.env, user=1000, group=1000, extra_groups=groups,
            start_new_session=True, cwd=ROOT)
        self.procs[key] = p
        return p

    def halt(self, key, kill=False):
        p = self.procs.get(key)
        if p is None or p.poll() is not None: return
        p.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
        try: p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill(); p.wait(timeout=5)

    def gate(self):
        if self.safety: raise RuntimeError('physical/memory safety hold: ' + self.safety)
        if self.stop.is_set(): raise RuntimeError('owned runner stop requested')

    def owned(self):
        ids = []
        for f in self.receipts:
            if not f.exists(): continue
            if f.suffix == '.jsonl':
                data = []
                with f.open() as stream:
                    for line in stream:
                        try:
                            e = json.loads(line)
                        except ValueError:
                            continue  # A writer may be holding its append lock; replay validates it later.
                        if e.get('kind') == 'start': data.append(e)
            else:
                try: data = [json.loads(f.read_text())]
                except (ValueError, OSError): continue
            for e in data:
                try: identity = process(e['pid'])
                except (OSError, KeyError): continue
                if (identity['start'] == e.get('start_ticks') and identity['cgroup'] == self.cgroup
                        and identity['exe'] == os.path.realpath(XPU)):
                    ids.append(identity)
        return ids

    def measurement_observers(self):
        parent=self.procs.get('measure')
        if parent is None or parent.poll() is not None:return []
        expected=process(parent.pid)
        executable=os.path.realpath(shutil.which('xpu-smi'))
        observers=[]
        for path in Path('/proc').iterdir():
            if not path.name.isdigit():continue
            try:
                identity=process(int(path.name))
                fields=(path/'stat').read_bytes().rsplit(b')',1)[1].split()
                command=(path/'cmdline').read_bytes().split(b'\0')
                if (int(fields[1])==parent.pid and identity['exe']==executable
                        and identity['cgroup']==self.cgroup
                        and command[1:5]==[b'stats',b'-d',b'0',b'--json']
                        and process(parent.pid)==expected):observers.append(identity)
            except (OSError,ValueError):continue
        return observers

    def physical(self, events=None):
        evidence = {}
        with self.inventory_lock:
            try:
                owners = drm_owners([PCI], baseline=self.baseline, allowed=self.owned,
                    observers=lambda:self.passive+self.measurement_observers(), evidence=evidence)
            except Exception:
                atomic(ROOT/"ownership-failure.json",evidence);raise
        if len(owners[PCI]) > 1: raise RuntimeError('physical_double_owner')
        if events: emit(events, 'physical', valid=True, owners=owners)
        return evidence

    def monitor(self):
        while not self.stop.is_set():
            began = time.monotonic()
            try:
                active = self.active
                self.physical(active)
                mem = {r.split()[0]:int(r.split()[1]) for r in Path('/proc/meminfo').read_text().splitlines()
                       if r.startswith(('MemTotal:', 'MemAvailable:'))}
                available = mem['MemAvailable:'] / 1024**2
                if available < 18: raise RuntimeError('host headroom below 18 GiB')
                group = Path('/sys/fs/cgroup') / self.cgroup.split('::',1)[1].lstrip('/')
                charged = int((group / 'memory.current').read_text())
                self.total_peak['host_used_gib'] = max(self.total_peak['host_used_gib'], (mem['MemTotal:']-mem['MemAvailable:'])/1024**2)
                self.total_peak['cgroup_bytes'] = max(self.total_peak['cgroup_bytes'], charged)
                if active:
                    emit(active,'memory',valid=True,available_gib=available,minimum_available_gib=18)
                    try: observe(self.url, active, timeout=.25)
                    except OSError: emit(active,'outage')
                gpu = self.device_current
                if self.device_at and time.monotonic()-self.device_at>3:
                    raise RuntimeError("native device counter stale")
                with (ROOT / 'samples.jsonl').open('a') as f:
                    f.write(json.dumps(dict(at=time.monotonic(),available_gib=available,
                        host_used_gib=(mem['MemTotal:']-mem['MemAvailable:'])/1024**2,
                        device_gib=gpu,counter_at=self.device_at,cgroup_bytes=charged,scenario=str(active)))+'\n')
            except Exception as e:
                self.safety = type(e).__name__ + ':' + str(e)
                self.save(); return
            self.stop.wait(max(0,.5-(time.monotonic()-began)))

    def counters(self):
        # CLI probes take about one second. Keep physical/status/headroom at 0.5s independently.
        executable=os.path.realpath(shutil.which('xpu-smi'))
        while not self.stop.is_set():
            try:
                with self.inventory_lock:
                    p=subprocess.Popen([executable,'stats','-d','0','--json'],stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,env={k:v for k,v in self.env.items() if not k.startswith('MYCE_')})
                    identity=process(p.pid);identity['exe']=executable
                    self.passive=[identity]
                try:
                    output,_=p.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill();p.communicate();raise
                finally:
                    with self.inventory_lock:self.passive=[]
                if p.returncode:raise RuntimeError('native counter exit '+str(p.returncode))
                stats=json.loads(output);assert stats['device_id']==0
                metrics={v['metrics_type']:v.get('value') for v in stats['device_level']}
                gpu=float(metrics['XPUM_STATS_MEMORY_USED'])/1024
                if gpu>4:raise RuntimeError('device usage above 4 GiB test envelope')
                self.device_current=gpu;self.device_at=time.monotonic()
                self.device_peak=max(self.device_peak,gpu)
                self.total_peak['device_gib']=self.device_peak
            except Exception as e:
                self.safety=type(e).__name__+':'+str(e);self.save();return
            self.stop.wait(.05)

    def wait(self, condition, seconds=180):
        end = time.monotonic()+seconds
        while time.monotonic()<end:
            self.gate()
            try:
                value = condition()
                if value: return value
            except (OSError, ValueError, KeyError): pass
            time.sleep(.1)
        raise TimeoutError('bounded scenario wait')

    def start_queue(self):
        self.qargs = [str(ROOT/'myce'),'queue','--listen',f'127.0.0.1:{self.port}',
            '--db',str(self.phase/'queue.db'),'--checkpoints',str(self.cp),
            '--production-stop-grace','10s','--restart-grace','5s']
        self.spawn('queue', self.qargs)
        self.wait(lambda: api(self.url),10)

    def start_worker(self):
        self.wargs = [str(ROOT/'myce'),'join','--queue',self.workerurl,
            '--name','b70-pressure-gpu1','--host-id','b70-pressure-host',
            '--gpu','xpu','--device','0','--memory-mode','fixed','--memory-gb','4',
            '--reserve-host-memory-gb','18','--heartbeat','250ms',
            '--state-dir',str(self.phase/'worker'),'--checkpoint-root',str(self.cp)]
        self.spawn('worker',self.wargs)
        self.wait(lambda: (api(self.url).get('machines') or []),10)

    def begin(self,name,bound=30):
        self.gate()
        self.phase = ROOT/'phases'/name; self.phase.mkdir()
        os.chown(self.phase,1000,1000)
        self.cp = NAS/name; self.nas_directory(self.cp)
        self.events = self.phase/'events.jsonl'
        self.receipts.append(self.events)
        self.checker = Checker(bound, True, (PCI,))
        self.active = None
        with socket.socket() as s: s.bind(('127.0.0.1',0)); self.port=s.getsockname()[1]
        self.ports.append(self.port);atomic(ROOT/'ports.json',self.ports)
        self.url = f'http://127.0.0.1:{self.port}'
        self.workerurl=self.url
        self.start_queue(); self.start_worker()
        self.physical(self.events)
        os.chown(self.events,1000,1000)
        self.active = self.events
        self.a = SimpleNamespace(events=self.events, backend='xpu',card='1',selector='0',machine='b70-pressure-gpu1',
            python=XPU,units=40,unit_seconds=.2,host_mib=64,gpu_mib=64,memory_gib=4,
            host_reservation_gib=6,refuse_stop=False)

    def rows(self):
        if not self.events.exists(): return []
        return [json.loads(line) for line in self.events.read_text().splitlines()]

    def event(self,job,kind):
        return next((e for e in self.rows() if e['kind']==kind and e.get('job')==job),None)

    def finished(self,ids,seconds=180):
        return self.wait(lambda: (lambda jobs: jobs if all(jobs.get(j,{}).get('state') in
            ('succeeded','failed','cancelled') for j in ids) else None)(
            {j['id']:j for j in api(self.url).get('jobs') or []}),seconds)

    def end(self,name,**evidence):
        self.active = None
        observe(self.url,self.events); self.physical(self.events)
        replay(self.events,self.checker)
        checked = self.checker.finish()
        self.result(name,'PASS' if not checked['violations'] else 'FAIL',checker=checked,**evidence)
        self.halt('worker'); self.halt('queue')
        self.release_owned()
        connection=sqlite3.connect(f'file:{self.phase}/queue.db?mode=ro',uri=True)
        try: integrity=connection.execute('PRAGMA integrity_check').fetchone()[0]
        finally: connection.close()
        assert integrity=='ok'
        self.results[-1]['sqlite_integrity']=integrity;self.save()

    def release_owned(self):
        # Signal only exact numeric/start identities proven to belong to this test unit.
        ids=self.owned()
        for sig in (signal.SIGTERM,signal.SIGKILL):
            for i in ids:
                try:
                    if process(i['pid'])==i: os.kill(i['pid'],sig)
                except ProcessLookupError: pass
            time.sleep(.3)
        self.wait(lambda: not self.owned(),5)
        self.physical()

    def scenario(self,name,work,bound=30):
        try:
            self.begin(name,bound); work();self.end(name)
        except Exception as e:
            self.result(name,'HOLD' if self.safety else 'FAIL',error=type(e).__name__,
                        detail=str(e),events=str(getattr(self,'events','')))
            self.active=None;self.halt('worker');self.halt('queue')
            self.release_owned()
            if self.safety: raise

    def swap(self,seed,action=None):
        self.a.units=400 if action in ('refuse','worker-before-kill','worker-after-kill') else 40
        self.a.refuse_stop=action in ('refuse','worker-before-kill','worker-after-kill')
        experiment=submit(self.a,self.url,'experiment',self.a.units)
        self.wait(lambda:self.event(experiment,'commit'))
        time.sleep(random.Random(seed).uniform(0,.19))
        if action=='queue-compute': self.halt('queue',True);self.start_queue()
        if action=='worker-compute': self.halt('worker',True);self.start_worker()
        if action=='partition': self.block.set()
        production=submit(self.a,self.url,'production',1)
        if action=='queue-stopping':
            self.wait(lambda:any(j['id']==experiment and j['state']=='stopping' for j in api(self.url)['jobs']))
            self.halt('queue',True);self.start_queue()
        if action=='worker-before-kill':
            self.wait(lambda:any(j['id']==experiment and j['state']=='stopping' for j in api(self.url)['jobs']))
            self.halt('worker',True);self.start_worker()
        if action=='partition':
            time.sleep(5);self.block.clear()
        if action=='queue-assigned':
            self.wait(lambda:self.event(production,'start'))
            self.halt('queue',True);self.start_queue()
        if action=='queue-result':
            self.wait(lambda:self.event(production,'finish'))
            self.halt('queue',True);self.start_queue()
        if action=='worker-after-kill':
            self.wait(lambda:self.event(production,'start'))
            self.halt('worker',True);self.start_worker()
        self.finished([experiment,production])
        if action=='queue-idle': self.halt('queue',True);self.start_queue();self.finished([experiment,production])

    def proxy_start(self):
        runner=self
        target=self.url
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_GET(self):self.relay()
            def do_POST(self):self.relay()
            def relay(self):
                if runner.block.is_set(): self.send_error(503);return
                data=self.rfile.read(int(self.headers.get('Content-Length','0')))
                request=urllib.request.Request(target+self.path,method=self.command,
                    data=data if self.command=='POST' else None,
                    headers={k:v for k,v in self.headers.items() if k.lower() not in ('host','content-length')})
                try:
                    with urllib.request.urlopen(request,timeout=4) as res:
                        payload=res.read()
                        if self.path=='/api/workers/complete' and runner.ack_armed:
                            runner.ack_armed=False
                            emit(runner.events,'fault',boundary='completion-ack')
                            time.sleep(5)
                        self.send_response(res.status);self.end_headers();self.wfile.write(payload)
                except OSError:self.send_error(503)
        self.proxy=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        threading.Thread(target=self.proxy.serve_forever,daemon=True).start()
        self.ports.append(self.proxy.server_port);atomic(ROOT/'ports.json',self.ports)
        self.workerurl='http://127.0.0.1:'+str(self.proxy.server_port)
        self.halt('worker');self.start_worker()

    def direct(self):
        self.a.units=300
        cp=self.cp/'direct';self.nas_directory(cp)
        env={**self.env,'MYCE_JOB_ID':'direct','MYCE_ATTEMPT':'1','MYCE_CHECKPOINT_DIR':str(cp),
             'ONEAPI_DEVICE_SELECTOR':'level_zero:0','ZE_AFFINITY_MASK':'0'}
        env.pop('MYCE_TOKEN')
        cmd=spec(self.a,'experiment',300)['run']['command']
        p=self.spawn('direct',cmd,env)
        self.wait(lambda:p.poll() is not None,100)
        assert p.returncode==0
        direct=self.event('direct','finish')
        j=submit(self.a,self.url,'experiment',300);self.finished([j],100)
        queued=self.event(j,'finish')
        ratio=(queued['kernels']/queued['compute_seconds'])/(direct['kernels']/direct['compute_seconds'])
        self.results.append(dict(scenario='A3-throughput',result='PASS' if ratio>=.95 else 'FAIL',
                                 ratio=ratio,direct=direct,queued=queued))

    def measure(self,host):
        self.active=None
        self.halt('worker');self.release_owned()
        receipt=self.phase/f'measure-{host}.pid.json';self.receipts.append(receipt)
        manifest=self.phase/'synthetic-manifest';manifest.write_bytes(b'synthetic allocator, no inference\n')
        config=self.phase/f'measure-{host}.yaml'
        config.write_text(json.dumps(dict(models=[dict(name='synthetic',engine='server',path=str(manifest),
            size_bytes=manifest.stat().st_size,sha256=__import__('hashlib').sha256(manifest.read_bytes()).hexdigest(),
            platforms=['xpu'],memory_gb=4,host_memory_gb=6,device='0',
            device_env=dict(ONEAPI_DEVICE_SELECTOR='level_zero:{device}',ZE_AFFINITY_MASK='{device}'),
            start_command=[XPU,str(BASE/'measure_allocator.py'),'--port','{port}',
                           '--receipt',str(receipt),'--host-mib',str(host),'--gpu-mib','1024'],
            health_url='http://127.0.0.1:{port}/health',ready_timeout='30s',stop_timeout='2s')])) )
        warm=self.phase/'warmup.json';warm.write_text(json.dumps({'synthetic':dict(method='POST',path='/warm',body={})}))
        for p in (config,warm):os.chown(p,1000,1000)
        prefix=self.phase/f'measure-{host}'
        args=[str(ROOT/'myce'),'measure','--queue',self.url,'--model-config',str(config),
            '--models','synthetic','--worker','b70-pressure-gpu1','--device','0','--platform','xpu',
            '--memory-gb','4','--state-dir',str(self.phase/'worker'),'--warmup',str(warm),
            '--report-prefix',str(prefix),'--load-timeout','40s']
        p=self.spawn('measure',args);self.wait(lambda:p.poll() is not None,70)
        measured=json.loads(prefix.with_suffix('.json').read_text())
        self.result(f'A-memory-myce-measure-{host}MiB','PASS' if p.returncode==0 else 'FAIL',
                    measured=measured,report=str(prefix))
        self.release_owned();self.start_worker()

    def cleanup(self):
        self.active=None;self.stop.set()
        for thread in (self.observer,self.counter):
            if thread.ident is not None:thread.join(timeout=5)
        self.block.clear()
        if self.proxy:self.proxy.shutdown();self.proxy.server_close()
        for key in list(self.procs):self.halt(key)
        # Processes in this cgroup cannot be unrelated services; verify before each signal.
        for sig in (signal.SIGTERM,signal.SIGKILL):
            for p in Path('/proc').iterdir():
                if not p.name.isdigit() or int(p.name)==os.getpid():continue
                try:
                    i=process(int(p.name))
                    if i['cgroup']==self.cgroup and process(i['pid'])==i:os.kill(i['pid'],sig)
                except OSError:pass
            time.sleep(.3)
        final_evidence={}
        try:drm_owners([PCI],baseline=self.baseline,allowed=[],evidence=final_evidence);physical='released'
        except Exception as e:physical='HOLD:'+type(e).__name__
        if self.pause:
            p=QUEUE/'PAUSED'
            if p.exists() and p.read_bytes()==self.pause:
                p.rename(ROOT/'queue-PAUSED.released')
                pause='released'
            else:pause='ownership-changed; not altered'
        else:pause='not created'
        remaining=[]
        for p in Path('/proc').iterdir():
            if not p.name.isdigit() or int(p.name)==os.getpid():continue
            try:
                if process(int(p.name))['cgroup']==self.cgroup:remaining.append(int(p.name))
            except OSError:pass
        cleanup=dict(physical=physical,queue_pause=pause,remaining_pids=remaining,
                     final_display=final_evidence,units='transient unit collected on runner exit',
                     install='source/binary retained as inert reproduction evidence; no package installed')
        atomic(ROOT/'cleanup.json',cleanup)
        atomic(self.output,dict(results=self.results,peaks=self.total_peak,safety_hold=self.safety,cleanup=cleanup))
        for f in self.files:f.close()
        return cleanup

    def run(self):
        marker='FAILED'
        error=None
        try:
            assert Path('/mnt/nas').is_mount()
            assert not NAS.exists()
            self.nas_directory(NAS)
            for path in (ROOT/'logs',ROOT/'phases'):
                path.mkdir();os.chown(path,1000,1000)
            assert not (ROOT/'DONE').exists() and not (ROOT/'FAILED').exists()
            # Require the pre-existing disabled 564 hold, without mutating its state/timer.
            d=next(QUEUE.glob('564-*'))
            assert json.loads((d/'meta.json').read_text())['enabled'] is False
            assert json.loads((d/'status').read_text())['state']=='failed'
            self.fixed_defect_probes()
            self.result('B-whole-machine-hold','PENDING',executed=False,reason='approved de2365b follow-on; separate detached runner')
            self.result('B-forced-reboots','EXCLUDED',executed=False,reason='not approved')
            self.physical()
            pause_path=QUEUE/'PAUSED'
            self.pause=(json.dumps(dict(owner='myce-test-stage-ab-r2b',nonce=os.urandom(16).hex()))+'\n').encode()
            with pause_path.open('xb') as f:f.write(self.pause);f.flush();os.fsync(f.fileno())
            (ROOT/'owned-pause').write_bytes(self.pause)
            time.sleep(1)
            for d in QUEUE.iterdir():
                if not d.is_dir():continue
                try:s=json.loads((d/'status').read_text())
                except (ValueError,OSError):continue
                assert not (s.get('state') in ('running','stopping','preempting') and s.get('assigned_xpu')==1)
            self.counter.start();self.observer.start()
            self.wait(lambda:self.device_at>0,5)
            self.scenario('A1-30-production',lambda:self.production_only())
            for seed in range(20):self.scenario(f'A2-swap-{seed:02}',lambda seed=seed:self.swap(seed))
            self.scenario('A3-direct-queue-60s',self.direct)
            self.begin('A-memory')
            for host in (512,1024,2048,4096):self.measure(host)
            self.halt('worker');self.halt('queue');self.active=None
            self.scenario('A4-over-declaration',self.overcap)
            self.scenario('A4-wrong-allowlist',self.wrong_allowlist)
            # Wrong selectors are rejected by the workload before any XPU runtime allocation.
            self.scenario('A4-wrong-selector',self.wrong_selector)
            for boundary in ('startup','save','completion-ack'):
                self.scenario('B-production-during-'+boundary,lambda boundary=boundary:self.boundary(boundary))
                if self.proxy:self.proxy.shutdown();self.proxy.server_close();self.proxy=None
            self.cpu_matrix()
            for seed in (17,43,91):
                for fault in ('queue-compute','queue-stopping','queue-assigned','queue-result','queue-idle',
                              'worker-compute','worker-before-kill','worker-after-kill','refuse','partition'):
                    name=f'B-{fault}-seed-{seed}'
                    def work(fault=fault,seed=seed):
                        if fault=='partition':self.proxy_start()
                        if fault in ('worker-before-kill','worker-after-kill'):self.a.refuse_stop=True
                        self.swap(seed,fault)
                    self.scenario(name,work)
                    if self.proxy:self.proxy.shutdown();self.proxy.server_close();self.proxy=None
            self.scenario('B-20-job-backlog',self.backlog,68)
            self.result('B-Docker-settlement','HOLD',executed=False,
                reason='reviewed adapter/isolated daemon absent; raw command does not settle accepted daemon starts')
            self.result('B-NAS-native-hung-IO-boundaries','HOLD',executed=False,
                reason='missing-root probes are covered; actual hung-filesystem injection is not qualified')
            self.result('B-exact-start-save-ack-fsync-hooks','HOLD',executed=False,
                reason='native crash triggers use receipt boundaries; internal gate/fsync cut points qualified only by test binary, not a GPU job')

            marker='DONE' if all(r['result'] in ('PASS','EXPECTED FAIL','EXCLUDED') for r in self.results) else 'FAILED'
        except Exception as e:
            error=dict(type=type(e).__name__,detail=str(e));self.result('runner','HOLD' if self.safety else 'FAIL',error=error)
        finally:
            try:cleanup=self.cleanup()
            except Exception as e:cleanup=dict(error=type(e).__name__);marker='FAILED'
            if cleanup.get('remaining_pids') or cleanup.get('physical')!='released':marker='FAILED'
            atomic(ROOT/marker,dict(results=str(self.output),cleanup=cleanup,error=error))

    def fixed_defect_probes(self):
        for name,pattern in (('B-mtime-skew','^TestVersoPressureR2CheckpointSkew$'),
                             ('B-unavailable-NAS-production','^TestVersoPressureProductionDuringMissingNAS$')):
            p=self.spawn(name,[str(ROOT/'cpu-matrix.test'),'-test.run',pattern,'-test.v','-test.timeout','30s'])
            self.wait(lambda:p.poll() is not None,35)
            self.result(name,'PASS' if p.returncode==0 else 'FAIL',retested=True,
                log=str(ROOT/'logs'/(name+'.log')),invariants='test assertions; B70 CPU, no GPU allocation')

    def cpu_matrix(self):
        log=ROOT/'logs/cpu-matrix.log'
        temp=ROOT/'cpu-temp';temp.mkdir();os.chown(temp,1000,1000)
        p=self.spawn('cpu-matrix',[str(ROOT/'cpu-matrix.test'),'-test.run',
            'TestPressureLargeState|TestPressureDockerProxyMatrix|TestPressureForeignVersoStateMustReject|TestWorkerStateLockAndCorruption|TestAPIRejectsMalformedRequests|'
            'TestWorkerAttemptCheckpointIsolation|TestWorkerOrphanCleanupVerifiesIdentity|'
            'TestWorkerLegacyRootCheckpointAndSymlink|TestWorkerStopEventPreservesQueueDeadline|'
            'TestWorkerStopAndGraceDuringBlockedUpload|TestWorkerStopAndGraceDuringBlockedCheckin|'
            'TestProductionPreemptsDuringNASLoss|TestWorkerProductionWithoutNAS|TestBlockedNASDoesNotHoldQueue|'
            'TestBlockedCheckpointPreparationIsBounded|TestWorkerForceStopsBlockedSaveAtGrace',
            '-test.v','-test.timeout','5m'],{**self.env,'TMPDIR':str(temp)})
        self.wait(lambda:p.poll() is not None,310)
        self.result('B-native-CPU-state-and-control-matrix','PASS' if p.returncode==0 else 'FAIL',
            log=str(log),scope='real B70 filesystem/process/API tests; fake clocks and CPU commands, no GPU allocation',
            invariants='test assertions; no GPU job ledger for this CPU-only subset')

    def boundary(self,boundary):
        if boundary=='completion-ack':
            self.proxy_start();self.ack_armed=True
            experiment=submit(self.a,self.url,'experiment',10)
            self.wait(lambda:any(e['kind']=='fault' and e.get('boundary')=='completion-ack' for e in self.rows()))
        else:
            configured=spec(self.a,'experiment',40)
            command=configured['run']['command']
            configured['run']['command']=[XPU,str(BASE/'fault_job.py'),boundary,str(self.events)]+command[2:]
            at=time.monotonic();job=api(self.url,'POST','/api/jobs',configured);experiment=job['id']
            emit(self.events,'submit',at=at,job=experiment,priority='experiment',units=40)
            self.wait(lambda:any(e['kind']=='fault' and e.get('boundary')==boundary for e in self.rows()))
        production=submit(self.a,self.url,'production',1)
        self.finished([experiment,production])

    def production_only(self):
        for index in range(30):
            j=submit(self.a,self.url,'production',1);states=self.finished([j],45)
            if index==0 and (states[j]['state']!='succeeded' or not self.event(j,'allocated')):
                self.safety='native_runtime_qualification_failed'
                raise RuntimeError(self.safety)
        j=submit(self.a,self.url,'experiment',10);self.finished([j])

    def backlog(self):
        self.a.units=80
        experiment=submit(self.a,self.url,'experiment',80)
        self.wait(lambda:self.event(experiment,'commit'))
        ids=[submit(self.a,self.url,'production',1) for _ in range(20)]
        self.finished([experiment]+ids,180)
        starts=[self.event(j,'start')['at']-self.event(j,'submit')['at'] for j in ids]
        assert starts[0]<=30 and max(starts)<=68
        self.result('B-backlog-starts','PASS',first_seconds=starts[0],last_seconds=starts[-1])

    def overcap(self):
        self.a.memory_gib=4.01
        j=submit(self.a,self.url,'production',1)
        time.sleep(3)
        assert not self.event(j,'start')
        api(self.url,'POST',f'/api/jobs/{j}/cancel')
        self.finished([j])

    def wrong_allowlist(self):
        request=spec(self.a,'production',1)
        request['allowed_machines']=['not-an-approved-worker']
        at=time.monotonic();job=api(self.url,'POST','/api/jobs',request);j=job['id']
        emit(self.events,'submit',at=at,job=j,priority='production',units=1)
        time.sleep(3)
        assert not self.event(j,'start')
        api(self.url,'POST',f'/api/jobs/{j}/cancel');self.finished([j])

    def wrong_selector(self):
        # A deliberately incompatible selector pair fails before importing torch.
        # Do not initialize GPU0 even to demonstrate rejection.
        cp=self.cp/'wrong';self.nas_directory(cp)
        cmd=spec(self.a,'experiment',1)['run']['command']
        env={**self.env,'MYCE_JOB_ID':'wrong-selector','MYCE_ATTEMPT':'1','MYCE_CHECKPOINT_DIR':str(cp),
             'ONEAPI_DEVICE_SELECTOR':'level_zero:invalid','ZE_AFFINITY_MASK':'invalid'}
        env.pop('MYCE_TOKEN')
        p=self.spawn('wrong',cmd,env);self.wait(lambda:p.poll() is not None,10)
        assert p.returncode!=0 and not self.event('wrong-selector','allocated')
        j=submit(self.a,self.url,'production',1);self.finished([j])


if __name__=='__main__':Run().run()
