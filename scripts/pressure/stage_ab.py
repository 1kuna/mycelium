#!/usr/bin/env python3
"""Detached, GPU1-only synthetic Stage A/B runner; never changes Mycelium core."""
from __future__ import annotations
from kernel_reads import kernel_read, Vanished
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

from pressure import (Checker, DiagnosticReaders, ForeignReaderPause, api, clock_domain, digest, drm_identity, drm_inventory,
                      drm_owners, emit, observe, replay, spec, submit)

PCI = '0000:07:00.0'
ROOT = Path('/home/kuna/myce-test-stage-ab-r11b-20261011')
NAS = Path('/mnt/verso-nas/Verso/experiments/exp-238-mycelium-stage-ab-r11b-20261011')
XPU = '/home/kuna/verso-labs/runtimes/experiment-xpu-venv/bin/python'
QUEUE = Path('/home/kuna/exp42-queue')
BASE = Path(__file__).parent


def ram_settlement(charged, stat, idle):
    reclaimable = stat.get('inactive_file', 0) + stat.get('slab_reclaimable', 0)
    return dict(cgroup_bytes=charged, memory_stat=stat, idle_cgroup_bytes=idle,
                reclaimable_bytes=reclaimable,
                ram_released=0 <= reclaimable <= charged and charged-reclaimable <= idle+345*1024**2)


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
    raw = kernel_read(p / 'stat', 'bytes')
    identity=dict(boot=clock_domain(),pid=pid,start=int(raw.rsplit(b')',1)[1].split()[19]),
                  exe=kernel_read(p/'exe','link'),cgroup=kernel_read(p/'cgroup').strip())
    assert int(kernel_read(p/'stat','bytes').rsplit(b')',1)[1].split()[19])==identity['start'],'process identity changed'
    return identity


def cgroup_pids(group):
    root=Path('/sys/fs/cgroup')/group.split('::',1)[-1].lstrip('/')
    def members(path):
        try:
            ids={int(v) for v in kernel_read(path/'cgroup.procs').split()}
            for child in kernel_read(path,'directories'):
                ids.update(members(child))
            return ids
        except Vanished:return set()
    return sorted(members(root)-{os.getpid()})


class Run:
    def __init__(self):
        assert os.getuid() == 0, 'privileged read-only proc collector required'
        self.cgroup = process(os.getpid())['cgroup']
        assert self.cgroup == '0::/system.slice/myce-test-stage-ab-r11b.service'
        self.baseline = json.loads(kernel_read(ROOT / 'display-baseline.json', 'text'))
        self.diagnostics = DiagnosticReaders(json.loads(kernel_read(ROOT/'diagnostic-executables.json', 'text')),
                                             ROOT/'diagnostics.jsonl')
        self.env = {k:v for k,v in os.environ.items() if not k.startswith('MYCE_') and k!='INVOCATION_ID'}
        self.env['MYCE_TOKEN'] = os.urandom(32).hex()
        os.environ['MYCE_TOKEN'] = self.env['MYCE_TOKEN']
        self.results, self.procs, self.files, self.receipts = [], {}, [], []
        self.safety = None
        self.recovering = threading.Event()
        self.counter_idle = threading.Event()
        self.observer_idle = threading.Event()
        self.pause = None
        self.active = None
        self.stop = threading.Event()
        self.block = threading.Event()
        self.proxy = None
        self.ack_armed = False
        self.total_peak = dict(device_gib=0, host_used_gib=0, cgroup_bytes=0)
        self.output = ROOT / 'results.json'
        self.foreign=ForeignReaderPause(ROOT/'diagnostics.jsonl')
        self.foreign_paused=threading.Event()
        self.foreign_exited_at=None
        self.next_scan=0
        self.last_physical={}
        self.interruptions_at_begin=0
        self.worker_cgroup=None
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
        if self.foreign.interruptions>self.interruptions_at_begin:
            evidence['interruption']='interrupted-by-foreign-reader, continued'
        self.results.append(dict(scenario=name, result=state, **evidence))
        self.save()

    def save(self):
        atomic(self.output, dict(results=self.results, peaks=self.total_peak,
            safety_hold=self.safety, cleanup='pending', boot=clock_domain(),
            issue=238, source_sha256=digest(kernel_read(BASE / 'pressure.py', 'text'))))

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
        while self.foreign_paused.is_set():
            if self.safety or self.stop.is_set():break
            self.stop.wait(.1)
        if self.safety: raise RuntimeError('physical/memory safety hold: ' + self.safety)
        if self.stop.is_set(): raise RuntimeError('owned runner stop requested')

    def submit(self, priority, units):
        self.physical()
        self.gate()
        return submit(self.a,self.url,priority,units)

    def submit_spec(self, request):
        self.physical()
        self.gate()
        return api(self.url,'POST','/api/jobs',request)

    def owned_group(self, group):
        return any(root and (group==root or group.startswith(root+'/')) for root in (self.cgroup,self.worker_cgroup))

    def owned(self, holders=None):
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
                try: data = [json.loads(kernel_read(f, 'text'))]
                except (ValueError, OSError): continue
            for e in data:
                if holders is None:
                    try: identity = process(e['pid'])
                    except Vanished: continue
                else:
                    holder=next((h for h in holders if h['pid']==e['pid']),None)
                    if holder is None:continue
                    identity={k:holder[k] for k in ('boot','pid','start','exe','cgroup')}
                if (identity['start'] == e.get('start_ticks') and self.owned_group(identity['cgroup'])
                        and identity['exe'] == os.path.realpath(XPU)):
                    ids.append(identity)
        return ids

    def measurement_observers(self,holders=()):
        parent=self.procs.get('measure')
        if parent is None or parent.poll() is not None:return []
        expected=process(parent.pid)
        executable=os.path.realpath(shutil.which('xpu-smi'))
        observers=[]
        for holder in holders:
            if (holder.get('ppid')==parent.pid and holder['exe']==executable
                    and holder['cgroup']==self.cgroup and holder.get('stats_readonly')
                    and process(parent.pid)==expected):
                observers.append({k:holder[k] for k in ('boot','pid','start','exe','cgroup')})
        return observers

    def physical(self, events=None):
        with self.inventory_lock:
            now=time.monotonic()
            if self.foreign_paused.is_set() and now<self.next_scan:return self.last_physical
            evidence={}
            try:
                owners=drm_owners([PCI],baseline=self.baseline,allowed=self.owned,
                    diagnostics=self.diagnostics,evidence=evidence)
                pending=any(not self.owned_group(r['cgroup'])
                            for r in self.diagnostics.pending.values())
                if pending:self.foreign_exited_at=None
                elif self.foreign.since is not None and self.foreign_exited_at is None:
                    self.foreign_exited_at=now
                # Corroborate vanished processes/DRM with a fresh independent driver counter.
                allowed=self.owned(evidence['holders']);vram={}
                for holder in evidence['holders']:
                    identity={k:holder[k] for k in ('boot','pid','start','exe','cgroup')}
                    if identity not in allowed:continue
                    for client in holder['clients']:
                        key=(client['pci'],client['client'])
                        vram[key]=max(vram.get(key,0),sum(v for k,v in client['resident_bytes'].items()
                                                     if k.startswith('drm-resident-vram')))
                freed=(self.foreign_exited_at is not None and self.device_at>self.foreign_exited_at
                       and self.device_current <= (48.140625+1)/1024+sum(vram.values())/1024**3)
                paused=self.foreign.update(pending,freed,now)
                if paused:self.foreign_paused.set();self.next_scan=now+2
                else:self.foreign_paused.clear();self.next_scan=0
                evidence['foreign_reader_paused']=paused
                self.last_physical=evidence
            except Exception as error:
                self.safety=type(error).__name__+':'+str(error)
                atomic(ROOT/'ownership-failure.json',evidence);raise
        if len(owners[PCI])>1:raise RuntimeError('physical_double_owner')
        if events:emit(events,'physical',valid=True,owners=owners)
        return evidence

    def monitor(self):
        while not self.stop.is_set():
            self.observer_idle.set()
            if self.recovering.is_set():
                self.stop.wait(.1);continue
            self.observer_idle.clear()
            began = time.monotonic()
            try:
                active = self.active
                self.physical(active)
                mem = {r.split()[0]:int(r.split()[1]) for r in kernel_read(Path('/proc/meminfo'), 'text').splitlines()
                       if r.startswith(('MemTotal:', 'MemAvailable:'))}
                available = mem['MemAvailable:'] / 1024**2
                if available < 18: raise RuntimeError('host headroom below 18 GiB')
                group = Path('/sys/fs/cgroup') / self.cgroup.split('::',1)[1].lstrip('/')
                charged = int(kernel_read(group / 'memory.current', 'text'))
                if self.worker_cgroup:
                    worker_group=Path('/sys/fs/cgroup')/self.worker_cgroup.split('::',1)[1].lstrip('/')
                    try:charged+=int(kernel_read(worker_group/'memory.current'))
                    except Vanished:
                        from disable_drill import empty,show
                        assert empty(self.worker_cgroup.split('::',1)[1],show(self)),'worker RAM evidence unavailable'
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
                self.save()
            self.stop.wait(max(0,.5-(time.monotonic()-began)))

    def counters(self):
        # CLI probes take about one second. Keep physical/status/headroom at 0.5s independently.
        executable=os.path.realpath(shutil.which('xpu-smi'))
        while not self.stop.is_set():
            self.counter_idle.set()
            if self.recovering.is_set():
                self.stop.wait(.1);continue
            self.counter_idle.clear()
            try:
                with self.inventory_lock:
                    p=subprocess.Popen([executable,'stats','-d','0','--json'],stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,env={k:v for k,v in self.env.items() if not k.startswith('MYCE_')})
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
                self.safety=type(e).__name__+':'+str(e);self.save()
            self.stop.wait(.05)

    def wait(self, condition, seconds=180, operation='scenario condition'):
        end = time.monotonic()+seconds
        while time.monotonic()<end:
            self.gate()
            try:
                value = condition()
                if value: return value
            except (OSError, ValueError, KeyError): pass
            time.sleep(.1)
        raise TimeoutError('bounded wait: '+operation)

    def start_queue(self):
        self.qargs = [str(ROOT/'myce'),'queue','--listen',f'127.0.0.1:{self.port}',
            '--db',str(self.phase/'queue.db'),'--checkpoints',str(self.cp),
            '--production-stop-grace','10s','--restart-grace','5s']
        if (self.phase/'hosts.json').exists():self.qargs+=['--host-config',str(self.phase/'hosts.json')]
        self.spawn('queue', self.qargs)
        self.wait(lambda: api(self.url),10)

    def start_worker(self):
        self.wargs = [str(ROOT/'myce'),'join','--queue',self.workerurl,
            '--name','b70-pressure-gpu1','--host-id','b70-pressure-host',
            '--gpu','xpu','--device','0','--memory-mode','fixed','--memory-gb','4',
            '--reserve-host-memory-gb','18','--host-memory-budget-gb','6','--memory-ceiling-slack-gb','0','--memory-swap-max-bytes','0','--heartbeat','250ms',
            '--state-dir',str(self.phase/'worker'),'--checkpoint-root',str(self.cp),
            '--host-state-dir',str(self.phase/'host-state')]
        self.spawn('worker',self.wargs)
        self.wait(lambda: (api(self.url).get('machines') or []),10)

    def begin(self,name,bound=30,hold=False):
        self.gate()
        self.interruptions_at_begin=self.foreign.interruptions
        self.idle_charged=int(kernel_read(Path('/sys/fs/cgroup')/self.cgroup.split('::',1)[1].lstrip('/')/'memory.current'))
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
        if hold:(self.phase/'hosts.json').write_text(json.dumps(dict(hosts={'b70-pressure-host':dict(production_hold=True,production_idle='180s')})))
        self.env['MYCE_HOST_STATE_DIR']=str(self.phase/'host-state')
        self.start_queue(); self.start_worker()
        self.physical(self.events)
        os.chown(self.events,1000,1000)
        self.active = self.events
        self.a = SimpleNamespace(events=self.events, backend='xpu',card='1',selector='0',machine='b70-pressure-gpu1',
            python=XPU,units=40,unit_seconds=.2,host_mib=64,gpu_mib=64,memory_gib=4,
            host_reservation_gib=5,refuse_stop=False)

    def rows(self):
        if not self.events.exists(): return []
        return [json.loads(line) for line in kernel_read(self.events, 'text').splitlines()]

    def event(self,job,kind):
        return next((e for e in self.rows() if e['kind']==kind and e.get('job')==job),None)

    def wait_event(self,job,kind,seconds=180):
        def check():
            value=self.event(job,kind)
            if value:return value
            state=next(j for j in api(self.url)['jobs'] if j['id']==job)
            if state['state'] in ('failed','cancelled'):
                raise RuntimeError('job '+job+' ended '+state['state']+' before '+kind)
        return self.wait(check,seconds)

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
        self.scenario_cleanup()

    def release_owned(self):
        # Signal only exact numeric/start identities proven to belong to this test unit.
        ids=self.owned()
        for sig in (signal.SIGTERM,signal.SIGKILL):
            for i in ids:
                try:
                    if process(i['pid'])==i: os.kill(i['pid'],sig)
                except (Vanished,ProcessLookupError): pass
            time.sleep(.3)
        end=time.monotonic()+5
        while self.owned():
            if time.monotonic()>=end:raise RuntimeError('test payload process remains')
            time.sleep(.1)

    def scenario(self,name,work,bound=30):
        try:
            self.begin(name,bound);work()
            if name.startswith('A-memory-') or name=='B-CPU-matrix-container':
                self.scenario_cleanup()
            else:self.end(name)
        except Exception as e:
            self.result(name,'HOLD' if self.safety else 'FAIL',error=type(e).__name__,
                        detail=str(e),events=str(getattr(self,'events','')))
            self.scenario_cleanup()

    def scenario_cleanup(self):
        from disable_drill import cleanup as installed_cleanup
        self.active=None;self.recovering.set()
        try:
            if self.observer.ident is not None and not self.observer_idle.wait(10):
                raise RuntimeError('observer did not settle')
            if self.counter.ident is not None and not self.counter_idle.wait(10):
                raise RuntimeError('counter did not settle')
            with self.inventory_lock:
                installed_cleanup(self)
                for key in list(self.procs):self.halt(key)
                self.block.clear();self.ack_armed=False
                if self.proxy:self.proxy.shutdown();self.proxy.server_close();self.proxy=None
                self.release_owned()
                # Only the runner may remain in its exact unit; no substring ownership.
                for sig in (signal.SIGTERM,signal.SIGKILL):
                    for number in cgroup_pids(self.cgroup):
                        pid=Path('/proc')/str(number)
                        try:
                            identity=process(int(pid.name))
                            if (identity['cgroup']==self.cgroup or identity['cgroup'].startswith(self.cgroup+'/')) and process(identity['pid'])==identity:
                                os.kill(identity['pid'],sig)
                        except (Vanished,ProcessLookupError):pass
                    time.sleep(.3)
                remaining=[]
                for number in cgroup_pids(self.cgroup):
                    pid=Path('/proc')/str(number)
                    try:
                        remaining.append(int(pid.name))
                    except Vanished:pass
                if remaining:raise RuntimeError('test processes remain')
                end=time.monotonic()+60
                evidence={}
                while True:
                    owners=drm_owners([PCI],baseline=self.baseline,allowed=[],diagnostics=self.diagnostics,evidence=evidence)
                    if not self.diagnostics.pending:break
                    if time.monotonic()>=end:raise RuntimeError('diagnostic release unconfirmed')
                    time.sleep(2)
                stats=json.loads(subprocess.check_output(['xpu-smi','stats','-d','0','--json'],timeout=3))
                metrics={v['metrics_type']:v.get('value') for v in stats['device_level']}
                device_mib=float(metrics['XPUM_STATS_MEMORY_USED'])
                assert stats['device_id']==0 and device_mib<=48.140625+16,'driver release unconfirmed'
                mem={r.split()[0]:int(r.split()[1]) for r in kernel_read('/proc/meminfo').splitlines()
                     if r.startswith(('MemTotal:','MemAvailable:'))}
                charged=int(kernel_read(Path('/sys/fs/cgroup')/self.cgroup.split('::',1)[1].lstrip('/')/'memory.current'))
                assert mem['MemAvailable:']/1024**2>=18,'RAM headroom unconfirmed'
                group=Path('/sys/fs/cgroup')/self.cgroup.split('::',1)[1].lstrip('/')
                stat=dict((k,int(v)) for k,v in (line.split() for line in kernel_read(group/'memory.stat').splitlines()))
                accounting=ram_settlement(charged,stat,self.idle_charged)
                atomic(self.phase/'ram-settlement.json',accounting)
                assert accounting['ram_released'],'test RAM release unconfirmed; see ram-settlement.json'
                receipt=dict(physical='released',remaining_pids=remaining,device_memory_mib=device_mib,
                             **accounting)
                atomic(self.phase/'scenario-cleanup.json',receipt)
                self.results[-1]['scenario_cleanup']=receipt
                self.device_current=device_mib/1024;self.device_at=time.monotonic()
                self.foreign_paused.clear();self.foreign.since=None;self.foreign_exited_at=None;self.next_scan=0
                self.safety=None;self.save()
        finally:self.recovering.clear()

    def swap(self,seed,action=None):
        self.a.units=400 if action in ('refuse','worker-before-kill','worker-after-kill') else 40
        self.a.refuse_stop=action in ('refuse','worker-before-kill','worker-after-kill')
        experiment=self.submit('experiment',self.a.units)
        self.wait_event(experiment,'commit')
        time.sleep(random.Random(seed).uniform(0,.19))
        if action=='queue-compute': self.halt('queue',True);self.start_queue()
        if action=='worker-compute': self.halt('worker',True);self.start_worker()
        if action=='partition': self.block.set()
        production=self.submit('production',1)
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
        j=self.submit('experiment',300);self.finished([j],100)
        queued=self.event(j,'finish')
        ratio=(queued['kernels']/queued['compute_seconds'])/(direct['kernels']/direct['compute_seconds'])
        self.result('A3-throughput','PASS' if ratio>=.95 else 'FAIL',
                    ratio=ratio,direct=direct,queued=queued)

    def measure(self,host):
        self.active=None
        self.halt('worker');self.release_owned()
        receipt=self.phase/f'measure-{host}.pid.json';self.receipts.append(receipt)
        manifest=self.phase/'synthetic-manifest';manifest.write_bytes(b'synthetic allocator, no inference\n')
        config=self.phase/f'measure-{host}.yaml'
        config.write_text(json.dumps(dict(models=[dict(name='synthetic',engine='server',path=str(manifest),
            size_bytes=manifest.stat().st_size,sha256=__import__('hashlib').sha256(kernel_read(manifest, 'bytes')).hexdigest(),
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
        measured=json.loads(kernel_read(prefix.with_suffix('.json'), 'text'))
        self.result(f'A-memory-myce-measure-{host}MiB','PASS' if p.returncode==0 else 'FAIL',
                    measured=measured,report=str(prefix))
        self.release_owned();self.start_worker()

    def cleanup(self):
        from disable_drill import cleanup as installed_cleanup
        installed_cleanup(self)
        self.active=None;self.stop.set()
        for thread in (self.observer,self.counter):
            if thread.ident is not None:thread.join(timeout=5)
        self.block.clear()
        if self.proxy:self.proxy.shutdown();self.proxy.server_close()
        for key in list(self.procs):self.halt(key)
        # Processes in this cgroup cannot be unrelated services; verify before each signal.
        for sig in (signal.SIGTERM,signal.SIGKILL):
            for number in cgroup_pids(self.cgroup):
                p=Path('/proc')/str(number)
                try:
                    i=process(int(p.name))
                    if (i['cgroup']==self.cgroup or i['cgroup'].startswith(self.cgroup+'/')) and process(i['pid'])==i:os.kill(i['pid'],sig)
                except (Vanished,ProcessLookupError):pass
            time.sleep(.3)
        final_evidence={}
        try:
            end=time.monotonic()+60
            while True:
                drm_owners([PCI],baseline=self.baseline,allowed=[],diagnostics=self.diagnostics,evidence=final_evidence)
                if not self.diagnostics.pending:break
                if time.monotonic()>=end:raise RuntimeError('diagnostic exit unconfirmed')
                time.sleep(2)
            physical='released'
        except Exception as e:physical='HOLD:'+type(e).__name__
        pause='pending independent ExecStopPost release proof' if self.pause else 'not created'
        remaining=[]
        for number in cgroup_pids(self.cgroup):
            p=Path('/proc')/str(number)
            try:
                remaining.append(int(p.name))
            except (Vanished,ProcessLookupError):pass
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
            assert Path('/mnt/verso-nas').is_mount()
            assert not NAS.exists()
            self.nas_directory(NAS)
            for path in (ROOT/'logs',ROOT/'phases'):
                path.mkdir();os.chown(path,1000,1000)
            assert not (ROOT/'DONE').exists() and not (ROOT/'FAILED').exists()
            # Require the pre-existing disabled 564 hold, without mutating its state/timer.
            d=next(QUEUE.glob('564-*'))
            assert json.loads(kernel_read(d/'meta.json', 'text'))['enabled'] is False
            assert json.loads(kernel_read(d/'status', 'text'))['state']=='failed'
            self.result('B-forced-reboots','EXCLUDED',executed=False,reason='not approved')
            self.physical()
            pause_path=QUEUE/'PAUSED'
            self.pause=kernel_read(ROOT/'owned-pause','bytes')
            assert kernel_read(pause_path,'bytes')==self.pause,'legacy pause ownership changed'
            time.sleep(1)
            for d in kernel_read(QUEUE, 'list'):
                if not d.is_dir():continue
                try:s=json.loads(kernel_read(d/'status', 'text'))
                except (ValueError,OSError):continue
                assert not (s.get('state') in ('running','stopping','preempting') and s.get('assigned_xpu')==1)
            self.counter.start();self.observer.start()
            self.wait(lambda:self.device_at>0,5)
            self.followons()
            from docker_drill import scenarios
            scenarios(self)
            from memory_drill import scenario as memory_scenario
            memory_scenario(self)
            from cpu_drill import scenarios as cpu_scenarios
            cpu_scenarios(self)
            self.scenario('B-installed-disable-enable',self.disable_enable)
            marker='DONE' if all(r['result'] in ('PASS','SKIP','EXCLUDED') for r in self.results) else 'FAILED'
        except Exception as e:
            error=dict(type=type(e).__name__,detail=str(e));self.result('runner','HOLD' if self.safety else 'FAIL',error=error)
        finally:
            try:cleanup=self.cleanup()
            except Exception as e:cleanup=dict(error=type(e).__name__);marker='FAILED'
            if cleanup.get('remaining_pids') or cleanup.get('physical')!='released':marker='FAILED'
            atomic(ROOT/marker,dict(results=str(self.output),cleanup=cleanup,error=error))

    def foreign_reader_canary(self):
        count=self.foreign.interruptions
        subprocess.run(['systemd-run','--quiet','--collect','--unit=myce-test-r11-foreign-reader',
            '--property=DevicePolicy=closed','--property=DeviceAllow=/dev/dri/card1 rw',
            '--property=DeviceAllow=/dev/dri/renderD129 rw','--property=MemoryMax=512M',
            '--property=RuntimeMaxSec=15s','--property=StandardOutput=null',
            '/usr/bin/xpu-smi','dump','-d','0','-m','0,18','-n','5'],check=True)
        self.wait(lambda:self.foreign.interruptions>count,20)
        job=self.submit('experiment',1)
        self.finished([job],15)

    def disable_enable(self):
        from disable_drill import drill,cleanup
        try:
            evidence=drill(self)
            self.result('B-disable-switch-evidence','PASS',**evidence)
        finally:
            cleanup(self)

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

    def spark_request(self,mode):
        request=dict(mode=mode,phase=self.phase.name,port=self.port,boot=clock_domain(),
            request_id=os.urandom(16).hex(),token=self.env['MYCE_TOKEN'])
        path=ROOT/'spark-request.json'
        temporary=path.with_suffix('.tmp')
        fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        with os.fdopen(fd,'w') as stream:json.dump(request,stream);stream.flush();os.fsync(stream.fileno())
        temporary.replace(path)
        def reply():
            path=ROOT/'spark-response.json'
            if not path.exists():return None
            value=json.loads(kernel_read(path, 'text'))
            if value.get('request_id')==request['request_id']:return value
        response=self.wait(reply,45,operation='Spark '+mode+' reply')
        assert response['ok'], 'Spark transport/worker failed: '+response.get('error','unknown')
        if mode=='stop':
            assert response.get('cleanup_confirmed'), 'Spark cleanup unconfirmed'
            self.spark_cleanup_confirmed=True
        return response

    def portable(self):
        request=spec(self.a,'experiment',40)
        request['run']['command']=['/usr/bin/python3',str(BASE/'migration_job.py'),str(self.events),'40']
        request['needs']['platform']={'preferred':['xpu','cuda']}
        request['needs']['memory_gb']=1;request['needs']['host_memory_gb']=.1
        request['allowed_machines']=['b70-pressure-gpu1','spark-pressure']
        at=time.monotonic();job=self.submit_spec(request)
        emit(self.events,'submit',at=at,job=job['id'],priority='experiment',units=40)
        return job['id']

    def followons(self):
        for mode,name in (('active','C-migrate-to-Spark'),('full','C-Spark-full-return-B70')):
            self.spark_cleanup_confirmed=False
            try:
                self.followon(name,mode)
            except Exception as error:
                self.result(name,'HOLD' if self.safety else 'FAIL',error=type(error).__name__,detail=str(error))
            finally:
                try:
                    if not self.spark_cleanup_confirmed:self.spark_request('stop')
                finally:self.scenario_cleanup()

    def followon(self,name,mode):
        self.begin(name,hold=True)
        self.a.memory_gib=1
        experiment=self.portable() if mode else self.submit('experiment',40)
        self.wait_event(experiment,'commit')
        if mode:
            self.spark_request(mode)
            self.wait(lambda:any(m['id']=='spark-pressure' and not m.get('gone') and not m.get('leaving') and (mode=='full' or m['free_memory_gb']>=1) for m in api(self.url)['machines']),20,operation='live Spark eligibility')
        production=self.submit('production',1)
        self.wait(lambda:any(j['id']==production and j['state']=='succeeded' for j in api(self.url)['jobs']),45)
        idle=self.event(production,'finish')['at']
        status=api(self.url)
        holds=status.get('host_holds') or []
        assert any('production idle' in text or 'production hold' in text for text in holds)
        waiting=None
        if mode=='full':
            spark_machine=next(m for m in status['machines'] if m['id']=='spark-pressure')
            assert spark_machine['free_memory_gb']==0, 'Spark-full worker reported capacity'
            waiting=next(j for j in status['jobs'] if j['id']==experiment)
            assert waiting['state']=='queued' and waiting.get('reason'), 'no visible capacity/hold explanation'
        states=self.finished([experiment,production],300)
        assert all(states[j]['state']=='succeeded' for j in (experiment,production))
        if mode:
            mapping=self.spark_request('stop')
            assert not mapping.get('phase_error'), 'Spark phase failure: '+str(mapping.get('phase_error'))
            sparkrows=json.loads(kernel_read(self.phase/'spark-events.json', 'text'))
        else:sparkrows=[];mapping={}
        resumed=[r for r in self.rows()+sparkrows if r['kind']=='start' and r.get('job')==experiment and r['attempt']>1]
        assert len(resumed)==1 and resumed[0]['resumed']>0
        if mode=='active':
            assert resumed[0]['machine']=='spark-pressure' and resumed[0]['platform']=='cuda'
        else:
            assert resumed[0]['machine']=='b70-pressure-gpu1' and resumed[0]['platform']=='xpu'
            assert resumed[0]['at']-idle>=180, 'B70 reopened before 180 idle seconds'
        self.active=None
        observe(self.url,self.events);self.physical(self.events)
        # Receipt clocks are mapped with recorded RTT uncertainty; use only final status after all output arrives.
        rows=self.rows();final_status=next(r for r in reversed(rows) if r['kind']=='status')
        checker=Checker(30,True,(PCI,))
        for row in sorted([r for r in rows if r['kind']!='status']+sparkrows,key=lambda r:r['at']):checker.feed(row)
        checker.feed(final_status);checked=checker.finish()
        self.result(name,'PASS' if not checked['violations'] else 'FAIL',checker=checked,
            host_holds=holds,waiting=waiting,placement=resumed,clock_mapping=mapping,
            reopen_seconds=resumed[0]['at']-idle if mode!='active' else None)
        self.halt('worker');self.halt('queue');self.release_owned()

    def boundary(self,boundary):
        if boundary=='completion-ack':
            self.proxy_start();self.ack_armed=True
            experiment=self.submit('experiment',10)
            self.wait(lambda:any(e['kind']=='fault' and e.get('boundary')=='completion-ack' for e in self.rows()))
        else:
            configured=spec(self.a,'experiment',40)
            command=configured['run']['command']
            configured['run']['command']=[XPU,str(BASE/'fault_job.py'),boundary,str(self.events)]+command[2:]
            at=time.monotonic();job=self.submit_spec(configured);experiment=job['id']
            emit(self.events,'submit',at=at,job=experiment,priority='experiment',units=40)
            self.wait(lambda:any(e['kind']=='fault' and e.get('boundary')==boundary for e in self.rows()))
        production=self.submit('production',1)
        self.finished([experiment,production])

    def production_only(self):
        for index in range(30):
            j=self.submit('production',1);states=self.finished([j],45)
            if index==0 and (states[j]['state']!='succeeded' or not self.event(j,'allocated')):
                self.safety='native_runtime_qualification_failed'
                raise RuntimeError(self.safety)
        j=self.submit('experiment',10);self.finished([j])

    def backlog(self):
        self.a.units=80
        experiment=self.submit('experiment',80)
        self.wait_event(experiment,'commit')
        ids=[self.submit('production',1) for _ in range(20)]
        self.finished([experiment]+ids,180)
        starts=[self.event(j,'start')['at']-self.event(j,'submit')['at'] for j in ids]
        assert starts[0]<=30 and max(starts)<=68
        self.result('B-backlog-starts','PASS',first_seconds=starts[0],last_seconds=starts[-1])

    def overcap(self):
        self.a.memory_gib=4.01
        j=self.submit('production',1)
        time.sleep(3)
        assert not self.event(j,'start')
        api(self.url,'POST',f'/api/jobs/{j}/cancel')
        self.finished([j])

    def wrong_allowlist(self):
        request=spec(self.a,'production',1)
        request['allowed_machines']=['not-an-approved-worker']
        at=time.monotonic();job=self.submit_spec(request);j=job['id']
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
        j=self.submit('production',1);self.finished([j])


if __name__=='__main__':Run().run()
