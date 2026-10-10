#!/usr/bin/env python3
"""CPU-only chaos runner, using only Python stdlib and the real myce binary."""
import argparse
import collections
import datetime
import fcntl
import http.server
import hashlib
import json
import os
from pathlib import Path
import random
import secrets
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

FINAL = {'succeeded', 'failed', 'cancelled'}


def synchronized(fn):
    def wrapped(self, *args, **kwargs):
        with self.mu: return fn(self, *args, **kwargs)
    return wrapped


def atomic(path, data):
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w') as f:
        json.dump(data, f); f.flush(); os.fsync(f.fileno())
    tmp.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def job(a):
    root = Path(a.run)
    jid, attempt = os.environ['MYCE_JOB_ID'], int(os.environ['MYCE_ATTEMPT'])
    cp = Path(os.environ['MYCE_CHECKPOINT_DIR']) / 'progress.json'
    saved = json.loads(cp.read_text()) if a.resumable and cp.exists() else {'step': 0}
    n = saved['step']
    rec = dict(job=jid, attempt=attempt, start=time.time(), resumed=n,
               source_attempt=saved.get('attempt'), pid=os.getpid(),
               token_present='MYCE_TOKEN' in os.environ,
               environment_hashes=[hashlib.sha256(v.encode()).hexdigest() for v in os.environ.values()])
    out = root / 'attempts' / (jid + '-' + str(attempt) + '.json')
    atomic(out, rec)
    if rec['token_present']: return 90
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    while n < a.steps and not stop.wait(2):
        n += 1
        if a.resumable: atomic(cp, dict(step=n, attempt=attempt, saved=time.time()))
    rec.update(end=time.time(), step=n, stopped=stop.is_set())
    atomic(out, rec)
    print(json.dumps({k: v for k, v in rec.items() if k != 'environment_hashes'}), flush=True)
    return 0


class Soak:
    def __init__(self, a):
        self.root = Path(a.run).resolve(); self.root.mkdir(parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.lock = (self.root / 'harness.lock').open('a')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.mu = threading.RLock(); self.stop = threading.Event()
        self.procs = {}; self.restores = []; self.files = []; self.proxy = None
        self.path = self.root / 'state.json'
        self.s = json.loads(self.path.read_text()) if self.path.exists() else dict(
            started=time.time(), duration=a.duration, seed=a.seed or secrets.randbits(32),
            jobs={}, violations=[], successes={}, terminal={}, failures=[], sequence=0,
            next_submit=0, next_check=0, next_fault=time.time()+60, phase='chaos')
        self.rng = random.Random(self.s['seed'])
        self.s.setdefault('drain_until', 0)
        tokenfile = self.root / '.token'
        if not tokenfile.exists():
            fd = os.open(tokenfile, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, 'w') as f: f.write(secrets.token_hex(32))
        self.token = tokenfile.read_text()
        self.env = {k: v for k, v in os.environ.items() if not k.startswith('MYCE_')}
        self.env.update(MYCE_TOKEN=self.token, GOMAXPROCS='2')
        for folder in ('attempts', 'fake-nas', 'logs'): (self.root / folder).mkdir(exist_ok=True)
        if 'queue_port' not in self.s:
            self.s.update(queue_port=self.port(), proxy_port=self.port())
        self.url = 'http://127.0.0.1:' + str(self.s['queue_port'])
        self.workerurl = 'http://127.0.0.1:' + str(self.s['proxy_port'])
        self.binary = str(Path(a.binary).resolve()); self.script = str(Path(__file__).resolve())
        self.prefix = ['nice', '-n', '19']
        if Path('/usr/bin/ionice').exists(): self.prefix += ['/usr/bin/ionice', '-c3']
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())

    def port(self):
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0)); return s.getsockname()[1]

    def save(self):
        with self.mu: atomic(self.path, self.s)

    def log(self, event, **fields):
        with self.mu:
            with (self.root / 'ledger.jsonl').open('a') as f:
                f.write(json.dumps(dict(at=time.time(), event=event, **fields))+'\n'); f.flush()

    def violation(self, kind, **fields):
        item = dict(kind=kind, **fields)
        with self.mu:
            if item not in self.s['violations']:
                self.s['violations'].append(item); self.log('violation', **item); self.save()

    def api(self, method='GET', path='/api/status', data=None):
        req = urllib.request.Request(self.url+path, method=method,
            data=None if data is None else json.dumps(data).encode(),
            headers={'Authorization': 'Bearer '+self.token, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=4) as r: return json.load(r)

    def spawn(self, key, args):
        f = (self.root / 'logs' / (key+'.log')).open('a'); self.files.append(f)
        p = subprocess.Popen(self.prefix+[self.binary]+args, env=self.env, stdout=f,
                             stderr=subprocess.STDOUT, start_new_session=True)
        self.procs[key] = p; self.log('process_start', process=key, pid=p.pid)
        atomic(self.root / 'pids.json', {k: v.pid for k, v in self.procs.items()})
        return p

    def queue(self):
        self.q = self.spawn('queue', ['queue', '--listen', '127.0.0.1:'+str(self.s['queue_port']),
            '--db', str(self.root / 'queue.db'), '--checkpoints', str(self.root / 'fake-nas')])
        for _ in range(100):
            if self.q.poll() is not None: raise RuntimeError('queue startup exit')
            try: self.api(); return
            except OSError: time.sleep(.1)
        raise RuntimeError('queue startup timeout')

    def worker(self, i, old=None):
        key = 'worker-'+str(i)
        p = self.spawn(key, ['join', '--queue', self.workerurl, '--name', key,
            '--gpu', 'fake', '--memory-mode', 'fixed', '--memory-gb', '8', '--heartbeat', '1s',
            '--state-dir', str(self.root / key), '--checkpoint-root', str(self.root / 'fake-nas')])
        self.restores.append((time.time()+8, 'audit', i, old or []))
        return p

    def processes(self, supervisors=False):
        found = []
        for d in Path('/proc').iterdir():
            if not d.name.isdigit(): continue
            try:
                argv = (d / 'cmdline').read_bytes().split(b'\0')
                if (self.script.encode() in argv and b'job' in argv and str(self.root).encode() in argv
                        and (supervisors or Path(os.fsdecode(argv[0])).name.startswith('python'))):
                    found.append(int(d.name))
            except OSError: pass
        return found

    def job_pids(self, i):
        ids = {j['id'] for j in (self.api()['jobs'] or []) if j.get('machine') == 'worker-'+str(i)}
        live = set(self.processes())
        return [r['pid'] for f in (self.root / 'attempts').glob('*.json')
                for r in [json.loads(f.read_text())] if r['job'] in ids and r['pid'] in live]

    def sig(self, pid, sig):
        try: os.kill(pid, sig)
        except ProcessLookupError: pass

    def proxy_start(self):
        h = self
        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_GET(self): self.relay()
            def do_POST(self): self.relay()
            def relay(self):
                raw = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                req = urllib.request.Request(h.url+self.path, method=self.command,
                    data=raw if self.command == 'POST' else None,
                    headers={'Authorization': self.headers.get('Authorization', ''), 'Content-Type': 'application/json'})
                try:
                    try:
                        with urllib.request.urlopen(req, timeout=35) as r: code, body = r.status, r.read()
                    except urllib.error.HTTPError as e: code, body = e.code, e.read()
                    if self.path == '/api/workers/complete' and code == 200:
                        result = json.loads(raw)
                        if result.get('success'):
                            for j in h.api()['jobs'] or []:
                                if j['id'] == result['id'] and j['state'] == 'succeeded' and j['attempt'] == result['attempt']:
                                    h.success(j)
                    self.send_response(code); self.end_headers(); self.wfile.write(body)
                except OSError:
                    try: self.send_response(503); self.end_headers()
                    except OSError: pass
        self.proxy = http.server.ThreadingHTTPServer(('127.0.0.1', self.s['proxy_port']), Proxy)
        self.proxy.daemon_threads = True
        threading.Thread(target=self.proxy.serve_forever, daemon=True).start()

    def success(self, j):
        with self.mu:
            signature = [j['attempt'], j.get('finished_at')]
            old = self.s['successes'].get(j['id'])
            if old and old != signature: self.violation('duplicate_success', job=j['id'], old=old, new=signature)
            if not old:
                self.s['successes'][j['id']] = signature
                self.log('success_report', job=j['id'], attempt=j['attempt']); self.save()

    @synchronized
    def submit(self):
        seq = self.s['sequence']; self.s['sequence'] += 1
        priority = 'production' if seq % 9 == 6 else ('deadline' if seq % 5 == 3 else 'experiment')
        resumable = priority != 'production' and seq % 4 != 3
        steps = 3 if priority == 'production' else self.rng.randint(12, 28)
        spec = dict(name='soak-'+str(seq), project='project-'+str(seq % 3), priority=priority,
            run=dict(command=['python3', self.script, 'job', '--run', str(self.root), '--steps', str(steps)]+
                     (['--resumable'] if resumable else []), workdir=str(self.root)),
            needs=dict(memory_gb=8, platform=dict(required=['fake'])), expected_minutes=steps/30, resumable=resumable)
        if priority == 'deadline':
            spec['deadline'] = datetime.datetime.fromtimestamp(time.time()+600, datetime.timezone.utc).isoformat()
        self.s['pending'] = dict(spec=spec, submitted=time.time(), healthy_capacity=not self.restores and
            all(p.poll() is None for k, p in self.procs.items() if k.startswith('worker')))
        self.save(); self.reconcile()

    @synchronized
    def reconcile(self):
        pending = self.s.get('pending')
        if not pending: return
        matches = [j for j in (self.api()['jobs'] or []) if j['spec']['name'] == pending['spec']['name']]
        j = matches[0] if matches else self.api('POST', '/api/jobs', pending['spec'])
        self.s['jobs'][j['id']] = pending; self.s.pop('pending')
        self.log('submit', job=j['id'], priority=j['spec']['priority'], project=j['spec']['project'], resumable=j['spec']['resumable'])
        self.save()

    def integrity(self):
        with sqlite3.connect('file:'+str(self.root / 'queue.db')+'?mode=ro', uri=True, timeout=5) as db:
            rows = db.execute('PRAGMA integrity_check').fetchall()
        self.log('integrity_check', ok=rows == [('ok',)])
        if rows != [('ok',)]: self.violation('sqlite_integrity', result=rows)

    @synchronized
    def failure(self):
        choices = ['kill_worker', 'pause_worker', 'kill_queue', 'leave', 'pause_all']
        kind = choices[len(self.s['failures']) % 5] if self.s['duration'] <= 300 else self.rng.choice(choices)
        i = self.rng.randrange(4); now = time.time(); old = self.job_pids(i)
        if kind == 'kill_queue':
            self.sig(self.q.pid, signal.SIGKILL); self.q.wait(timeout=5); self.integrity(); self.queue()
        elif kind == 'kill_worker':
            p = self.procs['worker-'+str(i)]; self.sig(p.pid, signal.SIGKILL); p.wait(timeout=5)
            self.restores.append((now+self.rng.randint(10, 30), 'worker', i, old))
        elif kind == 'leave':
            r = subprocess.run(self.prefix+[self.binary, 'leave', '--now', '--name', 'worker-'+str(i), '--queue', self.url],
                env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=15)
            if r.returncode: raise RuntimeError('leave failed')
            self.restores.append((now+8, 'leave_restart', i, old))
        else:
            delay = self.rng.randint(30, 120) if kind == 'pause_all' else self.rng.randint(10, 90)
            if self.s['duration'] <= 300: delay = 30 if kind == 'pause_all' else 10
            for w in range(4) if kind == 'pause_all' else [i]:
                self.sig(self.procs['worker-'+str(w)].pid, signal.SIGSTOP)
                self.restores.append((now+delay, 'continue', w, []))
        self.s['failures'].append(dict(at=now, kind=kind, worker=i))
        self.log('failure', kind=kind, worker=i); self.save()

    @synchronized
    def restore(self, force=False):
        pending = self.restores; self.restores = []
        for at, action, i, old in pending:
            if not force and at > time.time(): self.restores.append((at, action, i, old)); continue
            if action == 'continue': self.sig(self.procs['worker-'+str(i)].pid, signal.SIGCONT)
            elif action == 'leave_restart':
                p = self.procs['worker-'+str(i)]; self.sig(p.pid, signal.SIGTERM)
                try: p.wait(timeout=10)
                except subprocess.TimeoutExpired: self.sig(p.pid, signal.SIGKILL); p.wait(timeout=5)
                self.worker(i, old)
            elif action == 'worker': self.worker(i, old)
            elif action == 'audit':
                leftover = sorted(set(old) & set(self.processes()))
                self.log('restart_cleanup_check', worker=i, leftovers=leftover)
                if leftover: self.violation('restart_leftovers', worker=i, pids=leftover)
            self.log('restore', action=action, worker=i)

    @synchronized
    def check(self, final=False):
        jobs = {j['id']: j for j in self.api()['jobs'] or []}
        for jid, submission in list(self.s['jobs'].items()):
            j = jobs.get(jid)
            if not j: self.violation('missing_job', job=jid); continue
            old = self.s['terminal'].get(jid)
            if old and old != [j['state'], j['attempt']]: self.violation('terminal_regressed', job=jid)
            if j['state'] in FINAL: self.s['terminal'][jid] = [j['state'], j['attempt']]
            if j['state'] == 'failed' and j.get('losses', 0) < 3:
                self.violation('unexpected_job_failure', job=jid, reason=j.get('reason'))
            if j['state'] == 'succeeded':
                self.success(j)
                path = self.root / 'attempts' / (jid+'-'+str(j['attempt'])+'.json')
                if not path.exists(): self.violation('success_without_output', job=jid)
                else:
                    rec = json.loads(path.read_text()); cmd = j['spec']['run']['command']
                    steps = int(cmd[cmd.index('--steps')+1])
                    if rec.get('step', -1) < steps: self.violation('incomplete_success', job=jid)
                    if j['spec']['resumable']:
                        prior = [json.loads(p.read_text()) for p in (self.root / 'fake-nas' / jid).glob('attempt-*/progress.json')]
                        prior = [r for r in prior if r['attempt'] < rec['attempt'] and r['saved'] <= rec['start']]
                        if prior and max(r['step'] for r in prior) > 0 and rec['resumed'] == 0:
                            self.violation('saved_progress_lost', job=jid)
            if j['spec']['priority'] == 'production' and submission['healthy_capacity']:
                fault = any(submission['submitted'] <= f['at'] <= submission['submitted']+30 for f in self.s['failures'])
                if not fault:
                    if j.get('started_at') and j['start_latency_seconds'] > 30:
                        self.violation('production_starved', job=jid, seconds=j['start_latency_seconds'])
                    elif not j.get('started_at') and time.time() > submission['submitted']+30:
                        self.violation('production_not_started', job=jid)
            if final and j['state'] not in FINAL: self.violation('not_final_after_drain', job=jid, state=j['state'])
        digest = hashlib.sha256(self.token.encode()).hexdigest()
        for p in (self.root / 'attempts').glob('*.json'):
            r = json.loads(p.read_text())
            if r['token_present'] or digest in r['environment_hashes']:
                self.violation('token_in_job_environment', job=r['job'], attempt=r['attempt'])
        for p in (self.root / 'logs').glob('*'):
            if self.token.encode() in p.read_bytes(): self.violation('token_in_log', file=p.name)
        with sqlite3.connect('file:'+str(self.root / 'queue.db')+'?mode=ro', uri=True) as db:
            if db.execute('SELECT COUNT(*) FROM logs WHERE instr(text, ?) > 0', (self.token,)).fetchone()[0]:
                self.violation('token_in_queue_logs')
        count = len(self.processes())
        if count > 8: raise RuntimeError('job process ceiling exceeded')
        disk = sum(p.stat().st_size for p in self.root.parent.rglob('*') if p.is_file())
        if disk >= 1800*1024*1024: raise RuntimeError('disk safety ceiling reached')
        self.log('check', submitted=len(self.s['jobs']), states=dict(collections.Counter(j['state'] for j in jobs.values())),
            violations=len(self.s['violations']), job_processes=count, disk_bytes=disk, final=final)
        self.save()
        return all(jid in jobs and jobs[jid]['state'] in FINAL for jid in self.s['jobs'])

    def cleanup(self):
        for p in self.procs.values():
            if p.poll() is None: self.sig(p.pid, signal.SIGCONT); self.sig(p.pid, signal.SIGTERM)
        deadline = time.time()+12
        while any(p.poll() is None for p in self.procs.values()) and time.time() < deadline: time.sleep(.2)
        for p in self.procs.values():
            if p.poll() is None: self.sig(p.pid, signal.SIGKILL); p.wait(timeout=5)
        for pid in self.processes(supervisors=True): self.sig(pid, signal.SIGTERM)
        time.sleep(.5)
        for pid in self.processes(supervisors=True): self.sig(pid, signal.SIGKILL)
        if self.proxy: self.proxy.shutdown(); self.proxy.server_close()
        self.log('cleanup', remaining_job_processes=self.processes(supervisors=True))
        for f in self.files: f.close()

    def run(self):
        if (self.root / 'DONE').exists(): return
        if (self.root / 'FAILED').exists():
            (self.root / 'FAILED').rename(self.root / ('FAILED.previous-'+str(int(time.time()))))
        try:
            if (self.root / 'pids.json').exists():
                for pid in json.loads((self.root / 'pids.json').read_text()).values():
                    try:
                        cmd = (Path('/proc') / str(pid) / 'cmdline').read_bytes()
                        if self.binary.encode() in cmd and str(self.root).encode() in cmd:
                            self.sig(pid, signal.SIGCONT); self.sig(pid, signal.SIGTERM)
                    except OSError: pass
                time.sleep(2)
            self.queue(); self.proxy_start()
            for i in range(4): self.worker(i)
            self.log('start', seed=self.s['seed'], duration=self.s['duration'], queue=self.url, proxy=self.workerurl)
            end = self.s['started']+self.s['duration']
            while not self.stop.is_set():
                now = time.time(); self.restore()
                if self.s['phase'] == 'chaos' and now >= end:
                    self.s.update(phase='drain', drain_until=now+900); self.restore(force=True)
                    self.reconcile(); self.log('drain_start', seconds=900); self.save()
                if self.s['phase'] == 'chaos':
                    if now >= self.s['next_submit']:
                        self.reconcile()
                        if sum(j['state'] not in FINAL for j in self.api()['jobs'] or []) < 16: self.submit()
                        self.s['next_submit'] = now+self.rng.randint(8, 14); self.save()
                    if now >= self.s['next_fault'] and not self.restores:
                        self.failure()
                        self.s['next_fault'] = now+(40 if self.s['duration'] <= 300 else self.rng.randint(60, 300)); self.save()
                if now >= self.s['next_check']:
                    done = self.check(); self.s['next_check'] = now+(10 if self.s['phase'] == 'drain' else 60); self.save()
                    if self.s['phase'] == 'drain' and (done or now >= self.s['drain_until']):
                        self.check(final=True); self.integrity(); break
                if len(self.processes()) > 8: raise RuntimeError('job process ceiling exceeded')
                if self.q.poll() is not None: raise RuntimeError('unexpected queue exit')
                for i in range(4):
                    expected = any(x[1] in ('worker', 'leave_restart') and x[2] == i for x in self.restores)
                    if self.procs['worker-'+str(i)].poll() is not None and not expected: raise RuntimeError('unexpected worker exit')
                self.stop.wait(1)
            if self.stop.is_set():
                self.log('interrupted'); self.save(); self.cleanup()
                atomic(self.root / 'FAILED', dict(interrupted=True, message='Stopped by SIGTERM or SIGINT; rerun to resume'))
                return
            self.cleanup()
            atomic(self.root / 'DONE', dict(submitted=len(self.s['jobs']), terminal=len(self.s['terminal']),
                succeeded=len(self.s['successes']), failures_injected=self.s['failures'],
                violations=self.s['violations'], finished=time.time(), seed=self.s['seed'],
                terminal_states=dict(collections.Counter(v[0] for v in self.s['terminal'].values())),
                resumed_attempts=sum(json.loads(p.read_text())['resumed'] > 0 for p in (self.root / 'attempts').glob('*.json'))))
            self.s['phase'] = 'done'; self.save()
        except Exception as e:
            message = str(e).replace(self.token, '[redacted]')
            self.log('harness_failed', error_type=type(e).__name__, message=message)
            self.cleanup(); atomic(self.root / 'FAILED', dict(error_type=type(e).__name__, message=message))
            return 1
        return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['run', 'job']); p.add_argument('--run', required=True)
    p.add_argument('--binary', default=str(Path.home() / 'myce-soak' / 'myce'))
    p.add_argument('--duration', type=int, default=14400); p.add_argument('--seed', type=int)
    p.add_argument('--steps', type=int, default=20); p.add_argument('--resumable', action='store_true')
    a = p.parse_args()
    if a.mode == 'job': return job(a)
    if a.duration <= 0: p.error('duration must be positive')
    os.umask(0o077)
    return Soak(a).run()


if __name__ == '__main__': sys.exit(main())
