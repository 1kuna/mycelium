#!/usr/bin/env python3
"""Archive abandoned measured custody only after affirmative fixture-exit proof."""
import datetime
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import uuid


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


capacity, budget = module('gate-capacity'), module('gate-budget')


def read(path):
    if path.is_symlink() or not path.is_file():
        raise RuntimeError('custody file is missing or linked')
    return json.loads(path.read_text())


def hexadecimal(value, length=32):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{' + str(length) + '}', value) is not None


def host_inventory():
    # Environments are inspected privately; no commands or environment values
    # are returned in a decision receipt.
    selection = '-eo' if budget.sys.platform.startswith('linux') else '-axo'
    result = subprocess.run(['ps', 'eww', selection, 'pid=,ppid=,uid=,stat=,lstart=,command='],
                            capture_output=True, text=True, check=True, timeout=10,
                            env=dict(os.environ, LC_ALL='C'))
    rows = {}
    supervisors = {'gate-local.py', 'gate-budget.py', 'gate-measure.py',
                   'gate-scheduler.py', 'gate-progress.py', 'gate-linux.py', 'gate-guest.py',
                   'gate.sh', 'gate-leg.sh', 'gate-command.sh'}
    for line in result.stdout.splitlines():
        fields = line.split(None, 9)
        if len(fields) != 10:
            raise RuntimeError('host process inventory is incomplete')
        pid, parent, uid = map(int, fields[:3])
        tokens = fields[9].split()
        # Only executable/script arguments identify a supervisor. A shell -c
        # body or an environment value mentioning a script proves nothing.
        invocation = tokens[:1]
        if tokens and re.fullmatch(r'(?:python[0-9.]*|bash|sh)', Path(tokens[0]).name, re.IGNORECASE):
            index = 1
            while index < len(tokens) and tokens[index] in ('-u', '-B', '-E', '-I', '-s', '-S', '-O', '-OO', '-e', '-f'):
                index += 1
            if index < len(tokens) and not tokens[index].startswith('-'):
                invocation += tokens[index:index + 1]
        rows[pid] = dict(parent=parent, uid=uid, birth=' '.join(fields[4:9]),
                         markers=[t.split('=', 1)[1] for t in tokens if t.startswith(('GATE_RUN_ID=', 'GATE_CUSTODY_ID='))],
                         paths=[t.split('=', 1)[1] for t in tokens if t.startswith(('HOME=', 'TMPDIR=', 'GATE_SCRATCH='))],
                         supervisor=pid != os.getpid() and any(Path(t).name in supervisors for t in invocation))
    if os.getpid() not in rows:
        raise RuntimeError('host process inventory omitted its observer')
    return rows


GUEST_PROBE = r'''
import json, pathlib, sys
requests = json.loads(sys.argv[1])
challenge = sys.argv[2]
active = pathlib.Path.home()/'.cache/mycelium-gates/active'
if not active.is_dir(): raise RuntimeError('guest custody directory unavailable')
markers = [p.name for p in active.iterdir()]
cleanups = []
for request in requests:
    nonce = request['nonce']
    paths = [pathlib.Path('/tmp/mycelium-gate-'+nonce)] if nonce else pathlib.Path('/tmp').glob('mycelium-gate-*')
    matches = []
    for path in paths:
        info = path/'source-identity.json'
        receipt = path/'.reports/cleanup.json'
        if path.is_symlink(): raise RuntimeError('linked guest scratch')
        if info.is_symlink() or receipt.is_symlink(): raise RuntimeError('linked guest custody evidence')
        if not info.is_file(): continue
        if json.loads(info.read_text()) != request['source_info']: continue
        row = json.loads(receipt.read_text())
        if row.get('schema') != 1 or row.get('identity') != path.name.removeprefix('mycelium-gate-') or row.get('stopped') is not True or type(row.get('status')) is not int:
            raise RuntimeError('guest cleanup unconfirmed')
        if nonce and row['identity'] != nonce: raise RuntimeError('guest nonce changed')
        matches.append(row)
    if len(matches) != 1: raise RuntimeError('guest cleanup is missing or ambiguous')
    cleanups.append(dict(run_id=request['run_id'], receipt=matches[0]))
print(json.dumps(dict(markers=markers, cleanups=cleanups, challenge=challenge, stopped=not markers)))
'''


def guest_inventory(vm, requests):
    challenge = uuid.uuid4().hex
    result = subprocess.run(['limactl', 'shell', '--workdir=/', vm, '--', 'python3', '-c',
                             GUEST_PROBE, json.dumps(requests), challenge], capture_output=True, text=True,
                            check=True, timeout=20)
    row = json.loads(result.stdout)
    if not isinstance(row, dict) or row.get('markers') != [] or row.get('stopped') is not True or row.get('challenge') != challenge or not isinstance(row.get('cleanups'), list):
        raise RuntimeError('guest markers or inventory are unconfirmed')
    if sorted(r['run_id'] for r in row['cleanups']) != sorted(r['run_id'] for r in requests):
        raise RuntimeError('guest cleanup inventory is incomplete')
    for item in row['cleanups']:
        receipt = item['receipt']
        if (receipt.get('schema') != 1 or not hexadecimal(receipt.get('identity')) or
                receipt.get('stopped') is not True or type(receipt.get('status')) is not int):
            raise RuntimeError('guest cleanup is unconfirmed')
        request = next(r for r in requests if r['run_id'] == item['run_id'])
        if request['nonce'] and request['nonce'] != receipt['identity']:
            raise RuntimeError('guest cleanup nonce changed')
    return row


def identity_live(identity, processes):
    if (not isinstance(identity, dict) or type(identity.get('pid')) is not int or identity['pid'] <= 0
            or not isinstance(identity.get('process_identity'), str) or not identity['process_identity'].strip()):
        raise RuntimeError('supervisor identity is malformed')
    return processes.get(identity['pid'], {}).get('birth') == identity['process_identity']


def validate(folder, control, group):
    if not hexadecimal(folder.name) or folder.is_symlink():
        raise RuntimeError('receipt directory identity is malformed')
    manifest = read(folder / 'manifest.json')
    owner = manifest['capacity_owner']
    slot = Path(manifest['capacity_slot'])
    source = folder / 'source'
    if (manifest['identity'] != folder.name or slot.parent != control or slot.is_symlink()
            or not re.fullmatch(r'local-go-slot-[0-9]+', slot.name) or int(slot.name.rsplit('-', 1)[-1]) < 3
            or owner.get('uid', os.getuid()) != os.getuid()
            or owner.get('admission_protocol') != 2 or owner.get('resource_group') != group['resource_group']
            or owner.get('admission_config_sha256') != group['admission_config_sha256']
            or not hexadecimal(owner.get('run_id')) or type(owner.get('pid')) is not int or owner['pid'] <= 0
            or manifest.get('mode') != owner.get('mode') or manifest.get('platform') != owner.get('platform')
            or owner.get('source_root') != str(source) or not source.is_dir() or source.resolve() != source
            or owner.get('mode') not in ('full', 'quick') or owner.get('platform') not in ('native', 'linux', 'both')
            or read(slot / 'owner.json') != owner):
        raise RuntimeError('measured custody binding is malformed or changed')
    full = control / ('local-go-full-' + owner['run_id'] + '.custody') if owner['mode'] == 'full' else None
    if full and read(full) != owner:
        raise RuntimeError('full custody binding changed')
    identity_fields = ('process_identity', 'run_marker', 'scratch')
    precise = any(k in owner for k in identity_fields)
    if precise and (not all(k in owner for k in identity_fields) or not hexadecimal(owner['run_marker'])
                    or not isinstance(owner['process_identity'], str) or not owner['process_identity'].strip()
                    or owner['scratch'] != str(source / '.gate' / ('run.' + owner['run_marker']))):
        raise RuntimeError('execution identity is malformed')
    supervisors = []
    native = folder / 'native-custody.json'
    if native.exists():
        row = read(native)
        if (not precise or row.get('schema') != 1 or not hexadecimal(row.get('identity'))
                or row.get('run_marker') != owner['run_marker'] or row.get('scratch') != owner['scratch']
                or not isinstance(row.get('supervisors'), list) or not row['supervisors']):
            raise RuntimeError('native supervisor custody is malformed')
        supervisors = row['supervisors']
        for supervisor in supervisors:
            identity_live(supervisor, {})
    request, vm = None, 'mycelium-gates'
    if owner['platform'] in ('linux', 'both'):
        info = read(folder / 'source-identity.json')
        if (not isinstance(info, dict) or not hexadecimal(info.get('source_sha'), 40)
                or type(info.get('source_dirty')) is not bool
                or any(not hexadecimal(info.get(key), 64) for key in
                       ('source_tree_sha256', 'source_archive_sha256', 'instrumentation_sha256'))):
            raise RuntimeError('source identity is malformed')
        guest = folder / 'linux-custody.json'
        nonce = None
        if guest.exists():
            row = read(guest)
            if (row.get('schema') != 1 or not hexadecimal(row.get('identity'))
                    or row.get('source_info') != info or not isinstance(row.get('vm'), str) or not row['vm']):
                raise RuntimeError('guest custody binding is malformed')
            nonce, vm = row['identity'], row['vm']
        elif precise:
            raise RuntimeError('guest nonce custody is missing')
        request = dict(run_id=folder.name, nonce=nonce, source_info=info)
    return dict(folder=folder, owner=owner, slot=slot, full=full, source=source,
                precise=precise, supervisors=supervisors, request=request, vm=vm)


def verify_host(runs, processes, proof=None):
    if proof is None:
        proof = []
    proof.append(dict(host_process_count=len(processes),
        gate_marker_pids=sorted(p for p, r in processes.items() if r['markers']),
        supervisor_pids=sorted(p for p, r in processes.items() if r['supervisor'])))
    if any(row['markers'] or row['supervisor'] for row in processes.values()):
        raise RuntimeError('reconciliation requires global gate quiet')
    for run in runs:
        owner = run['owner']
        row_proof = dict(run_id=run['folder'].name, precise=run['precise'],
                         holder_pid=owner['pid'], holder_present=owner['pid'] in processes,
                         recorded_supervisors=run['supervisors'])
        proof.append(row_proof)
        if owner['pid'] in processes:
            raise RuntimeError('recorded holder is still live')
        if any(identity_live(s, processes) for s in run['supervisors']):
            raise RuntimeError('recorded supervisor is still live')
        scopes = [str(run['source'])]
        roots = {owner['pid'], *(s['pid'] for s in run['supervisors'])}
        owned = {p for p, row in processes.items() if row['parent'] in roots}
        for pid, row in processes.items():
            if run['precise'] and owner['run_marker'] in row['markers']:
                owned.add(pid)
            if any(p == scopes[0] or p.startswith(scopes[0] + os.sep) for p in row['paths']):
                owned.add(pid)
        while True:
            descendants = {p for p, r in processes.items() if r['parent'] in owned}
            if descendants <= owned:
                break
            owned.update(descendants)
        row_proof['marked_descendant_pids'] = sorted(owned)
        if owned:
            raise RuntimeError('marked or descendant fixtures remain')
        candidates = {pid for pid, row in processes.items() if row['uid'] == os.getuid()}
        row_proof['handle_inventory_uid'] = os.getuid()
        row_proof['checked_owner_processes'] = len(candidates)
        holders = budget.scratch_processes(scopes[0], candidates, all_handles=True)
        row_proof['file_holder_pids'] = sorted(holders)
        if holders:
            raise RuntimeError('source or scratch file handles remain')
        row_proof['verified'] = True
    return proof


def reconcile(folders, repo):
    control = capacity.root(repo).resolve()
    decision_id = uuid.uuid4().hex
    decision = dict(schema=1, identity=decision_id, at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    status='refused', phase='bindings', requested=[str(p) for p in folders], evidence={})
    receipt = control / ('reconcile-' + decision_id + '.json')
    decision['receipt'] = str(receipt)
    with capacity.admission_lock(control):
        try:
            group_path = control / 'local-go-resource-group.custody'
            group = read(group_path)
            if group.get('admission_protocol') != 2 or not hexadecimal(group.get('resource_group')) or not hexadecimal(group.get('admission_config_sha256'), 64):
                raise RuntimeError('resource group is malformed')
            lanes = capacity.execution_lanes(control)
            runs = [validate(p.absolute(), control, group) for p in folders]
            if len({r['slot'] for r in runs}) != len(runs) or not runs:
                raise RuntimeError('duplicate or empty reconciliation request')
            decision['phase'] = 'host-before'
            first = host_inventory()
            decision['evidence']['host_before'] = []
            verify_host(runs, first, decision['evidence']['host_before'])
            requests = [r['request'] for r in runs if r['request']]
            if any(not r['precise'] for r in runs) and not requests:
                raise RuntimeError('legacy global quiet also requires bound guest cleanup')
            vms = {r['vm'] for r in runs if r['request']} or {os.environ.get('GATE_VM', 'mycelium-gates')}
            if len(vms) != 1:
                raise RuntimeError('guest custody spans different VMs')
            decision['phase'] = 'guest'
            decision['evidence']['guest'] = guest_inventory(vms.pop(), requests)
            decision['phase'] = 'host-after'
            decision['evidence']['host_after'] = []
            verify_host(runs, host_inventory(), decision['evidence']['host_after'])
            decision['phase'] = 'bindings'
            # Recheck every immutable binding immediately before archiving.
            for run in runs:
                validate(run['folder'], control, group)
            if read(group_path) != group or capacity.execution_lanes(control) != lanes:
                raise RuntimeError('resource custody changed during verification')
            archive = control / ('.reconciled-' + decision_id)
            decision.update(status='verified', phase='archive', archive=str(archive))
            receipt.write_text(json.dumps(decision, indent=2) + '\n')
            archive.mkdir(mode=0o700)
            # Keep the group observation even when unrelated live lanes remain.
            (archive / 'resource-group.json').write_text(json.dumps(group) + '\n')
            for run in runs:
                (run['slot'] / 'owner.json').rename(archive / (run['slot'].name + '-owner.json'))
                if run['full']:
                    run['full'].rename(archive / run['full'].name)
            for run in runs:
                run['slot'].rmdir()
            if not capacity.execution_lanes(control):
                for name in ('local-go-slot', 'local-go-slot-2'):
                    guard = control / name
                    (guard / 'owner.json').rename(archive / (name + '-guard.json'))
                    guard.rmdir()
                group_path.rename(archive / 'resource-group-final.custody')
            decision['status'] = 'reconciled'
            return decision
        except Exception as error:
            if decision['status'] == 'verified':
                decision['status'] = 'archive-incomplete'
            decision['reason'] = str(error)
            raise RuntimeError('measured custody retained: ' + str(error)) from error
        finally:
            receipt.write_text(json.dumps(decision, indent=2) + '\n')
