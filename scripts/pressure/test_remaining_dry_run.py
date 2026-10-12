"""Run every remaining scenario body on Mac; Linux, Docker and queue are fakes."""
import copy
import ctypes
import stat
import sys
from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import stage_ab as s
import docker_drill as d
import docker_disable as dd
import memory_drill as m
import cpu_drill as c
import disable_drill as i
import installed_validation as iv
import pressure
from kernel_reads import Vanished, kernel_read as real_kernel

CONFIRM='Host cleanup confirmed: empty systemd worker cgroup and Mycelium Docker execution gone'

class FakeRun(s.Run):
    def __init__(self, world):
        self.world=world;self.env={'MYCE_TOKEN':'fake-local-token','PATH':'/usr/bin:/bin'}
        self.cgroup='0::/system.slice/myce-test-stage-ab-r12.service';self.worker_cgroup=None
        self.safety=None;self.stop=threading.Event();self.foreign_paused=threading.Event()
        self.foreign=SimpleNamespace(interruptions=0);self.receipts=[];self.ports=[]
        self.interruptions_at_begin=0;self.results=[];self.procs={};self.a=SimpleNamespace();self.active=None
    def save(self):pass
    def nas_directory(self,path):path.mkdir()
    def spawn(self,key,args,env=None):
        self.procs[key]=SimpleNamespace(returncode=None,poll=lambda:None)
        if key=='worker':
            self.world.machine=dict(id='b70-pressure-gpu1',gone=False,host_id='b70-pressure-host',free_memory_gb=4,host_available_memory_gb=20,free_host_memory_gb=20)
            if self.world.restarting:self.world.settle()
        if key=='loss-sentinel':self.world.sentinel=True
        return self.procs[key]
    def halt(self,key,kill=False):
        if key=='worker' and kill:
            self.world.machine['gone']=True;self.world.restarting=True
        self.procs.pop(key,None)
    def physical(self,events=None):
        # Real ownership logic consumes both native and daemon-owned start receipts.
        self.owned([dict(pid=900,start=123,boot='fake',exe=s.XPU,cgroup=self.cgroup)])
        if events:pressure.emit(events,'physical',valid=True,owners={s.PCI:[]})
        return {}
    def release_owned(self):self.owned([])
    def scenario_cleanup(self):
        self.release_owned();self.world.jobs={};self.world.machine={};self.world.restarting=False
    def finished(self,ids,seconds=180):
        if self.phase.name=='B-installed-disable-enable':
            job=self.world.jobs[ids[0]];job.update(state='succeeded',attempt=2)
            pressure.emit(self.events,'start',job=job['id'],attempt=2,resumed=1,pid=900,start_ticks=123)
        return super().finished(ids,seconds)

class World:
    def __init__(self,root):
        self.root=root;self.run=FakeRun(self);self.jobs={};self.rows=[];self.machine={}
        self.sentinel=False;self.restarting=False;self.unit_active=False;self.disabled=False;self.clock=10
        self.fixture=json.loads((Path(__file__).parent/'fixtures/b70-test-inspect.json').read_text())[0]
        self.commands=[];self.cgroups={};self.oom_observed=0;self.device_nodes={'card0':1,'renderD128':2,'card1':3,'renderD129':4}
    def sleep(self,seconds):
        self.clock+=seconds
        if hasattr(self.run,'phase') and self.run.phase.name=='Docker-late':
            (self.run.phase/'delayed-started').touch();self.settle()
    def settle(self):
        for row in self.rows:row['State'].update(Running=False,Pid=0)
        for job in self.jobs.values():
            if job['state']=='running':job['state']='cancelled'
        for p in self.root.glob('phases/*/worker/*.docker.json'):
            rec=json.loads(p.read_text());rec.update(released=True,forced=p.parent.parent.name in ('Docker-force','Docker-slow'))
            p.write_text(json.dumps(rec))
        self.restarting=False
    def api(self,url,method='GET',path='/api/status',data=None,**kwargs):
        phase=self.run.phase;name=phase.name
        if method=='POST' and path=='/api/jobs':
            jid='job-'+str(len(self.jobs));job=dict(id=jid,state='running',attempt=1,losses=0,reason='')
            self.jobs[jid]=job
            if data['name']=='ceiling-oom':
                job.update(state='failed',reason='exceeded its 0.03125 GiB memory ceiling');self.oom_observed=2
            elif data['run'].get('docker'):
                ident='fake-'+name;row=copy.deepcopy(self.fixture);cid=('%064x' % (len(self.rows)+1))
                row['Id']=cid;row['Config']['Labels']['myce.attempt']=ident
                row['State'].update(Running=name!='Docker-late',Pid=901 if name!='Docker-late' else 0)
                row['HostConfig'].update(CgroupParent=d.SLICE,Memory=128*1024**2,MemorySwap=128*1024**2,Devices=[],DeviceRequests=None)
                self.rows.append(row);worker=phase/'worker';worker.mkdir(exist_ok=True)
                (worker/(ident+'.docker.json')).write_text(json.dumps(dict(identity=ident,released=False,forced=False)))
                pressure.emit(self.run.events,'start',job=jid,attempt=1,card='docker-cpu',identity=ident)
                pressure.emit(self.run.events,'commit',job=jid,attempt=1,unit=1)
                if name=='Docker-late':
                    (phase/'delayed-accepted').touch();(phase/'late-unit').write_text('myce-test-r12-late-fake.service')
            else:
                pressure.emit(self.run.events,'start',job=jid,attempt=1,pid=900,start_ticks=123)
                pressure.emit(self.run.events,'commit',job=jid,attempt=1,unit=1)
            return job.copy()
        if method=='POST' and path.endswith('/cancel'):
            if name=='Docker-slow':
                (self.root/'logs/queue.log').write_text('event=docker_cleanup_escalated')
            elif name!='Docker-late':self.settle()
            return {}
        holds=[]
        if name=='Docker-late' or self.restarting:holds=['Docker cleanup unconfirmed']
        if name=='Docker-slow' and (phase/'docker-config.json').exists():
            if json.loads((phase/'docker-config.json').read_text())['mode']=='slow':
                holds=['operator action required'];self.machine.update(free_memory_gb=0,free_host_memory_gb=19)
            elif (phase/'escalation.json').exists():self.settle()
        if name=='Memory-ceiling-isolation':
            # First read after the OOM must prove its neighbor is still running.
            if self.oom_observed:self.oom_observed-=1
            elif any(j['state']=='failed' for j in self.jobs.values()):
                for j in self.jobs.values():
                    if j['state']=='running':j['state']='succeeded'
            self.machine=dict(id='ceiling-cpu',memory_enforcement='memory ceilings enforced (cgroup v2)')
        machines=[self.machine.copy()] if self.machine else []
        if self.sentinel:machines.append(dict(id='cpu-loss-sentinel'))
        return dict(jobs=[j.copy() for j in self.jobs.values()],machines=machines,host_holds=holds)
    def kernel(self,path,operation='text',**kwargs):
        path=Path(path);parts=str(path)
        if parts=='/sys/bus/pci/devices/0000:07:00.0/drm':return [Path('card1'),Path('renderD129')]
        if parts=='/dev/dri':return [Path('/dev/dri')/name for name in self.device_nodes]
        if parts=='/proc/self/ns/mnt':return 'fake-private-namespace'
        if parts.startswith('/proc/'):
            if parts=='/proc/self/cgroup':return '0::/system.slice/myce-test-r12-installed-invocation.service'
            if path.name=='cgroup':
                row=next(r for r in self.rows if r['State']['Pid']==int(path.parent.name))
                return '0::/myce.slice/myce-test.slice/myce-test-r12.slice/'+d.SLICE+'/docker-'+row['Id']+'.scope\n'
            if path.name=='stat':return b'900 (fake) '+b' '.join([b'0']*19+[b'123'])
            if path.name=='exe':return s.XPU
        if parts.startswith('/sys/fs/cgroup'):
            if path.name=='memory.current':return '1024'
            if path.name=='cgroup.procs':return ''
            if path.name=='cgroup.events':return 'populated 0\n'
            if operation=='list':raise Vanished(path)
            return 'anon 512\n'
        return real_kernel(path,operation)
    def output(self,args,**kwargs):
        self.commands.append(args)
        if args[0]==d.REAL:
            if args[1]=='ps':value='\n'.join(r['Id'] for r in self.rows if ('-aq' in args or r['State']['Running']))
            elif args[1]=='inspect':value=json.dumps([r for r in self.rows if r['Id'] in args[2:]])
            else:raise AssertionError(args)
        elif args[0]=='findmnt':value='ext4\n'
        else:raise AssertionError(args)
        return value if kwargs.get('text') else value.encode()
    def command(self,args,**kwargs):
        self.commands.append(args);out='';rc=0
        if args[0]==d.REAL:
            if args[1]=='kill':
                assert any(r['Id']==args[-1] for r in self.rows);self.settle()
            elif args[1:3]!=['image','inspect']:raise AssertionError(args)
        elif args[0]=='systemd-run':
            if '--unit=myce-test-r12-installed-invocation.service' in args:
                code=(self.root/'installed-invocation.py').read_text().replace('/home/user/synthetic-run',str(self.root))
                original_stat=os.stat
                def node_stat(path,*args,**kwargs):
                    if str(path)=='/dev/null':return SimpleNamespace(st_mode=stat.S_IFCHR,st_rdev=0)
                    if str(path).startswith('/dev/dri/'):return SimpleNamespace(st_mode=stat.S_IFCHR,st_rdev=self.device_nodes[Path(path).name])
                    return original_stat(path,*args,**kwargs)
                with patch.object(os,'getuid',return_value=0),patch.object(os,'stat',side_effect=node_stat), \
                     patch.object(ctypes,'CDLL',return_value=SimpleNamespace(unshare=lambda flags:0)), \
                     patch.object(os,'execv') as execute,patch.object(sys,'argv',['fenced-installer',str(self.root/'installation/worker-gpu1/config.json'),'detect']), \
                     patch('kernel_reads.kernel_read',self.kernel):
                    exec(compile(code,'fenced-installer','exec'),{'__name__':'__main__'})
                    exec_args=execute.call_args.args[1][5:]
                assert self.device_nodes['card0']==self.device_nodes['renderD128']==0
                return self.user_command(exec_args)
            self.unit_active=True
            if any('CPU-fast-matrix' in a for a in args):
                marker=self.run.phase/'child.exit';marker.write_text('0')
                self.cgroups['cpu']='/system.slice/myce-test-r12-cpu-fast-matrix.service'
            else:self.unit_active=True
        elif args[0]=='systemctl':
            if 'show' in args and m.UNIT in args:
                out='LoadState=loaded\nActiveState='+('active' if self.unit_active else 'inactive')+'\nControlGroup='+('/system.slice/'+m.UNIT if self.unit_active else '')+'\nMainPID='+('901' if self.unit_active else '0')+'\nDevicePolicy=closed\n'
            elif 'show' in args:
                active=self.unit_active
                out='ActiveState='+('active' if active else 'inactive')+'\nControlGroup='+ (self.cgroups.get('cpu','') if active else '')+'\nExecMainStatus=0\n'
            elif 'stop' in args:self.unit_active=False
        elif args[0]=='mount':
            if '--bind' in args:self.device_nodes[Path(args[-1]).name]=0
        elif args[0].endswith('/myce') and 'resolve-docker' in args:pass
        elif args[0]=='/usr/sbin/runuser':return self.user_command(args[4:],**kwargs)
        else:raise AssertionError('unfaked boundary '+repr(args))
        return subprocess.CompletedProcess(args,rc,out,'')
    def user_command(self,args,**kwargs):
        out='';scoped=i.SCOPED if getattr(self.run,'installed',False) else self.run.phase/'host-state'
        if 'install' in args:
            i.GPU_UNIT.write_text(str(self.root/'installation'))
            config=self.root/'installation/worker-gpu1/config.json';config.parent.mkdir(parents=True)
            config.write_text('{}');self.unit_active=False
        elif 'uninstall' in args:
            i.GPU_UNIT.rename(self.root/'retired-unit');self.unit_active=False
        elif 'disable' in args:
            scoped.mkdir(parents=True,exist_ok=True)
            (scoped/'DISABLED').write_text('{"generation":1}');(scoped/'disable-receipt.json').write_text('{"generation":1}')
            self.disabled=True;self.unit_active=False;self.settle()
            for job in self.jobs.values():job['state']='queued'
            out=CONFIRM
        elif 'enable' in args:
            (scoped/'DISABLED').rename(scoped/'retired-DISABLED');self.disabled=False;self.unit_active=True
        elif 'show' in args:
            enabled=self.unit_active
            out='ActiveState='+('active' if enabled else 'inactive')+'\nControlGroup='+(('/user.slice/myce-worker@gpu1.service' if getattr(self.run,'installed',False) else '/user.slice/myce-worker.service') if enabled else '')+'\nMainPID='+('901' if enabled else '0')+'\nConditionResult='+('yes' if enabled else 'no')+'\nKillMode=control-group\nSendSIGKILL=yes\n'
        elif 'start' in args:
            self.unit_active=not self.disabled and ((self.root/'installed-ready').exists() if getattr(self.run,'installed',False) else True)
            self.machine=dict(id='b70-pressure-gpu1',gone=False,host_id='installed-host')
        elif 'stop' in args:self.unit_active=False
        elif 'is-active' in args:out='inactive\n'
        elif 'daemon-reload' in args or 'reset-failed' in args:pass
        else:raise AssertionError('unfaked user boundary '+repr(args))
        return subprocess.CompletedProcess(args,0,out,'')

class RemainingDryRun(unittest.TestCase):
    def exercise(self,kind):
        with tempfile.TemporaryDirectory() as temp,ExitStack() as stack:
            root=Path(temp)/'test';root.mkdir();(root/'phases').mkdir();(root/'logs').mkdir();nas=Path(temp)/'nas';nas.mkdir()
            world=World(root)
            for mod in (s,d,dd,m,c,i,iv,pressure):
                if hasattr(mod,'ROOT'):stack.enter_context(patch.object(mod,'ROOT',root))
                if hasattr(mod,'NAS'):stack.enter_context(patch.object(mod,'NAS',nas))
                if hasattr(mod,'api'):stack.enter_context(patch.object(mod,'api',world.api))
                if hasattr(mod,'kernel_read'):stack.enter_context(patch.object(mod,'kernel_read',world.kernel))
            units=Path(temp)/'units';units.mkdir();unit=units/'myce-worker.service';gpu=units/'myce-worker@gpu1.service'
            for mod in (i,dd):stack.enter_context(patch.object(mod,'UNIT',unit))
            stack.enter_context(patch.object(i,'GPU_UNIT',gpu));stack.enter_context(patch.object(i,'DROP',units/(gpu.name+'.d')/'myce-test-r12.conf'))
            control=root/'installed-host-state';stack.enter_context(patch.object(i,'CONTROL',control));stack.enter_context(patch.object(i,'SCOPED',control/'units/myce-worker@gpu1.service'))
            stack.enter_context(patch('os.chown'));stack.enter_context(patch('time.sleep',world.sleep));stack.enter_context(patch('time.monotonic',lambda:world.clock))
            stack.enter_context(patch.object(subprocess,'run',world.command));stack.enter_context(patch.object(subprocess,'check_output',world.output))
            stack.enter_context(patch.object(c,'cgroup_pids',return_value=[]))
            stack.enter_context(patch.object(c,'Path',lambda path:Path(temp)/'Trash' if str(path)=='/home/user/.local/share/Trash' else Path(path)))
            if kind=='docker':d.scenarios(world.run);expected=['Docker-'+x for x in ('grace','force','restart','loss','late','slow','disable')]
            elif kind=='memory':m.scenario(world.run);expected=['Memory-ceiling-isolation']
            elif kind=='cpu':c.scenarios(world.run);expected=['CPU-fast-matrix']
            else:
                world.run.begin('B-installed-disable-enable');world.run.disable_enable();expected=['B-disable-switch-evidence']
            self.assertEqual([r['scenario'] for r in world.run.results],expected)
            self.assertTrue(all(r['result']=='PASS' for r in world.run.results),world.run.results)
            self.assertGreater(len(world.commands),3)
    def test_all_seven_docker_paths(self):self.exercise('docker')
    def test_memory_ceiling_isolation(self):self.exercise('memory')
    def test_isolated_cpu_matrix(self):self.exercise('cpu')
    def test_installed_gpu_worker_disable_enable(self):self.exercise('installed')

if __name__=='__main__':unittest.main()

class DockerWrapperDryRun(unittest.TestCase):
    def test_create_start_inspect_ps_logs_term_kill_slow_and_late_dispatch(self):
        with patch.dict(os.environ,{'MYCE_TEST_DOCKER_PHASE':str(s.ROOT/'phases/fake')}):
            import docker_wrapper as wrapper
        with tempfile.TemporaryDirectory() as tmp,ExitStack() as stack:
            root=Path(tmp);phase=root/'phases/Docker-wrapper';phase.mkdir(parents=True)
            world=World(root);world.run.phase=phase
            ident='test-wrapper';row=copy.deepcopy(world.fixture);row['Config']['Labels']['myce.attempt']=ident
            world.rows=[row];worker=phase/'worker';worker.mkdir();(worker/'record.docker.json').write_text(json.dumps(dict(identity=ident)))
            config=phase/'docker-config.json';config.write_text('{"mode":"normal"}')
            stack.enter_context(patch.object(wrapper,'ROOT',root));stack.enter_context(patch.object(wrapper,'PHASE',phase))
            stack.enter_context(patch.object(subprocess,'check_output',world.output));stack.enter_context(patch.object(subprocess,'run',world.command))
            stack.enter_context(patch('time.sleep',world.sleep));execute=stack.enter_context(patch.object(os,'execv'))
            host=['--host','unix:///var/run/docker.sock'];limit=str(128*1024**2)
            create=['create','--label','myce.attempt='+ident,'--env','MYCE_MEMORY_LIMIT_BYTES='+limit,'--memory',limit,'--memory-swap',limit,d.IMAGE]
            wrapper.main(host+create)
            argv=execute.call_args.args[1];self.assertIn('--network=none',argv);self.assertIn('--cgroup-parent='+d.SLICE,argv)
            for args in (['start',row['Id']],['inspect',row['Id']],['ps','-aq','--filter','label=myce.attempt'],['logs',row['Id']],['kill','--signal','TERM',row['Id']],['kill','--signal','KILL',row['Id']]):wrapper.main(host+args)
            config.write_text('{"mode":"slow"}');wrapper.main(host+['inspect',row['Id']]);self.assertGreaterEqual(world.clock,20)
            config.write_text('{"mode":"late"}');wrapper.main(host+['start',row['Id']])
            self.assertTrue((phase/'late-unit').exists());self.assertTrue((phase/'delayed-accepted').exists())
            # Fence real start to the exact test ID; no external daemon is reached.
            with patch.object(subprocess,'run',return_value=subprocess.CompletedProcess([],0)) as commands:
                wrapper.main(['--delayed',row['Id']]);self.assertEqual(commands.call_args.args[0],['/usr/bin/docker','start',row['Id']])
            self.assertTrue((phase/'delayed-started').exists())
