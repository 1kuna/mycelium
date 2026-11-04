package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Real `myce dev` API/runner, simulated scarce GPU inventory, no native GPU or
// fleet qualification. File-store transport moves an atomic generation between
// two separate worker roots; the main process and a native sidecar must release.
func TestScarceHostYieldDev(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "files"), Token: "local-fixture", Workers: 1, Devices: 2, HostID: "scarce", CPUBudgetCores: 2, MemoryGB: 8, Interval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c := d.Client()
	get := func(id string) *Job {
		t.Helper()
		var j Job
		if e := c.Call(ctx, "GET", "/api/jobs/"+id, nil, &j); e != nil {
			t.Fatal(e)
		}
		return &j
	}
	target := ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "cpu"}
	source := `import os, json, signal, sys, time
assert 'MYCE_TOKEN' not in os.environ
p=os.environ['MYCE_CHECKPOINT_DIR']
f=os.path.join(p,'save.json')
old=json.load(open(f)) if os.path.exists(f) else None
def save(g):
    temp=f+'.tmp'
    with open(temp,'w') as out:
        json.dump({'myce_checkpoint':{'generation':g},'value':{'step':g}},out)
        out.flush();os.fsync(out.fileno())
    os.replace(temp,f)
    fd=os.open(p,os.O_RDONLY);os.fsync(fd);os.close(fd)
with open('main.pid','w') as out:out.write(str(os.getpid()))
if old:
    assert old['myce_checkpoint']['generation']>=2
    with open('answer.json','w') as out:json.dump({'job':os.environ['MYCE_JOB_ID'],'attempt':int(os.environ['MYCE_ATTEMPT']),'restored':old['value']['step']},out)
    sys.exit(0)
save(1)
def stop(sig,frame):
    save(2)
    sys.exit(0)
signal.signal(signal.SIGTERM,stop)
while True:time.sleep(.01)
`
	s := cpuSpec(2, .25)
	s.SubmissionKey = "dev-portable-counter"
	s.Resumable = true
	s.ResumeCompatibility = &ResumeCompatibility{CheckpointFormat: "counter-v1", ResultContract: "counter-result-v1", Targets: []ResumeTarget{{Target: target, Environment: "none", ResultCheck: "counter-resume-test"}}}
	sourceFile := filepath.Join(root, "counter.py")
	if err := os.WriteFile(sourceFile, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(sourceFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.UploadFile(ctx, sourceFile, ref); err != nil {
		t.Fatal(err)
	}
	s.Files = FileManifest{"counter.py": ref}
	s.Run.Command = []string{python, "-u", "counter.py"}
	s.Run.MemoryGB = .125
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", MemoryGB: .125, Command: []string{"sh", "-c", `echo $$ > helper.pid; trap 'exit 0' TERM; while :; do sleep .01; done`}}}
	s.Outputs = map[string]string{"answer.json": "results"}
	j, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { j = get(j.ID); return j.StartedAt != nil && len(j.FileCheckpoints) > 0 })
	first := *j
	if first.Machine != "scarce" || first.Attempt != 1 {
		t.Fatal("initial placement", first.Machine, first.Attempt)
	}
	oldPIDs := []int{}
	for _, name := range []string{"main.pid", "helper.pid"} {
		data, e := os.ReadFile(filepath.Join(root, "workers", "fake-1", "job-files", j.ID, "1", name))
		if e != nil {
			t.Fatal(e)
		}
		pid, e := strconv.Atoi(strings.TrimSpace(string(data)))
		if e != nil {
			t.Fatal(e)
		}
		oldPIDs = append(oldPIDs, pid)
	}
	// Start the alternative only after the portable work occupies scarce CPU.
	workerCtx, stopWorker := context.WithCancel(context.Background())
	done := make(chan error, 1)
	general := cpuMachine("general", "general", "cpu", 2, 8)
	general.Devices = []MachineDevice{}
	general.CapabilityProfile.OS = runtime.GOOS
	general.CapabilityProfile.Arch = runtime.GOARCH
	w := Worker{sharedCPUDev: true, CPUIsolationProbe: func(context.Context) error { return nil }, Machine: general, StateDir: filepath.Join(root, "general"), Client: c, ModelConfig: d.Queue.models, Clock: d.Queue.clock, Interval: 20 * time.Millisecond, HostMemorySource: func(context.Context) (float64, error) { return 8, nil }}
	go func() { done <- w.Run(workerCtx) }()
	defer func() {
		stopWorker()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("alternative worker did not stop")
		}
	}()
	await(t, func() bool {
		d.Queue.mu.Lock()
		defer d.Queue.mu.Unlock()
		m := d.Queue.s.Machines[general.ID]
		return m != nil && !m.Gone
	})
	hard := spec("experiment")
	hard.Resumable = false
	hard.Needs.CPUCores = 1
	hard.Needs.HostMemoryGB = .125
	hard.Needs.Platform = Platform{Required: []string{"fake"}}
	hard.RequirementReasons = map[string]string{"backend:fake": "fixture requires simulated accelerator"}
	hard.Run.Command = []string{"sh", "-c", "sleep .2"}
	demand, err := c.Submit(ctx, hard)
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		j = get(j.ID)
		h := get(demand.ID)
		if h.StartedAt != nil {
			for _, pid := range oldPIDs {
				if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
					t.Fatalf("demand started before exact process/sidecar release: %d %v", pid, err)
				}
			}
		}
		return j.State == "succeeded" && h.State == "succeeded"
	})
	if j.Machine != "general" || j.Attempt != 2 || j.Spec.SubmissionKey != s.SubmissionKey || j.InfrastructureRetries != 0 || j.Losses != 0 || j.PreemptionCount != 1 || len(j.OutputRefs) != 1 {
		t.Fatal("yield identity/result/accounting", j)
	}
	if j.ScarceHostYield == nil || j.ScarceHostYield.Generation < 2 || len(j.FileCheckpoints) < 2 {
		t.Fatal("durable generations not preserved", j.ScarceHostYield, j.FileCheckpoints)
	}
	out := filepath.Join(root, "accepted.json")
	if err := c.DownloadFile(ctx, j.OutputRefs["answer.json"], out); err != nil {
		t.Fatal(err)
	}
	var answer struct {
		Job      string `json:"job"`
		Attempt  int    `json:"attempt"`
		Restored int    `json:"restored"`
	}
	bytes, err := os.ReadFile(out)
	if err != nil || json.Unmarshal(bytes, &answer) != nil || answer.Job != j.ID || answer.Attempt != 2 || answer.Restored != 2 {
		t.Fatal("accepted artifact does not prove resume", string(bytes), err)
	}
	replay, err := c.Submit(ctx, s)
	if err != nil || replay.ID != j.ID || replay.Attempt != 2 || replay.State != "succeeded" {
		t.Fatal("same-key identity changed", replay, err)
	}
}
