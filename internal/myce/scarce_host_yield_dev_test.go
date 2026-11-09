package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Real `myce dev` API/runner, simulated scarce GPU inventory, no native GPU or
// fleet qualification. File-store transport moves an atomic generation between
// two separate worker roots; the main process and a native sidecar must release.
func TestScarceHostYieldDev(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		python, err := exec.LookPath("python3")
		if err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		d, err := StartDev(DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "files"), Token: "local-fixture", Workers: 1, Devices: 2, HostID: "scarce", CPUBudgetCores: 2, MemoryGB: 8, Interval: 20 * time.Millisecond})
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
		w := Worker{EnvironmentLimit: 1 << 20, sharedCPUDev: true, CPUIsolationProbe: func(context.Context) error { return nil }, Machine: general, StateDir: filepath.Join(root, "general"), Client: c, ModelConfig: d.Queue.models, Clock: d.Queue.clock, Interval: 20 * time.Millisecond, HostMemorySource: func(context.Context) (float64, error) { return 8, nil }}
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

	})

	t.Run("ScarceHostYieldProjectionKeepsDemand", func(t *testing.T) {
		q, clock, _, _, victim := yieldFixture(t, false)
		s := spec("experiment")
		s.Resumable = false
		s.Needs.CPUCores, s.Needs.HostMemoryGB = 1, 1
		s.Needs.Platform = Platform{Required: []string{"fake"}}
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
		var target *Machine
		for _, m := range q.plannerMachines() {
			if m.GPU == "fake" {
				target = m
				break
			}
		}
		if target == nil {
			t.Fatal("missing scarce target")
		}
		set := &candidateSet{q: q, m: target, resources: map[batchResourceKey]batchResourceRoom{}}
		// A portable demand can populate the ordinary projection first. It must
		// neither suppress later yields nor donate another demand's stop authority.
		portable := &Job{ID: "portable-demand", State: "queued", Spec: s, Queued: clock.Now()}
		q.batchResourcesFitWithScan(portable, target, set.resources)
		for _, id := range []string{"demand-a", "demand-b"} {
			j := *portable
			j.ID = id
			j.Spec.RequirementReasons = map[string]string{"backend:fake": "requires simulated accelerator"}
			if !q.batchResourcesFitWithScan(&j, target, set.resources) {
				t.Fatal("yield demand lost admission", id)
			}
			p := set.batchPlan(&j)
			if p == nil || p.yields[victim.ID] == nil || p.yields[victim.ID].Demand != id {
				t.Fatal("yield plan reused a different demand", id, p)
			}
		}

	})

	t.Run("ScarceHostYieldLifecycle", func(t *testing.T) {
		q, clock, a, b, run := yieldFixture(t, false)
		demand := yieldDemand(t, q)
		stopped := job(t, q, run.ID)
		if stopped.State != "stopping" || stopped.ScarceHostYield == nil || priorityStop(stopped).Cause != scarceHostYieldCause {
			t.Fatal("yield missing", stopped.State, stopped.Reason)
		}
		authorizeYield(t, q, run)
		// A deadline, reported exit, or release-only receipt cannot qualify migration.
		clock.Advance(StopGrace)
		machineCheck(t, q, b)
		released := result(run, false)
		released.ReleaseOnly = true
		if err := q.Complete(released); err != nil {
			t.Fatal(err)
		}
		if got := assignedJob(machineCheck(t, q, b)); got != nil {
			t.Fatal("release alone resumed victim", got)
		}
		saveYieldGeneration(t, job(t, q, run.ID), 2)
		r := result(run, false)
		r.Interrupted = true
		g := int64(2)
		r.FinalCheckpointGeneration = &g
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		next := assignedJob(machineCheck(t, q, b))
		if next == nil || next.ID != run.ID || next.Attempt != 2 || next.Spec.SubmissionKey != "portable-counter" || next.InfrastructureRetries != 0 || next.Losses != 0 || next.PreemptionCount != 1 {
			t.Fatal("identity/accounting/resume", next, job(t, q, run.ID).Reason)
		}
		// New attempt cannot be qualified or published by the prior execution.
		staleResult := result(run, true)
		if err := q.Complete(staleResult); !errors.Is(err, ErrStale) {
			t.Fatal("stale result accepted", err)
		}
		if got := job(t, q, run.ID); got.State != "running" || got.Attempt != 2 {
			t.Fatal("stale report changed new work", got)
		}
		hard := assignedJob(machineCheck(t, q, a))
		if hard == nil || hard.ID != demand.ID {
			t.Fatal("demand did not get released host", hard, job(t, q, demand.ID).Reason)
		}

	})

	t.Run("ScarceHostYieldRevalidateBeforeStop", func(t *testing.T) {
		for _, change := range []string{"destination", "cancel", "demand-maintenance"} {
			t.Run(change, func(t *testing.T) {
				q, _, a, b, run := yieldFixture(t, false)
				demand := yieldDemand(t, q)
				if change == "cancel" {
					if err := q.Cancel(demand.ID); err != nil {
						t.Fatal(err)
					}
				} else if change == "destination" {
					if err := q.Leave(b.ID, false); err != nil {
						t.Fatal(err)
					}
				} else if err := q.Leave(a.ID, false); err != nil {
					t.Fatal(err)
				}
				ok, err := q.authorizeYieldStop(context.Background(), result(run, false))
				if err != nil || ok {
					t.Fatal("invalid destination/demand authorized", ok, err)
				}
				got := job(t, q, run.ID)
				if got.State != "running" || got.ScarceHostYield != nil || got.PreemptionCount != 0 {
					t.Fatal("undelivered stop not withdrawn", got)
				}
			})
		}

	})

	t.Run("ScarceHostYieldFailedFinalSaveHolds", func(t *testing.T) {
		for _, forced := range []bool{false, true} {
			t.Run(fmt.Sprint(forced), func(t *testing.T) {
				q, _, _, b, run := yieldFixture(t, false)
				yieldDemand(t, q)
				authorizeYield(t, q, run)
				if forced {
					saveYieldGeneration(t, job(t, q, run.ID), 2)
				}
				g := int64(1)
				if forced {
					g = 2
				}
				r := result(run, false)
				r.Interrupted = true
				r.Forced = forced
				r.FinalCheckpointGeneration = &g
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, run.ID)
				if got.State != "queued" || got.ScarceHostYield.Hold == "" || got.InfrastructureRetries != 0 || got.Losses != 0 {
					t.Fatal("failed final save not held", got)
				}
				if next := assignedJob(machineCheck(t, q, b)); next != nil {
					t.Fatal("held progress restarted", next)
				}
				data, err := os.ReadFile(filepath.Join(q.root, run.ID, fmt.Sprintf("attempt-%d", run.Attempt), "save.json"))
				if err != nil || len(data) == 0 {
					t.Fatal("committed save lost", err)
				}
			})
		}

	})

	t.Run("ScarceHostYieldDisposableExperiment", func(t *testing.T) {
		q, _, a, b, run := yieldFixture(t, true)
		demand := yieldDemand(t, q)
		authorizeYield(t, q, run)
		if got := assignedJob(machineCheck(t, q, a, run)); got != nil && got.ID == demand.ID {
			t.Fatal("demand started before release")
		}
		finishInterrupted(t, q, run)
		next := assignedJob(machineCheck(t, q, b))
		if next == nil || next.ID != run.ID || next.Attempt != 2 || next.Losses != 0 {
			t.Fatal("disposable restart", next)
		}

	})

	t.Run("ScarceHostYieldProductionProtectedUnderOwnerTiers", func(t *testing.T) {
		q, _, a, _, run := yieldFixture(t, false)
		// Same owner tier permits same-tier relocation, but cannot give an
		// experiment permission to displace a production attempt.
		policy := PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "shared", Selectors: []string{"*/*"}}}}
		if _, err := q.SetPriorityPolicy(0, policy); err != nil {
			t.Fatal(err)
		}
		if err := q.update(func() error {
			v := q.touchJob(run.ID)
			v.Spec.Class, v.Spec.Priority = "production", "production"
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		yieldDemand(t, q)
		machineCheck(t, q, a, run)
		if got := job(t, q, run.ID); got.State != "running" || got.ScarceHostYield != nil {
			t.Fatal("experiment displaced production under owner policy", got)
		}

	})

	t.Run("ScarceHostYieldAuthorContract", func(t *testing.T) {
		q, _ := fixture(t)
		s := cpuSpec(1, 1)
		s.Resumable = false
		s.Preemptible = true
		s.ResumeCompatibility = yieldCPUContract()
		s.SubmissionKey = "disposable-contract"
		accepted := submit(t, q, s)
		changed := s
		changed.ResumeCompatibility = &ResumeCompatibility{CheckpointFormat: "counter-v2", ResultContract: "counter-result-v1", Targets: s.ResumeCompatibility.Targets}
		if _, err := q.Submit(changed); err == nil {
			t.Fatal("same key changed frozen resume contract")
		}
		if got := job(t, q, accepted.ID); got.Spec.ResumeCompatibility.CheckpointFormat != "counter-v1" || !got.Spec.Preemptible {
			t.Fatal("contract not frozen", got.Spec)
		}
	})

	t.Run("ScarceHostYieldDestinationLostAfterStopAndDemandCancel", func(t *testing.T) {
		q, _, a, b, run := yieldFixture(t, false)
		demand := yieldDemand(t, q)
		authorizeYield(t, q, run)
		if err := q.Leave(b.ID, false); err != nil {
			t.Fatal(err)
		}
		saveYieldGeneration(t, job(t, q, run.ID), 2)
		r := result(run, false)
		r.Interrupted = true
		g := int64(2)
		r.FinalCheckpointGeneration = &g
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if got := assignedJob(machineCheck(t, q, a)); got == nil || got.ID != demand.ID {
			t.Fatal("demand missing", got)
		}
		got := job(t, q, run.ID)
		if got.State != "queued" || !strings.Contains(got.Reason, "scarce_host_yield") || got.ScarceHostYield.Generation != 2 {
			t.Fatal("durable waiting progress lost", got)
		}
		if err := q.Cancel(demand.ID); err != nil {
			t.Fatal(err)
		}
		// Delivered stop still owns cleanup, even though routing exclusion ended.
		if next := assignedJob(machineCheck(t, q, a)); next != nil {
			t.Fatal("canceled demand released without proof", next)
		}
		stopped := job(t, q, demand.ID)
		rr := result(stopped, false)
		rr.ReleaseOnly = true
		if err := q.Complete(rr); err != nil {
			t.Fatal(err)
		}
		next := assignedJob(machineCheck(t, q, a))
		if next == nil || next.ID != run.ID {
			t.Fatal("cancellation did not clear scarce exclusion", next, job(t, q, run.ID).Reason)
		}
		if err := q.Started(result(next, false)); err != nil {
			t.Fatal("CPU resume on accelerator-equipped host failed launch fence", err)
		}

	})

	t.Run("ScarceHostYieldCleanupPendingRetainsFinalReceipt", func(t *testing.T) {
		q, _, _, b, run := yieldFixture(t, false)
		yieldDemand(t, q)
		authorizeYield(t, q, run)
		saveYieldGeneration(t, job(t, q, run.ID), 2)
		r := result(run, false)
		r.Interrupted, r.ReleasePending = true, true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, run.ID)
		if got.State != "stopping" || got.ScarceHostYield.FinalReceived || got.Machine != run.Machine {
			t.Fatal("cleanup-pending result lost final-receipt custody", got)
		}
		r.ReleasePending, r.ReleaseOnly = false, true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		g := int64(2)
		r.ReleaseOnly = false
		r.FinalCheckpointGeneration = &g
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		next := assignedJob(machineCheck(t, q, b))
		if next == nil || next.ID != run.ID || next.Attempt != 2 || next.ScarceHostYield.Hold != "" {
			t.Fatal("cleanup retry did not accept final save", next, job(t, q, run.ID).Reason)
		}

	})

	t.Run("ScarceHostYieldDisposableKeepsFrozenCompatibility", func(t *testing.T) {
		q, _, _, b, run := yieldFixture(t, true)
		yieldDemand(t, q)
		authorizeYield(t, q, run)
		if err := q.Leave(b.ID, false); err != nil {
			t.Fatal(err)
		}
		finishInterrupted(t, q, run)
		other := cpuMachine("other", "other", "cpu", 8, 32)
		other.Devices = []MachineDevice{}
		other.CapabilityProfile.Arch = "arm64"
		other.Capabilities = append(other.Capabilities, scarceHostYieldCapability)
		if next := assignedJob(machineCheck(t, q, other)); next != nil {
			t.Fatal("disposable work used unproven result target", next)
		}
		got := job(t, q, run.ID)
		if got.State != "queued" || !strings.Contains(got.Reason, scarceHostYieldCause) {
			t.Fatal("missing compatibility hold", got)
		}

	})

	t.Run("ScarceHostYieldPendingNASVerificationRetries", func(t *testing.T) {
		q, _, _, b, run := yieldFixture(t, false)
		yieldDemand(t, q)
		authorizeYield(t, q, run)
		path := attemptCheckpoint(*job(t, q, run.ID))
		entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		q.nasProgress.timeout = time.Millisecond
		_, err := q.nasProgress.run(path, func() (int64, error) {
			close(entered)
			<-release
			close(finished)
			return 1, nil // Heartbeat began before the cooperative final save.
		})
		if !errors.Is(err, errCheckpointIOPending) {
			t.Fatal("missing pending scan", err)
		}
		<-entered
		saveYieldGeneration(t, job(t, q, run.ID), 2)
		g := int64(2)
		r := result(run, false)
		r.Interrupted, r.FinalCheckpointGeneration = true, &g
		if err := q.Complete(r); !errors.Is(err, errCheckpointIOPending) {
			t.Fatal("unfinished verification acknowledged as final", err)
		}
		if got := job(t, q, run.ID); got.ScarceHostYield.FinalReceived || got.State != "stopping" {
			t.Fatal("pending verification consumed final receipt", got)
		}
		unblock()
		<-finished
		q.nasProgress.timeout = testWaitTimeout
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		next := assignedJob(machineCheck(t, q, b))
		if next == nil || next.ID != run.ID || next.Attempt != 2 {
			t.Fatal("fresh verification did not resume", next, job(t, q, run.ID).Reason)
		}

	})

	t.Run("ScarceHostYieldCancelledFinalVerificationKeepsReceipt", func(t *testing.T) {
		q, _, _, b, run := yieldFixture(t, false)
		yieldDemand(t, q)
		authorizeYield(t, q, run)
		saveYieldGeneration(t, job(t, q, run.ID), 2)
		path := attemptCheckpoint(*job(t, q, run.ID))
		key := fmt.Sprintf("%s:yield-final:2", path)
		entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		q.nasProgress.timeout = 5 * time.Millisecond
		_, err := q.nasProgress.run(key, func() (int64, error) {
			close(entered)
			<-release
			close(finished)
			return 2, nil
		})
		if !errors.Is(err, errCheckpointIOPending) {
			t.Fatal(err)
		}
		<-entered
		g := int64(2)
		r := result(run, false)
		r.Interrupted, r.FinalCheckpointGeneration = true, &g
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := q.CompleteContext(ctx, r); !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled receipt was acknowledged", err)
		}
		if got := job(t, q, run.ID); got.State != "stopping" || got.ScarceHostYield.FinalReceived {
			t.Fatal("cancellation consumed receipt", got)
		}
		// Slow healthy verification retains late success for this exact generation;
		// the retry uses the same short I/O budget, never a widened deadline.
		if err := q.Complete(r); !errors.Is(err, errCheckpointIOPending) {
			t.Fatal("slow scan did not remain pending", err)
		}
		unblock()
		<-finished
		if err := q.Complete(r); err != nil {
			t.Fatal("late verification was not reusable", err)
		}
		next := assignedJob(machineCheck(t, q, b))
		if next == nil || next.ID != run.ID || next.Attempt != 2 {
			t.Fatal("valid retry could not resume", next, job(t, q, run.ID).Reason)
		}

	})

	t.Run("ScarceHostYieldDestinationProtocolRechecked", func(t *testing.T) {
		for _, fence := range []string{"dispatch", "launch"} {
			t.Run(fence, func(t *testing.T) {
				q, _, _, b, run := yieldFixture(t, false)
				yieldDemand(t, q)
				authorizeYield(t, q, run)
				saveYieldGeneration(t, job(t, q, run.ID), 2)
				g := int64(2)
				r := result(run, false)
				r.Interrupted, r.FinalCheckpointGeneration = true, &g
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				if fence == "dispatch" {
					b.Capabilities = []string{"native_isolation_landlock"}
					if next := assignedJob(machineCheck(t, q, b)); next != nil {
						t.Fatal("older worker accepted yielded work", next)
					}
				} else {
					next := assignedJob(machineCheck(t, q, b))
					if next == nil || next.ID != run.ID {
						t.Fatal("missing resume offer", next)
					}
					if err := q.update(func() error { q.s.Machines[b.ID].Capabilities = []string{"native_isolation_landlock"}; return nil }); err != nil {
						t.Fatal(err)
					}
					if err := q.Started(result(next, false)); !errors.Is(err, ErrStale) {
						t.Fatal("old worker passed yielded launch fence", err)
					}
				}
			})
		}

	})

	t.Run("ScarceHostYieldRechecksDemandToolsBeforeStop", func(t *testing.T) {
		q, _, a, _, run := yieldFixture(t, false)
		if err := q.update(func() error { q.s.Machines[a.ID].ToolInventory = map[string][]string{"": {"node"}}; return nil }); err != nil {
			t.Fatal(err)
		}
		s := spec("experiment")
		s.Needs.Tools = []string{"node"}
		s.Needs.Platform = Platform{Required: []string{"fake"}}
		s.RequirementReasons = map[string]string{"backend:fake": "requires simulated accelerator"}
		submit(t, q, s)
		if got := job(t, q, run.ID); got.State != "stopping" {
			t.Fatal("missing initial yield", got.State, got.Reason)
		}
		if err := q.update(func() error { q.s.Machines[a.ID].ToolInventory = nil; return nil }); err != nil {
			t.Fatal(err)
		}
		if ok, err := q.authorizeYieldStop(context.Background(), result(run, false)); ok || err != nil {
			t.Fatal("missing demand tools still authorized stop", ok, err)
		}
		if got := job(t, q, run.ID); got.State != "running" {
			t.Fatal("undelivered stop not withdrawn", got)
		}

	})

	t.Run("ScarceHostYieldRestartAndStalePublication", func(t *testing.T) {
		q, clock, a, b, run := yieldFixtureWithStore(t, false, true)
		demand := yieldDemand(t, q)
		authorizeYield(t, q, run)
		deadline := *job(t, q, run.ID).StopAt
		path, root, storeRoot := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root, q.files.root
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		var err error
		q, err = Open(path, root, clock)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		fakeCheckpointAvailability(t, q, true)
		if err := q.ConfigureFileStore(storeRoot, 1<<20, true); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, run.ID)
		if got.ScarceHostYield == nil || !got.ScarceHostYield.Authorized || !got.StopAt.Equal(deadline) || priorityStop(got).Cause != scarceHostYieldCause || got.ScarceHostYield.Demand != demand.ID {
			t.Fatal("restart lost exact yield intent", got)
		}
		machineCheck(t, q, a, run)
		machineCheck(t, q, b)
		p := yieldPublication(t, q, run, 2)
		if err := q.publishCheckpoint(context.Background(), p); err != nil {
			t.Fatal(err)
		}
		r := result(run, false)
		r.Interrupted = true
		r.FinalCheckpointGeneration = &p.Checkpoint.Generation
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		next := assignedJob(machineCheck(t, q, b))
		if next == nil || next.ID != run.ID || next.Attempt != 2 || next.InfrastructureRetries != 0 {
			t.Fatal("restart failed to resume", next, job(t, q, run.ID).Reason)
		}
		before := job(t, q, run.ID)
		if err := q.publishCheckpoint(context.Background(), yieldPublication(t, q, run, 3)); !errors.Is(err, ErrStale) {
			t.Fatal("old execution published", err)
		}
		staleRelease := result(run, false)
		staleRelease.ReleaseOnly = true
		_ = q.Complete(staleRelease) // An idempotent old release may be acknowledged.
		if got := job(t, q, run.ID); got.Attempt != 2 || got.ReleaseConfirmed || got.YieldGeneration != 0 || got.FileCheckpoints[1].Generation != before.FileCheckpoints[1].Generation {
			t.Fatal("stale report changed current custody/progress", got)
		}

	})

	t.Run("ScarceHostYieldCompletionBeforeDelivery", func(t *testing.T) {
		q, _, _, _, run := yieldFixture(t, false)
		yieldDemand(t, q)
		if err := q.Complete(result(run, true)); err != nil {
			t.Fatal(err)
		}
		if got := job(t, q, run.ID); got.State != "succeeded" || got.Attempt != 1 || got.Losses != 0 {
			t.Fatal("planned stop defeated completed work", got)
		}
		if ok, err := q.authorizeYieldStop(context.Background(), result(run, false)); ok || !errors.Is(err, ErrStale) {
			t.Fatal("terminal execution authorized stop", ok, err)
		}

	})

	t.Run("ScarceHostYieldTransportFailureAndVerificationFence", func(t *testing.T) {
		q, clock, _, b, run := yieldFixtureWithStore(t, false, true)
		demand := yieldDemand(t, q)
		authorizeYield(t, q, run)
		p := yieldPublication(t, q, run, 2)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		q.files.syncDir = func(string) error { close(entered); <-release; return errors.New("final transport sync failed") }
		done := make(chan error, 1)
		go func() { done <- q.publishCheckpoint(context.Background(), p) }()
		select {
		case <-entered:
		case <-time.After(testWaitTimeout):
			t.Fatal("verification did not begin")
		}
		// An unrelated control transaction and exact release must proceed while
		// final transport is blocked. Neither a timer nor release qualifies resume.
		progress := make(chan error, 1)
		go func() { progress <- q.Cancel(demand.ID) }()
		select {
		case err := <-progress:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(testWaitTimeout):
			t.Fatal("transport held q.mu")
		}
		unblock()
		if err := <-done; err == nil {
			t.Fatal("failed transport accepted")
		}
		r := result(run, false)
		r.ReleaseOnly = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		clock.Advance(StopGrace)
		machineCheck(t, q, b)
		got := job(t, q, run.ID)
		if got.ScarceHostYield.Hold == "" || got.FileCheckpoints[1].Generation != 1 || got.Attempt != 1 || got.Losses != 0 {
			t.Fatal("failed transport restarted or lost progress", got)
		}

	})

	t.Run("ScarceHostYieldWorkerRestartReceipt", func(t *testing.T) {
		w, store, p := d5NativeStart(t, true, nil, []string{"sh", "-c", `trap 'exit 0' TERM; touch "$1"; while :; do sleep .01; done`, "main", "{work}/ready"})
		nativeFile(t, p.job.Spec.Run.Workdir, "ready")
		intent := &PriorityPreemption{Cause: scarceHostYieldCause, Demand: "scarce-demand", Victim: p.job.ID}
		if err := p.recordPriorityStop(store, intent); err != nil {
			t.Fatal(err)
		}
		p.stop(w.Clock.Now(), time.Minute, false)
		r := d5Finish(t, p)
		if !r.Interrupted || !scarceYieldStop(r.PriorityStop) {
			t.Fatal("yield stop provenance lost", r)
		}
		reopened := &workerStore{dir: store.dir, state: store.state}
		if err := reopened.restorePriorityStop(); err != nil {
			t.Fatal(err)
		}
		if !scarceYieldStop(reopened.state.PriorityStop) || reopened.state.PriorityStop.Demand != intent.Demand {
			t.Fatal("worker restart lost yield custody")
		}

	})

	t.Run("DifferentWorkerCheckpointRoots", func(t *testing.T) {
		q, _ := fixture(t)
		shared := q.root
		aliases := []string{filepath.Join(t.TempDir(), "linux-nas"), filepath.Join(t.TempDir(), "mac-nas")}
		for _, a := range aliases {
			if e := os.Symlink(shared, a); e != nil {
				t.Fatal(e)
			}
		}
		j := submit(t, q, spec("experiment"))
		first := check(t, q, machine("linux", "fake"), nil).Job
		if filepath.IsAbs(first.Checkpoint) {
			t.Fatal("assignment leaked queue mount path")
		}
		first.Checkpoint = filepath.Join(aliases[0], first.Checkpoint)
		dir, e := prepareCheckpoint(*first)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(dir, "saved"), []byte("progress"), 0600); e != nil {
			t.Fatal(e)
		}
		if e = q.Leave("linux", true); e != nil {
			t.Fatal(e)
		}
		r := result(first, false)
		r.Interrupted = true
		if e = q.Complete(r); e != nil {
			t.Fatal(e)
		}
		second := check(t, q, machine("mac", "fake"), nil).Job
		second.Checkpoint = filepath.Join(aliases[1], second.Checkpoint)
		dir, e = prepareCheckpoint(*second)
		if e != nil {
			t.Fatal(e)
		}
		data, e := os.ReadFile(filepath.Join(dir, "saved"))
		if e != nil || string(data) != "progress" {
			t.Fatalf("%s lost progress: %q %v", j.ID, data, e)
		}

	})
}
