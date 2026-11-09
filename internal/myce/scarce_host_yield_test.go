package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func yieldCPUContract() *ResumeCompatibility {
	return &ResumeCompatibility{CheckpointFormat: "counter-v1", ResultContract: "counter-result-v1", Targets: []ResumeTarget{{Target: ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}, Environment: "none", ResultCheck: "counter-resume-test"}}}
}
func saveYieldGeneration(t *testing.T, j *Job, g int64) {
	t.Helper()
	dir := attemptCheckpoint(*j)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data := fmt.Sprintf(`{"myce_checkpoint":{"generation":%d},"value":{"step":%d}}`, g, g)
	if err := os.WriteFile(filepath.Join(dir, "save.json"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
func yieldFixture(t *testing.T, disposable bool) (*Queue, *fakeClock, Machine, Machine, *Job) {
	return yieldFixtureWithStore(t, disposable, false)
}
func yieldFixtureWithStore(t *testing.T, disposable, files bool) (*Queue, *fakeClock, Machine, Machine, *Job) {
	t.Helper()
	q, clock := fixture(t)
	if files {
		if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
			t.Fatal(err)
		}
	}
	a := m3Machine()
	a.Capabilities = append(a.Capabilities, scarceHostYieldCapability)
	a.ID = "scarce"
	a.HostID = "scarce"
	s := cpuSpec(8, 1)
	s.Resumable = !disposable
	s.Preemptible = disposable
	s.ResumeCompatibility = yieldCPUContract()
	s.SubmissionKey = "portable-counter"
	j := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, a))
	if run == nil || run.ID != j.ID {
		t.Fatal("missing portable attempt", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	if !disposable {
		if files {
			if err := q.publishCheckpoint(context.Background(), yieldPublication(t, q, run, 1)); err != nil {
				t.Fatal(err)
			}
		} else {
			saveYieldGeneration(t, job(t, q, j.ID), 1)
		}
		machineCheck(t, q, a, run)
	}
	b := cpuMachine("general", "general", "cpu", 8, 32)
	b.Devices = []MachineDevice{}
	b.Capabilities = append(b.Capabilities, scarceHostYieldCapability)
	machineCheck(t, q, b)
	return q, clock, a, b, run
}
func yieldDemand(t *testing.T, q *Queue) *Job {
	t.Helper()
	s := spec("experiment")
	s.Resumable = false
	s.Needs.CPUCores = 1
	s.Needs.HostMemoryGB = 1
	s.Needs.Platform = Platform{Required: []string{"fake"}}
	s.RequirementReasons = map[string]string{"backend:fake": "requires the simulated accelerator"}
	return submit(t, q, s)
}
func authorizeYield(t *testing.T, q *Queue, j *Job) {
	t.Helper()
	ok, err := q.authorizeYieldStop(context.Background(), result(j, false))
	if err != nil || !ok {
		t.Fatal("stop not authorized", ok, err, job(t, q, j.ID).Reason)
	}
}
func TestScarceHostYieldLifecycle(t *testing.T) {
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
}
func TestScarceHostYieldFailedFinalSaveHolds(t *testing.T) {
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
}
func TestScarceHostYieldDisposableExperiment(t *testing.T) {
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
}
func TestScarceHostYieldRevalidateBeforeStop(t *testing.T) {
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
}

func TestScarceHostYieldProductionProtectedUnderOwnerTiers(t *testing.T) {
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
}

func TestScarceHostYieldAuthorContract(t *testing.T) {
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
	for _, bad := range []Spec{
		{Priority: "production", Preemptible: true},
		{Class: "test", Preemptible: true},
		{Resumable: true, Preemptible: true},
		{Resumable: true, ResumeCompatibility: &ResumeCompatibility{CheckpointFormat: "counter-v1"}},
	} {
		if err := validateYieldContract(bad); err == nil {
			t.Fatal("invalid contract accepted", bad)
		}
	}
}
func TestScarceHostYieldDestinationLostAfterStopAndDemandCancel(t *testing.T) {
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
}

func TestScarceHostYieldCleanupPendingRetainsFinalReceipt(t *testing.T) {
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
}

func TestScarceHostYieldDisposableKeepsFrozenCompatibility(t *testing.T) {
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
}

func TestScarceHostYieldPendingNASVerificationRetries(t *testing.T) {
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
}

func TestScarceHostYieldCancelledFinalVerificationKeepsReceipt(t *testing.T) {
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
}

func TestScarceHostYieldConvenienceDevicePinIsNotUniqueness(t *testing.T) {
	q, _, _, _, run := yieldFixture(t, false)
	peer := m3Machine()
	peer.ID, peer.HostID = "peer", "peer"
	peer.Devices[0].ID, peer.Devices[1].ID = "2", "3"
	machineCheck(t, q, peer)
	s := spec("experiment")
	s.Resumable = false
	s.Needs.CPUCores, s.Needs.HostMemoryGB = 1, 1
	s.Needs.Platform = Platform{Required: []string{"fake"}}
	s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	s.RequirementReasons = map[string]string{"backend:fake": "needs simulated accelerator"}
	d := submit(t, q, s)
	if got := job(t, q, run.ID); got.State != "running" {
		t.Fatal("convenience pin manufactured uniqueness", got)
	}
	if got := job(t, q, d.ID); got.Spec.Needs.GPU.Devices[0] != "0" {
		t.Fatal("projection changed authored pin", got.Spec.Needs.GPU)
	}
}

func TestScarceHostYieldToolsDoNotManufactureUniqueness(t *testing.T) {
	q, _, a, _, run := yieldFixture(t, false)
	peer := m3Machine()
	peer.ID, peer.HostID = "peer", "peer"
	machineCheck(t, q, peer)
	if err := q.update(func() error { q.s.Machines[a.ID].ToolInventory = map[string][]string{"": {"node"}}; return nil }); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Needs.Tools = []string{"node"}
	s.Needs.Platform = Platform{Required: []string{"fake"}}
	s.RequirementReasons = map[string]string{"backend:fake": "requires simulated accelerator"}
	submit(t, q, s)
	if got := job(t, q, run.ID); got.State != "running" {
		t.Fatal("missing peer software manufactured uniqueness", got)
	}
}

func TestScarceHostYieldWorkerProtocolsAreNotHardwareUniqueness(t *testing.T) {
	for _, protocol := range []string{"shared-input", "model-snapshot"} {
		t.Run(protocol, func(t *testing.T) {
			q, _, a, _, _ := yieldFixture(t, false)
			peer := m3Machine()
			peer.ID, peer.HostID = "peer", "peer"
			machineCheck(t, q, peer)
			if err := q.update(func() error {
				q.s.Machines[a.ID].Capabilities = append(q.s.Machines[a.ID].Capabilities, "shared_inputs_v1", modelSnapshotsCapability)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			s := spec("experiment")
			s.Needs.Platform = Platform{Required: []string{"fake"}}
			s.RequirementReasons = map[string]string{"backend:fake": "requires simulated accelerator"}
			if protocol == "shared-input" {
				s.SharedInputs = map[string]SharedInput{"data": {SHA256: strings.Repeat("a", 64), SizeBytes: 1, Manifest: FileManifest{"data": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}}}
			} else {
				s.ModelSnapshots = map[string]ModelSnapshot{"base": {Repo: "example/model", Revision: strings.Repeat("a", 40)}}
			}
			// Read-only hardware projection: input acquisition/admission still
			// rejects this peer's older worker protocol in the ordinary planner.
			host, _ := q.scarceDemandHost(&Job{State: "queued", Spec: s})
			if host != "" {
				t.Fatal("worker software manufactured unique hardware", protocol, host)
			}
		})
	}
}

func TestScarceHostYieldDestinationProtocolRechecked(t *testing.T) {
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
}

func TestScarceHostYieldRechecksDemandToolsBeforeStop(t *testing.T) {
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
}
func TestScarceHostYieldRequiresRealUniquenessAndCompatibility(t *testing.T) {
	for _, gap := range []string{"busy-peer", "stale-peer", "maintained-peer", "unavailable", "unqualified", "incompatible", "no-checkpoint", "production", "not-opted-in", "protected"} {
		t.Run(gap, func(t *testing.T) {
			q, clock, a, b, run := yieldFixture(t, false)
			switch gap {
			case "busy-peer", "stale-peer", "maintained-peer":
				peer := m3Machine()
				peer.ID = "peer"
				peer.HostID = "peer"
				machineCheck(t, q, peer)
				if gap == "busy-peer" {
					s := cpuSpec(8, 1)
					s.AllowedMachines = []string{"peer"}
					j := submit(t, q, s)
					machineCheck(t, q, peer)
					_ = j
				}
				if gap == "stale-peer" {
					clock.Advance(LossAfter)
					machineCheck(t, q, a, run)
					machineCheck(t, q, b)
				}
				if gap == "maintained-peer" {
					if err := q.Leave(peer.ID, false); err != nil {
						t.Fatal(err)
					}
				}
			case "unavailable":
				if err := q.Leave(b.ID, false); err != nil {
					t.Fatal(err)
				}
			case "unqualified":
				// A declared supported target still needs its frozen environment
				// prepared on the destination. Source evidence alone is insufficient.
				if err := q.update(func() error {
					v := q.touchJob(run.ID)
					v.Spec.Environment = Environment{Name: "counter-runtime"}
					v.EnvironmentRecipes = map[string]EnvironmentRecipe{"cpu": {Key: "counter-runtime-v1"}}
					r := *v.Spec.ResumeCompatibility
					r.Targets = append([]ResumeTarget(nil), r.Targets...)
					r.Targets[0].Environment = "counter-runtime-v1"
					v.Spec.ResumeCompatibility = &r
					q.s.Machines[a.ID].Environments = []string{"counter-runtime-v1"}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "incompatible":
				if err := q.update(func() error {
					profile := *q.s.Machines[b.ID].CapabilityProfile
					profile.Arch = "arm64"
					q.s.Machines[b.ID].CapabilityProfile = &profile
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "no-checkpoint":
				if err := q.update(func() error { q.touchJob(run.ID).YieldGeneration = 0; return nil }); err != nil {
					t.Fatal(err)
				}
			case "production":
				if err := q.update(func() error {
					v := q.touchJob(run.ID)
					v.Spec.Priority = "production"
					v.Spec.Class = "production"
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "not-opted-in":
				if err := q.update(func() error {
					v := q.touchJob(run.ID)
					v.Spec.Resumable = false
					v.Spec.Preemptible = false
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "protected":
				if err := q.update(func() error { q.touchJob(run.ID).PreemptionCount = 2; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			yieldDemand(t, q)
			if got := job(t, q, run.ID); got.State != "running" {
				t.Fatal("unsafe yield", gap, got.State, got.Reason)
			}
		})
	}
}

func yieldPublication(t *testing.T, q *Queue, run *Job, generation int64) checkpointPublication {
	t.Helper()
	data := []byte(fmt.Sprintf(`{"myce_checkpoint":{"generation":%d},"value":%d}`, generation, generation))
	path := filepath.Join(t.TempDir(), "save.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
		t.Fatal(err)
	}
	return checkpointPublication{Result: result(run, false), Checkpoint: FileCheckpoint{Generation: generation, Files: FileManifest{"save.json": ref}}}
}

func TestScarceHostYieldRestartAndStalePublication(t *testing.T) {
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
}

func TestScarceHostYieldCompletionBeforeDelivery(t *testing.T) {
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
}

func TestScarceHostYieldTransportFailureAndVerificationFence(t *testing.T) {
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
}

func TestScarceHostYieldWorkerRestartReceipt(t *testing.T) {
	for _, when := range []string{"before-stop", "during-cleanup"} {
		t.Run(when, func(t *testing.T) {
			var p *process
			var intent *PriorityPreemption
			var armed atomic.Bool
			var once sync.Once
			var receiptErr error
			fs := &memberMemoryFS{beforeRemove: func(string) {
				if armed.Load() {
					// Removal is after the real main's selected exit snapshot,
					// but before result classification. Model control delivery
					// during cleanup without a scheduler delay or a runtime hook.
					once.Do(func() { receiptErr = p.recordPriorityStop(nil, intent) })
				}
			}}
			w, store, started := d5NativeStart(t, true, fs, []string{"sh", "-c", `trap 'exit 0' TERM; touch "$1"; while :; do sleep .01; done`, "main", "{work}/ready"})
			p = started
			nativeFile(t, p.job.Spec.Run.Workdir, "ready")
			intent = &PriorityPreemption{Cause: scarceHostYieldCause, Demand: "scarce-demand", Victim: p.job.ID}
			if when == "before-stop" {
				if err := p.recordPriorityStop(store, intent); err != nil {
					t.Fatal(err)
				}
			} else {
				armed.Store(true)
			}
			p.stop(w.Clock.Now(), time.Minute, false)
			r := d5Finish(t, p)
			if receiptErr != nil {
				t.Fatal(receiptErr)
			}
			if r.Success || !r.Interrupted || !scarceYieldStop(r.PriorityStop) || r.PriorityStop.Demand != intent.Demand {
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
	}
}
