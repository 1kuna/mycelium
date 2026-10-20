package myce

import (
	"context"
	"io"
	"math"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func cpuSpec(cores int, ram float64) Spec {
	return Spec{Name: "cpu", Project: "cpu-project", Priority: "experiment", Run: RunSpec{Command: []string{"echo", "done"}}, Needs: ResourceNeeds{CPUCores: cores, MemoryGB: ram}}
}
func cpuMachine(id, host, backend string, cores int, ram float64) Machine {
	m := machine(id, backend, host)
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "linux", "amd64"
	m.CapabilityProfile.LogicalCPUs = cores
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, ram
	if backend == "cpu" {
		m.CapabilityProfile.Accelerators = nil
		m.FreeMemoryGB = 0
	}
	return m
}

func TestCPUResourceInputAndCanonicalRoundtrip(t *testing.T) {
	q, _ := fixture(t)
	var s Spec
	if err := yaml.Unmarshal([]byte("name: cpu\nproject: demo\nclass: experiment\nrun: {command: [echo, done]}\nneeds: {cpu_cores: 2, memory_gb: 3}\n"), &s); err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, s)
	if !j.Spec.Needs.cpuOnly() || j.Spec.Needs.MemoryGB != 0 || j.Spec.Needs.HostMemoryGB != 3 || j.Spec.Needs.CPUCores != 2 {
		t.Fatalf("canonical vector: %+v", j.Spec.Needs)
	}
	if err := j.Spec.Validate(); err != nil {
		t.Fatal("canonical revalidation", err)
	}
	for _, needs := range []ResourceNeeds{{CPUCores: -1, MemoryGB: 1}, {CPUCores: 1}, {CPUCores: 1, MemoryGB: math.NaN()}, {CPUCores: 1, MemoryGB: math.Inf(1)}, {CPUCores: 1, MemoryGB: 1, HostMemoryGB: -1}} {
		bad := cpuSpec(1, 1)
		bad.Needs = needs
		if err := bad.Validate(); err == nil {
			t.Fatalf("invalid resources accepted: %+v", needs)
		}
	}
	gpu := spec("experiment")
	gpu.Needs.CPUCores = 2
	saved := submit(t, q, gpu)
	if saved.Spec.Needs.cpuOnly() || saved.Spec.Needs.MemoryGB != 8 {
		t.Fatal("GPU compatibility changed")
	}
}

func TestCPUResourceMixedHostSharesOneLedger(t *testing.T) {
	q, _ := fixture(t)
	a, b := cpuMachine("gpu", "physical", "cuda", 8, 12), cpuMachine("cpu", "physical", "cpu", 8, 12)
	a.CPUReserveCores, b.CPUReserveCores = 2, 2
	a.CPUBudgetCores, b.CPUBudgetCores = 5, 5
	a.HostMemoryBudgetGB, b.HostMemoryBudgetGB = 10, 10
	check(t, q, a, nil)
	check(t, q, b, nil)
	gpu := spec("experiment")
	gpu.Needs.CPUCores, gpu.Needs.HostMemoryGB = 3, 4
	gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
	gj := submit(t, q, gpu)
	first := check(t, q, a, nil).Job
	if first == nil || first.ID != gj.ID {
		t.Fatal("GPU assignment", first)
	}
	cpu := submit(t, q, cpuSpec(2, 6))
	second := check(t, q, b, nil).Job
	if second == nil || second.ID != cpu.ID {
		t.Fatal("CPU assignment", second)
	}
	for _, m := range q.s.Machines {
		if m.EffectiveCPUCores != 5 || m.CPUInUseCores != 5 || m.FreeCPUCores != 0 || m.HostMemoryInUseGB != 10 || m.FreeHostMemoryGB != 0 {
			t.Fatalf("multiplied or lost host reservation: %+v", m)
		}
	}
	next := submit(t, q, cpuSpec(1, 1))
	if cpuFits(next, q.s.Machines[b.ID]) {
		t.Fatal("overbooked shared host")
	}
	// A release receipt and its still-current job describe one allocation.
	q.retainRelease(second)
	q.refreshHostMemory()
	if q.s.Machines[a.ID].CPUInUseCores != 5 || q.s.Machines[a.ID].HostMemoryInUseGB != 10 {
		t.Fatal("pending release counted twice")
	}
	// Requeue/loss does not imply process release, even across queue restart.
	second.State = "queued"
	q.refreshHostMemory()
	if q.s.Machines[a.ID].CPUInUseCores != 5 || q.s.Machines[a.ID].HostMemoryInUseGB != 10 {
		t.Fatal("pending custody disappeared")
	}
	var seq int
	var databaseName, databasePath string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &databaseName, &databasePath); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(databasePath, q.root, q.clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.refreshHostMemory()
	if reopened.s.Machines[a.ID].CPUInUseCores != 5 {
		t.Fatal("CPU release custody did not survive restart")
	}
}

func TestCPUAdmissionUnknownCapacityAndOwnerReserve(t *testing.T) {
	for _, tc := range []struct {
		name, os, reason       string
		cores, reserve, budget int
		ram                    bool
	}{
		{"unknown", "linux", "logical CPU capacity unavailable", 0, 0, 0, true},
		{"reserve", "linux", "1 cores free of 1 admitted", 4, 3, 0, true},
		{"allowance", "darwin", "1 cores free of 1 admitted", 8, 1, 1, true},
		{"unsupported", "windows", "CPU execution requires Linux or macOS", 8, 0, 0, true},
		{"RAM unknown", "linux", "RAM", 8, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			m := cpuMachine("cpu", "host", "cpu", tc.cores, 8)
			m.CapabilityProfile.OS, m.CPUReserveCores, m.CPUBudgetCores = tc.os, tc.reserve, tc.budget
			if !tc.ram {
				m.CapabilityProfile.SystemMemoryBytes = 0
				m.HostMemoryMeasured = false
			}
			j := submit(t, q, cpuSpec(2, 1))
			if reply := check(t, q, m, nil); reply.Job != nil {
				t.Fatal("unknown/exhausted capacity assigned", reply.Job)
			}
			current := job(t, q, j.ID)
			if !strings.Contains(current.Reason, tc.reason) {
				t.Fatalf("untruthful reason %q; want %q", current.Reason, tc.reason)
			}
		})
	}
}

func TestCPUAllowanceUsesHostPolicyAndStartedFence(t *testing.T) {
	q, _ := fixture(t)
	a, b := cpuMachine("a", "shared", "cpu", 8, 16), cpuMachine("b", "shared", "cpu", 8, 16)
	a.CPUBudgetCores, b.CPUBudgetCores = 6, 6
	check(t, q, a, nil)
	check(t, q, b, nil)
	cores := 2
	if _, err := q.ChangeHostMaintenance("shared", HostMaintenanceRequest{Action: "budget", CPUBudgetCores: &cores}); err == nil {
		t.Fatal("edited enabled allowance")
	}
	if _, err := q.ChangeHostMaintenance("shared", HostMaintenanceRequest{Action: "drain", CPUBudgetCores: &cores}); err != nil {
		t.Fatal(err)
	}
	check(t, q, a, nil)
	check(t, q, b, nil) // Offers only; stale reports cannot admit.
	a.CPUBudgetCores, b.CPUBudgetCores = 2, 2
	check(t, q, a, nil)
	check(t, q, b, nil)
	if _, err := q.ChangeHostMaintenance("shared", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, cpuSpec(2, 1))
	run := check(t, q, a, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal(run)
	}
	if err := q.Started(Result{Machine: run.Machine, Session: run.Session, Execution: Execution{ID: run.ID, Attempt: run.Attempt}}); err != nil {
		t.Fatal("own core charge blocked Started", err)
	}
	tooBig := submit(t, q, cpuSpec(3, 1))
	if check(t, q, b, nil).Job != nil || !strings.Contains(job(t, q, tooBig.ID).Reason, "CPU capacity") {
		t.Fatal("allowance multiplied by worker views")
	}
	status, err := q.HostMaintenanceStatus("shared")
	if err != nil || status.Policy.CPUBudgetCores == nil || *status.Policy.CPUBudgetCores != 2 {
		t.Fatal(status, err)
	}
}

func TestCPUFairUsageChargesCoreHoursAndSeparatesGPU(t *testing.T) {
	q, c := fixture(t)
	q.lossTimeouts = [3]time.Duration{2 * time.Hour, 2 * time.Hour, 2 * time.Hour}
	a, b := cpuMachine("a", "host-a", "cpu", 8, 16), cpuMachine("b", "host-b", "cpu", 8, 16)
	small, big := cpuSpec(1, 1), cpuSpec(4, 1)
	small.Project, big.Project = "small", "big"
	sj, bj := submit(t, q, small), submit(t, q, big)
	first := check(t, q, a, nil).Job
	second := check(t, q, b, nil).Job
	if first == nil || second == nil {
		t.Fatal("simultaneous CPU assignments missing")
	}
	c.Advance(time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	su, bu := q.s.FairUsage[q.fairKey(sj.Spec)], q.s.FairUsage[q.fairKey(bj.Spec)]
	if su.Seconds <= 0 || math.Abs(bu.Seconds/su.Seconds-4) > 1e-9 {
		t.Fatalf("core-hour charges: small=%g big=%g", su.Seconds, bu.Seconds)
	}
	if gpu := q.s.FairUsage[fairUsageKey(q.priorityMatch(sj.Spec).Tier, "gpu", "small")]; gpu.Seconds != 0 {
		t.Fatal("CPU charged as GPU")
	}
	finish(t, q, first, true)
	finish(t, q, second, true)
	nextBig := submit(t, q, big)
	nextSmall := submit(t, q, small)
	picked := check(t, q, a, nil).Job
	if picked == nil || picked.ID != nextSmall.ID || picked.ID == nextBig.ID {
		t.Fatal("P2 planner ignored CPU core-hour deficit", picked)
	}
}

func TestCPUDevGPUlessAndMixedWorkerRunRealCommand(t *testing.T) {
	for _, backend := range []string{"cpu", "fake"} {
		t.Run(backend, func(t *testing.T) {
			dir := t.TempDir()
			d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(dir, "q.db"), Checkpoints: filepath.Join(dir, "checkpoints"), Token: "cpu-fixture", Workers: 1, GPU: backend, MemoryGB: 8, Interval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			s := cpuSpec(1, .1)
			s.SubmissionKey = "cpu-dev-" + backend
			s.Run.Command = []string{"sh", "-c", `test "$MYCE_PLATFORM" = cpu && test "$MYCE_DEVICE" = cpu && test -z "$CUDA_VISIBLE_DEVICES" && test "$ONEAPI_DEVICE_SELECTOR" = "!*:*" && test "$ZE_AFFINITY_MASK" = 2147483647 && test "$ZE_ENABLE_PCI_ID_DEVICE_ORDER" = 1 && test -z "$MYCE_TOKEN"`}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			j, err := d.Client().SubmitAndWait(ctx, s)
			if err != nil {
				t.Fatal("real CPU command", err)
			}
			if j.State != "succeeded" || !j.ReleaseConfirmed {
				t.Fatal("real CPU command", j.State, j.Reason)
			}
			snap, err := d.Queue.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if len(snap.Machines) != 1 {
				t.Fatal("worker missing")
			}
			m := snap.Machines[0]
			if backend == "cpu" && (len(m.CapabilityProfile.Accelerators) != 0 || m.GPU != "cpu") {
				t.Fatal("phantom GPU on CPU host")
			}
		})
	}
}

func TestCPUQualificationNativeTarget(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("R1 native CPU targets")
	}
	e := compositionExtension(t, false)
	e.Kinds = nil
	e.Variants[0].Target.Platform = "cpu"
	e.Variants[0].Needs = ResourceNeeds{CPUCores: 1, MemoryGB: .1}
	e.Variants[0].Execution.Main.Command = []string{"sh", "-c", `test "$MYCE_DEVICE" = cpu && test "$MYCE_PLATFORM" = cpu && test -z "$MYCE_TOKEN" && cat "$MYCE_EXTENSION_DIR/baseline/input"`}
	delete(e.Variants[0].Baselines, "kind:sample")
	e = freezeComposition(t, e)
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(dir, "q.db"), Checkpoints: filepath.Join(dir, "checkpoints"), ExtensionRoot: root, HostID: "cpu-host", Token: "fixture", Workers: 1, GPU: "cpu", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	await(t, func() bool { snap, _ := d.Queue.Snapshot(); return len(snap.Machines) == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20}
	reports, err := CheckExtension(ctx, root, e, "cpu", "0", "cpu-host", io.Discard, cache, d.Client())
	if err != nil || len(reports) != 1 || !reports[0].Qualified {
		t.Fatal("native CPU check", reports, err)
	}
	j, err := d.Client().SubmitAndWait(ctx, Spec{Name: "cpu-extension", SubmissionKey: "cpu-qualified", Project: "cpu", Extension: extensionKey(e)})
	if err != nil {
		t.Fatal("qualified CPU placement", err)
	}
	if j.State != "succeeded" {
		t.Fatal("qualified CPU placement", j.State, j.Reason)
	}
}

func TestCPUCapacityUnknownAlsoBlocksGPUAdmission(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("gpu", "host", "cuda", 0, 16)
	gpu := spec("experiment")
	gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
	j := submit(t, q, gpu)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("GPU allocation bypassed unknown host cores")
	}
	if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "logical CPU capacity unavailable") {
		t.Fatal(reason)
	}
}

func TestCPUReclaimProductionAtCoreAllowance(t *testing.T) {
	for _, tc := range []struct {
		name, priority string
		cores          int
		stop           bool
		otherCard      bool
		reverse        bool
		hold           bool
	}{
		{"legacy GPU production reclaims the only admitted core", "production", 1, true, false, false, false},
		{"production hold reclaims the only admitted core", "production", 1, true, false, false, true},
		{"production reclaims a core on another card of the host", "production", 1, true, true, false, false},
		{"same tier waits for the only admitted core", "experiment", 1, false, false, false, false},
		{"production cannot exceed total admitted cores", "production", 2, false, false, false, false},
		{"policy comparator protects the higher tier", "production", 1, false, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			m := cpuMachine("gpu", "physical", "cuda", 8, 16)
			m.CPUBudgetCores = 1
			if tc.hold {
				if err := q.ConfigureHosts(map[string]HostPolicy{m.HostID: {ProductionHold: true}}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.reverse {
				policy := defaultPriorityPolicy()
				policy.Tiers = []PriorityTier{{Name: "experiment", Selectors: []string{"*/experiment"}}, {Name: "other", Selectors: []string{"*/*"}}}
				if _, err := q.SetPriorityPolicy(0, policy); err != nil {
					t.Fatal(err)
				}
			}
			submit(t, q, spec("experiment"))
			running := check(t, q, m, nil).Job
			if running == nil {
				t.Fatal("experiment not assigned")
			}
			s := spec(tc.priority)
			target := m
			if tc.otherCard {
				target.ID, target.Session = "other", "session-other"
				check(t, q, target, nil)
				s.AllowedMachines = []string{target.ID}
			}
			if tc.cores > 1 {
				s.Needs.CPUCores = tc.cores
			}
			pending := submit(t, q, s)
			reply := check(t, q, m, running)
			if reply.Stop != tc.stop {
				t.Fatalf("stop=%v, want %v; experiment=%s", reply.Stop, tc.stop, job(t, q, running.ID).State)
			}
			if reply.Job != nil || job(t, q, pending.ID).State != "queued" {
				t.Fatal("admitted before the core was released")
			}
			if tc.stop {
				if err := q.Complete(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: running.ID, Attempt: running.Attempt}, Interrupted: true}); err != nil {
					t.Fatal(err)
				}
				if next := check(t, q, target, nil).Job; next == nil || next.ID != pending.ID {
					t.Fatal("production not assigned after release", next)
				}
			}
		})
	}
}

func TestCPUSubmitAllowedHostCapacity(t *testing.T) {
	for _, tc := range []struct {
		name         string
		busy, reject bool
		cores        int
	}{
		{"busy allow-listed legacy GPU host queues", true, false, 0},
		{"idle allow-listed GPU host queues", false, false, 0},
		{"job exceeding total core allowance rejects", true, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			m := cpuMachine("gpu", "physical", "cuda", 1, 16)
			check(t, q, m, nil)
			if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{m.ID}}}); err != nil {
				t.Fatal(err)
			}
			var running *Job
			if tc.busy {
				submit(t, q, spec("experiment"))
				running = check(t, q, m, nil).Job
				if running == nil {
					t.Fatal("first job not assigned")
				}
			}
			s := spec("experiment")
			s.Needs.CPUCores = tc.cores
			pending, err := q.Submit(s)
			if (err != nil) != tc.reject {
				t.Fatalf("submit error=%v, reject=%v", err, tc.reject)
			}
			if tc.reject {
				return
			}
			if pending.State != "queued" {
				t.Fatal("submission did not queue", pending.State)
			}
			if running != nil {
				finish(t, q, running, true)
			}
			if next := check(t, q, m, nil).Job; next == nil || next.ID != pending.ID {
				t.Fatal("queued job not assigned when capacity is free", next)
			}
		})
	}
}

func TestCPUPlacementDoesNotReserveGPUMemory(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("gpu", "host", "cuda", 8, 16)
	m.FreeMemoryGB = 0 // Healthy mixed host, with no accelerator memory free.
	j := submit(t, q, cpuSpec(2, 1))
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("CPU work required free GPU memory", job(t, q, j.ID).Reason)
	}
}

func TestCPULearnedProgramUsageRemainsHostRAM(t *testing.T) {
	q, _ := fixture(t)
	e := fakeExtension(t)
	e.Platforms = []string{"cpu"}
	j := &Job{Spec: placementSpec(true), Extensions: []Extension{e}}
	j.Spec.Needs = ResourceNeeds{CPUCores: 2, HostMemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}
	q.s.ProgramNeeds = map[string]SizeHint{programNeedsKey(j, e, "cpu"): {MemoryGB: 4, HostMemoryGB: 2}}
	q.applyLearnedNeeds(j)
	if j.Spec.Needs.MemoryGB != 0 || j.Spec.Needs.HostMemoryGB != 4 {
		t.Fatal("CPU observation became a GPU reservation", j.Spec.Needs)
	}
}

func TestCPUProfileDoesNotProbeAnAccelerator(t *testing.T) {
	for _, goos := range []string{"linux", "darwin"} {
		t.Run(goos, func(t *testing.T) {
			probe := ProfileProbe{Machine: MachineProbe{OS: goos, Arch: "amd64", Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
				if name != "/usr/sbin/sysctl" {
					t.Fatalf("CPU host probed accelerator: %s", name)
				}
				return []byte("17179869184"), nil
			}}, CPUs: func() int { return 8 }, ReadFile: func(string) ([]byte, error) { return []byte("MemTotal: 16777216 kB\n"), nil }, Disk: func(string) (uint64, uint64, error) { return 100 << 30, 50 << 30, nil }}
			p := probe.Detect(context.Background(), Machine{GPU: "cpu"}, "ignored", time.Time{})
			if p.LogicalCPUs != 8 || p.SystemMemoryBytes != 16<<30 || len(p.Accelerators) != 0 || len(p.Unavailable) != 0 {
				t.Fatalf("CPU facts: %+v", p)
			}
		})
	}
	source, err := newMemorySource("cpu", 0, 0, "auto", "linux", nil, func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("GPU probe on CPU source")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if free, err := source(context.Background()); err != nil || free != 0 {
		t.Fatal("phantom accelerator memory", free, err)
	}
}

func TestCPUFingerprintIgnoresVolatileReadings(t *testing.T) {
	a := []byte("processor : 0\nmodel name : example\ncpu MHz : 1000\nflags : one two\nprocessor : 1\nmodel name : example\nflags : one two\n")
	b := []byte("flags : one two\nmodel name : example\ncpu MHz : 2000\nprocessor : 3\n")
	if cpuIdentity(a) != cpuIdentity(b) || cpuIdentity(a) == "" {
		t.Fatal("volatile per-core readings changed qualification identity")
	}
	if cpuIdentity(a) == cpuIdentity([]byte("model name : different\nflags : one two")) {
		t.Fatal("different CPU shared identity")
	}
}

func TestCPUStatusNamesCoresInsteadOfAcceleratorMemory(t *testing.T) {
	m := cpuMachine("cpu", "host", "cpu", 8, 16)
	m.CPUReserveCores = 1
	m.EffectiveCPUCores, m.FreeCPUCores = 7, 7
	if got := strings.Join(CapabilityProfileLines(&m), "\n"); !strings.Contains(got, "7 free / 7 admitted") || strings.Contains(got, "accelerator measurements not reported") {
		t.Fatal(got)
	}
	if resourceWords("cpu") != "CPU" {
		t.Fatal("CPU resource not named")
	}
}
