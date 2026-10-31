package myce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func sharedHostFixture(t *testing.T, q *Queue) Machine {
	t.Helper()
	m := cpuMachine("interactive", "interactive", "cpu", 8, 12)
	m.CapabilityProfile.SystemMemoryBytes = 24 << 30
	m.HostMemoryBudgetGB, m.CPUBudgetCores = 8, 2
	m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
	m.SharedCPUEnforcement = "cgroup v2 cpu.weight=10 cpu.max=200000/100000"
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 24, UsedGB: 14, MyceliumGB: 0}
	check(t, q, m, nil)
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "profile", SharedHost: &SharedHostProfile{InteractiveReserveGB: 4}}); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSharedHostAdmissionUsesHistoryLiveReserveAndAcknowledgement(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	// A quieter live reading must not erase the observed external peak: 24-14-4=6.
	m.MemoryObservation.UsedGB = 8
	m.HostAvailableMemoryGB = 12
	if _, err := q.ChangeHostMaintenance(m.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	large := submit(t, q, cpuSpec(1, 6.5))
	small := submit(t, q, cpuSpec(1, 2))
	reply := check(t, q, m, nil)
	if reply.Job != nil || reply.SharedHost == nil {
		t.Fatal("unacknowledged profile admitted work", reply)
	}
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 3}
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("inexact profile acknowledgement admitted work", r.Job.ID)
	}
	m.SharedHost = cloneSharedHost(reply.SharedHost)
	reply = check(t, q, m, nil)
	got := reply.Job
	if got == nil || got.ID != small.ID {
		t.Fatalf("fitting light work not admitted: %+v", reply)
	}
	requireState(t, q, large.ID, "queued")
	if err := q.Started(result(got, false)); err != nil {
		t.Fatal(err)
	}
	// New pressure withholds further work, but phase one does not stop a Started job.
	clock.Advance(time.Second)
	m.MemoryObservation.UsedGB = 21
	m.HostAvailableMemoryGB = 0
	reply = check(t, q, m, got)
	for _, d := range reply.Attempts {
		if d.Stop || d.Kill {
			t.Fatal("phase one unexpectedly stopped work")
		}
	}
	if reply.Stop || reply.Kill {
		t.Fatal("phase one unexpectedly stopped work")
	}
	finish(t, q, got, true)
	requireState(t, q, large.ID, "queued")
	// Missing live evidence is unknown, even with retained history.
	m.MemoryObservation = nil
	check(t, q, m, nil)
	status, err := q.HostMaintenanceStatus(m.ID)
	if err != nil || status.Workers[0].Machine.SharedHostReason == "" || status.Workers[0].Machine.FreeHostMemoryGB != 0 {
		t.Fatal(status, err)
	}
}

func TestSharedHostHistoryExpiresAndReserveIsNotDoubleCounted(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	// Existing 4-GiB reserve is already removed: use min(12, raw 16 - 4), not 12-4.
	m.MemoryObservation.UsedGB = 8
	check(t, q, m, nil)
	q.mu.Lock()
	q.refreshHostMemory()
	got := q.s.Machines[m.ID].FreeHostMemoryGB
	// History, rather than live RAM, limits this to 6 GiB.
	if got != 6 {
		t.Errorf("headroom=%g want 6", got)
	}
	// Isolate a representative quieter history to prove the live reserve is not
	// deducted again from already-reserved availability.
	q.s.ExternalMemory = nil
	q.recordExternalMemory(&m)
	budget, _ := hostMemoryAllowance(20)
	policy := q.s.HostMaintenance[m.ID]
	policy.MemoryBudget = &budget
	q.s.HostMaintenance[m.ID] = policy
	q.s.Machines[m.ID].HostMemoryBudgetGB = 20
	q.refreshHostMemory()
	if free := q.s.Machines[m.ID].FreeHostMemoryGB; free != 12 {
		t.Errorf("reserve double counted: %g want 12", free)
	}
	clock.Advance(8 * 24 * time.Hour)
	q.refreshHostMemory()
	if q.s.Machines[m.ID].FreeHostMemoryGB != 0 || q.s.Machines[m.ID].SharedHostReason == "" {
		t.Error("expired history treated as zero external use")
	}
	q.mu.Unlock()
}

func TestSharedHostMeasuredBurstAtHighExternalPeak(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	m.CapabilityProfile.SystemMemoryBytes = 64 << 30
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 64, UsedGB: 52.6}
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 20, 10.4
	budget := 20.0
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &budget}); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, nil)
	// The peak already includes observed owner growth. Protect a measured
	// additional burst, not another 25% of that accumulated baseline.
	assertRoom := func(want float64) {
		t.Helper()
		s, err := q.HostMaintenanceStatus(m.HostID)
		if err != nil || len(s.Workers) != 1 || math.Abs(s.Workers[0].Machine.FreeHostMemoryGB-want) > 1e-9 {
			t.Fatalf("want %g GiB: %+v, %v", want, s, err)
		}
	}
	assertRoom(7.25) // floor((64 - 52.6 - 4) * 4) / 4
	clock.Advance(time.Second)
	m.MemoryObservation.UsedGB = 40
	m.HostAvailableMemoryGB = 23
	check(t, q, m, nil)
	assertRoom(7.25) // A quieter sample cannot erase the retained peak.
	m.HostAvailableMemoryGB = 1.5
	check(t, q, m, nil)
	assertRoom(1.5) // The historical formula cannot exceed live admission.
	// Worker RSS is not reclaimable headroom. It can keep the external peak
	// unchanged while raw RAM runs out; the raw live bound must still win.
	m.HostAvailableMemoryGB = 10.4
	m.MemoryObservation.UsedGB, m.MemoryObservation.MyceliumGB = 60, 20
	check(t, q, m, nil)
	assertRoom(0)
	m.MemoryObservation.UsedGB, m.MemoryObservation.MyceliumGB = 40, 0
	m.HostAvailableMemoryGB = 23
	m.HostMemoryBudgetGB, budget = 2, 2
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "budget", MemoryBudgetGB: &budget}); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, nil)
	assertRoom(2) // Nor can it increase the operator allowance.

	w := Worker{Machine: m, HostMemoryReserveGB: 1, HostMemorySource: func(context.Context) (float64, error) { return 10.4, nil }}
	free, err := w.sharedHostLiveMemory(context.Background())
	if err != nil || math.Abs(free-7.4) > 1e-9 {
		t.Fatalf("live launch headroom=%g, want 7.4: %v", free, err)
	}
}

func TestSharedHostPolicyPersistsAndExclusionsRemainIndependent(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	m := sharedHostFixture(t, q)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	// A new logical worker on this physical host receives the persisted policy.
	m.ID = "replacement-worker"
	m.MemoryObservation.UsedGB = 8
	first := check(t, q, m, nil)
	if first.SharedHost == nil || first.SharedHost.InteractiveReserveGB != 4 {
		t.Fatal("physical-host profile lost after restart", first.SharedHost)
	}
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	check(t, q, m, nil)
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.SharedHost == nil || !status.Policy.LastResort || status.Policy.Mode != "drained" {
		t.Fatal(status, err)
	}
	for _, worker := range status.Workers {
		if worker.Machine.ID == m.ID && worker.Machine.FreeHostMemoryGB != 6 {
			t.Fatal("persisted peak lost after quieter reading", worker.Machine.FreeHostMemoryGB)
		}
	}
	gpu := m
	gpu.GPU = "metal"
	q.mu.Lock()
	blocked := q.componentExcluded(&gpu, "metal", "")
	q.mu.Unlock()
	if !blocked {
		t.Fatal("profile did not use GPU exclusion path")
	}
	changeExclusion(t, q, "host=interactive;kind=gpu", "add")
	if _, err = q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "profile", SharedHost: &SharedHostProfile{}}); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	blocked = q.componentExcluded(&gpu, "metal", "")
	q.mu.Unlock()
	if !blocked {
		t.Fatal("removing profile cleared independent GPU exclusion")
	}
	status, err = q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.Mode != "drained" || !status.Policy.LastResort {
		t.Fatal("profile removal changed maintenance/placement", status, err)
	}
}

func TestSharedHostRejectsUnsafePolicyAndLaunches(t *testing.T) {
	q, _ := fixture(t)
	m := sharedHostFixture(t, q)
	for _, r := range []HostMaintenanceRequest{
		{Action: "budget", CPUBudgetCores: new(int)},
		{Action: "placement", LastResort: new(bool)},
		{Action: "profile", SharedHost: &SharedHostProfile{InteractiveReserveGB: math.NaN()}},
	} {
		if _, err := q.ChangeHostMaintenance(m.ID, r); err == nil {
			t.Error("accepted unsafe policy", r)
		}
	}
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	native := Job{Spec: cpuSpec(1, 1)}
	if err := normalizeResources(&native.Spec); err != nil {
		t.Fatal(err)
	}
	if !sharedHostJobFits(&native, &m) {
		t.Fatal("native CPU command refused")
	}
	native.Spec.Run.Docker = true
	if sharedHostJobFits(&native, &m) {
		t.Fatal("Docker descendants cannot inherit native priority")
	}
	// Worker checks raw availability after the protected reserve, not only its default reserve.
	w := Worker{Machine: m, HostMemoryReserveGB: 1, HostMemorySource: func(context.Context) (float64, error) { return 4, nil }}
	m.SharedHost.InteractiveReserveGB = 4
	free, err := w.sharedHostLiveMemory(context.Background())
	if err != nil || free != 1 {
		t.Fatal("worker reserve counted incorrectly", free, err)
	}
}

func TestSharedHostRequiresPreparedSidecarEnvironment(t *testing.T) {
	m := cpuMachine("worker", "physical", "cpu", 4, 16)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	side := NativeSidecar{Name: "helper", Environment: "tools", Command: []string{"sh", "-c", "exit 0"}}
	r := binaryRecipe(t, "cpu")
	j := Job{Spec: cpuSpec(1, 1), SidecarEnvironmentRecipes: map[string]map[string]EnvironmentRecipe{"helper": {"cpu": r}}}
	j.Spec.Run.Sidecars = []NativeSidecar{side}
	if err := normalizeResources(&j.Spec); err != nil {
		t.Fatal(err)
	}
	if sharedHostJobFits(&j, &m) {
		t.Error("cold sidecar environment admitted on shared host")
	}
	m.Environments = []string{r.Key}
	if !sharedHostJobFits(&j, &m) {
		t.Fatal("prepared sidecar environment refused")
	}
	// The advertised ready entry can disappear before launch. Do not rebuild
	// with a batch authorization: preparations are outside phase one.
	builds := 0
	w := Worker{Machine: m, environmentCache: &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Run: func(context.Context, []string, string, []string, io.Writer, io.Writer) error {
		builds++
		return errors.New("unexpected environment build")
	}}}
	g := nativeGroup{worker: w, ctx: t.Context()}
	_, release, err := g.sidecarEnvironment(j, side, "")
	if release != nil {
		release()
	}
	if err == nil || !strings.Contains(err.Error(), "environment preparation required") || builds != 0 {
		t.Fatalf("missing prepared sidecar rebuilt: builds=%d, err=%v", builds, err)
	}
}

func TestSharedHostDoesNotRebuildMainEnvironment(t *testing.T) {
	q, clock := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	r := binaryRecipe(t, "cpu")
	m.Environments = []string{r.Key} // Readiness reported before the cache disappeared.
	w := Worker{Machine: m, Clock: clock, StateDir: t.TempDir(), HostMemorySource: func(context.Context) (float64, error) { return 12, nil }, environmentCache: &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := Job{ID: "cold-main", Attempt: 1, Spec: cpuSpec(1, 1), EnvironmentRecipes: map[string]EnvironmentRecipe{"cpu": r}}
	if err := normalizeResources(&j.Spec); err != nil {
		t.Fatal(err)
	}
	p, err := startAfterFixtureInputs(t, w, store, j)
	if p != nil {
		p.stop(clock.Now(), 0, true)
		<-p.done
	}
	if err == nil || !strings.Contains(err.Error(), "environment preparation required") || w.environmentCache.ready(r.Key) {
		t.Fatalf("missing prepared main environment rebuilt: %v", err)
	}
}

func TestSharedHostCPUEnvironmentOnAcceleratorWorker(t *testing.T) {
	q, _ := fixture(t)
	m := sharedHostFixture(t, q)
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	m.GPU = "cuda"
	m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: "cuda", MemoryBytes: 8 << 30}}
	m.Devices = []MachineDevice{{ID: "0", Identity: "GPU-synthetic", Platform: "cuda", Selector: "0", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	r := binaryRecipe(t, "cpu")
	m.Environments = []string{r.Key}
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, nil)
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Environment = Environment{Name: "tools"}
	j := submit(t, q, s)
	reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol})
	if err != nil {
		t.Fatal(err)
	}
	assigned := assignedJob(reply)
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("prepared CPU command not assigned", assigned, job(t, q, j.ID).Reason)
	}
	if err := q.AuthorizeLaunch(HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: assigned.ID, Attempt: "1"}); err != nil {
		t.Fatal("prepared CPU environment checked against accelerator platform", err)
	}
	if err := q.Started(result(assigned, false)); err != nil {
		t.Fatal("prepared CPU command not authorized to start", err)
	}
}

func TestSharedHostBackgroundPriorityFailureWarns(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin background policy")
	}
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	old := sharedHostSetBackgroundPriority
	writer := log.Writer()
	var warnings bytes.Buffer
	log.SetOutput(&warnings)
	sharedHostSetBackgroundPriority = func(int) error { return syscall.EPERM }
	t.Cleanup(func() { sharedHostSetBackgroundPriority = old; log.SetOutput(writer) })
	if err := applySharedHostPriority(cmd.Process.Pid, &SharedHostProfile{}); err != nil {
		t.Fatalf("background policy failure must not abort launch: %v", err)
	}
	if !strings.Contains(warnings.String(), "warning: shared host background task policy") || !strings.Contains(warnings.String(), syscall.EPERM.Error()) {
		t.Fatalf("missing policy warning: %s", &warnings)
	}
}

func TestSharedHostDevCommandPriority(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "synthetic-worker-only-token")
	controlPriority, err := syscall.Getpriority(syscall.PRIO_PROCESS, os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	// Darwin's background scheduler band has maximum priority 4; nice alone
	// remains above it. Inspect actual payloads across fork/exec, not just the
	// worker's report that policy application succeeded.
	controlBand := ""
	backgroundCheck := ""
	if runtime.GOOS == "darwin" {
		b, err := exec.Command("ps", "-o", "pri=", "-p", fmt.Sprint(os.Getpid())).Output()
		if err != nil {
			t.Fatal(err)
		}
		controlBand = strings.TrimSpace(string(b))
		backgroundCheck = `test "$(ps -o pri= -p $$)" -le 4 && sh -c 'test "$(ps -o pri= -p $$)" -le 4' || exit 43; `
	}
	oldRSS := workerMemoryRSS
	workerMemoryRSS = func(context.Context) (float64, error) { return 0, nil }
	t.Cleanup(func() { workerMemoryRSS = oldRSS })
	root := t.TempDir()
	d, err := StartDev(DevConfig{SharedPressureSource: func(context.Context) (SharedHostPressure, error) {
		return SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}, nil
	}, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "dev-fixture", GPU: "cpu", Workers: 1, Interval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	c := d.Client()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := "/api/hosts/dev-host/maintenance"
	var status HostMaintenanceStatus
	await(t, func() bool {
		if err := c.Call(ctx, "GET", path, nil, &status); err != nil {
			t.Fatal(err)
		}
		return len(status.Workers) == 1 && status.Workers[0].Fresh && status.ExternalMemory != nil
	})
	change := func(r HostMaintenanceRequest) {
		t.Helper()
		if err := c.Call(ctx, "POST", path, r, &status); err != nil {
			t.Fatal(err)
		}
	}
	if runtime.GOOS == "darwin" {
		ordinary := cpuSpec(1, .01)
		ordinary.Run.Command = []string{"sh", "-c", `test "$(ps -o pri= -p $$)" -gt 4 && sh -c 'test "$(ps -o pri= -p $$)" -gt 4'`}
		var j Job
		if err := c.Call(ctx, "POST", "/api/jobs", ordinary, &j); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool {
			if err := c.Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &j); err != nil {
				t.Fatal(err)
			}
			if j.State == "failed" {
				t.Fatal("unprofiled payload entered background band", j)
			}
			return j.State == "succeeded"
		})
	}
	change(HostMaintenanceRequest{Action: "drain"})
	await(t, func() bool {
		if err := c.Call(ctx, "GET", path, nil, &status); err != nil {
			t.Fatal(err)
		}
		return status.Quiescent
	})
	cores, ram := 2, 2.0
	change(HostMaintenanceRequest{Action: "budget", CPUBudgetCores: &cores, MemoryBudgetGB: &ram})
	change(HostMaintenanceRequest{Action: "profile", SharedHost: &SharedHostProfile{InteractiveReserveGB: .125}})
	change(HostMaintenanceRequest{Action: "resume"})
	s := cpuSpec(1, .01)
	s.Run.Env = map[string]string{"GOMAXPROCS": "64"}
	s.Run.Command = []string{"sh", "-c", backgroundCheck + `n=$(ps -o nice= -p $$); test "$n" -ge 10 && test "$GOMAXPROCS" = 1 && test "${MYCE_TOKEN+x}" != x && printf shared-host-ok`}
	var j Job
	if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		if err := c.Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &j); err != nil {
			t.Fatal(err)
		}
		if j.State == "failed" {
			t.Fatalf("native command failed: %+v", j)
		}
		return j.State == "succeeded"
	})
	if err := c.Call(ctx, "GET", path, nil, &status); err != nil {
		t.Fatal(err)
	}
	if status.Workers[0].Machine.SharedHost == nil || !status.Policy.LastResort {
		t.Fatal("profile not acknowledged", status)
	}
	// Exercise the separate group/member gates, including a sidecar and the
	// main payload. Its marker proves the helper checked priority and token too.
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", Env: map[string]string{"GOMAXPROCS": "64"}, Command: []string{"sh", "-c", backgroundCheck + `n=$(ps -o nice= -p $$); test "$n" -ge 10 && test "$GOMAXPROCS" = 1 && test "${MYCE_TOKEN+x}" != x || exit 42; touch helper-ready; while :; do sleep 1; done`}}}
	s.Run.Command = []string{"sh", "-c", backgroundCheck + `n=$(ps -o nice= -p $$); test "$n" -ge 10 && test "$GOMAXPROCS" = 1 && test "${MYCE_TOKEN+x}" != x || exit 42; while test ! -f helper-ready; do sleep .01; done`}
	if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		if err := c.Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &j); err != nil {
			t.Fatal(err)
		}
		if j.State == "failed" {
			t.Fatalf("native group failed: %+v", j)
		}
		return j.State == "succeeded"
	})
	if priority, err := syscall.Getpriority(syscall.PRIO_PROCESS, os.Getpid()); err != nil || priority != controlPriority {
		t.Fatal("payload priority changed worker control", priority, controlPriority, err)
	}
	if runtime.GOOS == "darwin" {
		b, err := exec.Command("ps", "-o", "pri=", "-p", fmt.Sprint(os.Getpid())).Output()
		if err != nil || strings.TrimSpace(string(b)) != controlBand {
			t.Fatal("payload background policy changed worker control", string(b), controlBand, err)
		}
	}
}

func TestSharedHostPrefersOrdinaryAndFallsBackImmediately(t *testing.T) {
	q, _ := fixture(t)
	shared := sharedHostFixture(t, q)
	shared.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	check(t, q, shared, nil)
	if _, err := q.ChangeHostMaintenance(shared.ID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	ordinary := cpuMachine("ordinary", "ordinary", "cpu", 2, 16)
	check(t, q, ordinary, nil)
	first := submit(t, q, cpuSpec(2, 1))
	if r := check(t, q, shared, nil); r.Job != nil {
		t.Fatal("shared host took ordinary fitting work")
	}
	holder := check(t, q, ordinary, nil).Job
	if holder == nil || holder.ID != first.ID {
		t.Fatal("ordinary host not selected", holder)
	}
	if err := q.Started(result(holder, false)); err != nil {
		t.Fatal(err)
	}
	second := submit(t, q, cpuSpec(1, 1))
	fallback := check(t, q, shared, nil).Job
	if fallback == nil || fallback.ID != second.ID {
		t.Fatal("shared host did not take immediate fitting fallback", fallback)
	}
}
