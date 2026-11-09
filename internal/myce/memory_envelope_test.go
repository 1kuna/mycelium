package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMemoryEnvelopeCeilingBoundThenExploratory(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		success    bool
	}{{"auto success", "auto", true}, {"explicit success", "", true}} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			s.Needs.Memory = tc.mode
			if tc.mode == "auto" {
				first := startObservation(t, q, m, s)
				clock.Advance(time.Second)
				r := result(&first, true)
				host := 2.0
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			bound := startObservation(t, q, m, s)
			if tc.mode == "auto" && bound.MemoryEnvelope.Source != "accepted peak + measured uncertainty" {
				t.Fatalf("auto run did not reuse seed peak: %+v", bound.MemoryEnvelope)
			}
			a, err := fakeCeilings(t).create("attempt-bound", 5<<29, 0)
			if err != nil {
				t.Fatal(err)
			}
			for name, value := range map[string]string{
				"memory.stat":   "anon 2684354560\nkernel 0\nshmem 0\npgscan_file 100\n",
				"memory.events": "max 6264\nhigh 0\noom 0\noom_kill 0\n",
			} {
				if err := os.WriteFile(filepath.Join(a.path, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(time.Second)
			r := result(&bound, tc.success)
			r.MemoryPeak = a.peak()
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			wantState := "failed"
			if tc.success {
				wantState = "succeeded"
			}
			if got := job(t, q, bound.ID); got.State != wantState || got.Attempt != bound.Attempt {
				t.Fatalf("ceiling pressure changed the attempt outcome: %+v", got)
			}
			for _, point := range q.s.WorkObservations {
				if tc.success && tc.mode == "auto" {
					if point.MemoryRecordedAt.IsZero() || point.MemoryPeak == nil || *point.MemoryPeak.HostGB != 2 {
						t.Errorf("successful constrained sample erased accepted peak: %+v", point)
					}
					continue
				}
				if point.MemoryPeak != nil && !point.MemoryPeak.Constrained || !point.MemoryRecordedAt.IsZero() || point.Forbidden {
					t.Errorf("cap-bound result retained a learned peak or forbade work: %+v", point)
				}
			}
			s.Needs.Memory = "auto"
			next := startObservation(t, q, m, s)
			wantSource := physicalMemoryProbe
			if tc.mode == "auto" {
				wantSource = acceptedMemoryEscalation
			}
			if tc.success && tc.mode == "auto" {
				if next.MemoryEnvelope.Source != "accepted peak + measured uncertainty" || next.Spec.Needs.HostMemoryGB != 2+minimumHostMemoryGB {
					t.Fatalf("rejected sample changed accepted envelope: %+v", next.MemoryEnvelope)
				}
				return
			}
			if next.MemoryEnvelope.Source != wantSource || next.Spec.Needs.HostMemoryGB <= bound.Spec.Needs.HostMemoryGB {
				t.Fatalf("next auto run did not explore a larger envelope: bound=%+v next=%+v", bound.MemoryEnvelope, next.MemoryEnvelope)
			}
		})
	}
}

func TestMemoryEnvelopeCPUOnGPUHostReusesAndRelearnsPeak(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	m.CPUExtensionFingerprint = m.ExtensionFingerprint
	m.GPU, m.ExtensionFingerprint = "fake", "synthetic-gpu-v1"
	m.Devices = m3Machine().Devices
	for i := range m.Devices {
		m.Devices[i].ReserveMemoryGB, m.Devices[i].BudgetGB = 1, 7
	}
	machineCheck(t, q, m)
	s.Needs.Memory = "auto"
	start := func(source string, hostGB float64) *Job {
		t.Helper()
		queued := submit(t, q, s)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("CPU job not assigned: %+v", job(t, q, queued.ID))
		}
		if len(j.AllocatedDevices) != 0 || j.MemoryEnvelope == nil || j.MemoryEnvelope.Source != source || hostGB > 0 && j.Spec.Needs.HostMemoryGB != hostGB {
			t.Fatalf("wrong CPU admission: devices=%v envelope=%+v", j.AllocatedDevices, j.MemoryEnvelope)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		return j
	}
	complete := func(j *Job, peak float64) {
		t.Helper()
		clock.Advance(time.Second)
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, point := range q.s.WorkObservations {
			if point.Job == j.ID {
				found = true
				if point.StaleReason != "" || point.MemoryPeak == nil || point.MemoryPeak.HostGB == nil || *point.MemoryPeak.HostGB != peak || !point.MemoryRecordedAt.Equal(clock.Now()) {
					t.Fatalf("accepted CPU peak is not current: %+v", point)
				}
			}
		}
		if !found {
			t.Fatal("successful solo CPU run did not record a peak")
		}
	}
	first := start(physicalMemoryProbe, 0)

	complete(first, 2)
	next := start("accepted peak + measured uncertainty", 2+minimumHostMemoryGB)
	clock.Advance(time.Second)
	r := result(next, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	for _, point := range q.s.WorkObservations {
		if point.MemoryPeak != nil || !point.MemoryRecordedAt.IsZero() {
			t.Fatalf("OOM retained a memory peak: %+v", point)
		}
	}
	if err := q.Cancel(next.ID); err != nil {
		t.Fatal(err)
	}
	complete(start(acceptedMemoryEscalation, 0), 8)
	start("accepted peak + measured uncertainty", 8+minimumHostMemoryGB)
}

func TestMemoryEnvelopeProfileIdentityAndLifecycle(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	m.CPUExtensionFingerprint = "profile-cpu-v1"
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 16, 0.1
	for i := range m.Devices {
		m.Devices[i].ReserveMemoryGB, m.Devices[i].BudgetGB = 1, 7
	}
	machineCheck(t, q, m)
	s := cpuSpec(1, 0)
	s.Name, s.Project, s.RequireTarget, s.Needs.Memory = "gate-full", "synthetic", true, "auto"
	s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
	s.Run.Command = []string{"sh", "-ec", "tar -xf .myce-source.tar\nbash scripts/gate.sh full\ntest -f result"}
	setSource := func(data string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "source")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(q.files.blob(ref), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		s.Files = FileManifest{".myce-source.tar": ref}
	}
	start := func(source string, hostGB float64) *Job {
		t.Helper()
		queued := submit(t, q, s)
		if queued.State != "queued" {
			t.Fatal("profile did not queue", queued.State)
		}
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("profile not assigned: %+v", job(t, q, queued.ID))
		}
		if j.MemoryEnvelope.Source != source || hostGB > 0 && j.Spec.Needs.HostMemoryGB != hostGB {
			t.Fatalf("wrong profile estimate: %+v", j.MemoryEnvelope)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		return j
	}
	complete := func(j *Job, peak float64) {
		t.Helper()
		clock.Advance(time.Second)
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	setSource("commit one")
	first := start(physicalMemoryProbe, 0)
	// #559: a bounded first run leaves room for other work beside it.
	busy := submit(t, q, s)
	if beside := assignedJob(machineCheck(t, q, m, first)); beside != nil {
		t.Fatal("cold probe shared unknown demand", beside)
	}
	complete(first, 2)
	beside := assignedJob(machineCheck(t, q, m))
	if beside == nil || beside.ID != busy.ID || beside.MemoryEnvelope.Basis != "learned" {
		t.Fatal("accepted peak did not release probe exclusivity")
	}
	if err := q.Started(result(beside, false)); err != nil {
		t.Fatal(err)
	}
	complete(beside, 2)
	complete(start("accepted peak + measured uncertainty", 2+minimumHostMemoryGB), 2)
	setSource("commit two")
	complete(start("accepted peak + measured uncertainty", 2+minimumHostMemoryGB), 2)
	s.Needs.Memory, s.Needs.HostMemoryGB = "", 3
	complete(start("declared", 3), 1)
}

// #519: an accepted Linux cgroup peak times 2.5 used to strand Darwin
// despite its only local peak being constrained (and therefore no upper bound).
func TestMemoryEnvelopeCPUForeignOSPeak(t *testing.T) {
	q, clock, linux, darwin, s := memoryExplorationFixture(t)
	start := func(m Machine, source string, hostGB float64) *Job {
		t.Helper()
		s.Targets = []ExecutionTarget{machineTarget(&m)}
		queued := submit(t, q, s)
		run := check(t, q, m, nil).Job
		if run == nil || run.ID != queued.ID {
			t.Fatal("profile not assigned", job(t, q, queued.ID))
		}
		if run.MemoryEnvelope.Source != source || math.Abs(run.Spec.Needs.HostMemoryGB-hostGB) > 1e-9 {
			t.Fatalf("wrong target envelope: %+v", run.MemoryEnvelope)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		return run
	}
	complete := func(run *Job, peak float64, constrained bool) {
		t.Helper()
		clock.Advance(time.Second)
		if run.Machine != linux.ID {
			// The accepted source-platform prior is still from a live worker;
			// keep its advertised one-second reports flowing during peer runs.
			check(t, q, linux, nil)
		}
		r := result(run, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: constrained}
		if run.Machine == linux.ID {
			r.MemoryPeak.CgroupGB = &peak
		} else {
			r.MemoryPeak.Source = processGroupMemorySource
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	// Retained constrained evidence predates the foreign prior; it cannot
	// itself become an accepted estimate or shrink an accepted foreign prior.
	s.Needs.Memory, s.Needs.HostMemoryGB = "", 7.75
	first := start(darwin, "declared", 7.75)
	complete(first, 3, true)
	s.Needs.Memory, s.Needs.HostMemoryGB = "auto", 0
	const linuxPeak = 3.634326934814453
	complete(start(linux, physicalMemoryProbe, 31.75), linuxPeak, false)
	if facts := job(t, q, first.ID).MemoryAttempts; len(facts) != 1 || facts[0].LearningReason != "constrained" || *facts[0].MemoryPeak.HostGB != 3 {
		t.Fatal("constrained diagnostic was not retained", facts)
	}
	for _, p := range q.s.WorkObservations {
		if p.Key.Target.OS == "darwin" && (!p.MemoryRecordedAt.IsZero() || p.MemoryPeak != nil) {
			t.Fatal("constrained diagnostic became learned memory", p)
		}
	}
	store := q.files.root
	q = memoryFactsRestart(t, q, clock)
	if err := q.ConfigureFileStore(store, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	check(t, q, linux, nil)
	previousSession := darwin.Session
	darwin.Session = "restarted-darwin-session"
	if _, err := q.Checkin(Checkin{Machine: darwin, Fresh: true, PreviousSession: previousSession}); err != nil {
		t.Fatal(err)
	}
	// The lower bound permits one full-allowance measurement, not a 3 GiB cap.
	complete(start(darwin, boundedMemoryExploration, 7.75), 4, false)
	// A larger accepted local peak must wait for enough allowance.
	complete(start(darwin, "accepted peak + measured uncertainty", 4+minimumHostMemoryGB), 8.5, false)
	queued := submit(t, q, s)
	why := job(t, q, queued.ID).Reason
	for _, want := range []string{memoryEvidenceRecovering, "8.62506103515625 GiB", "allowance 8 GiB"} {
		if !strings.Contains(why, want) {
			t.Errorf("wait %q missing %q", why, want)
		}
	}
	// The accepted source-OS peak and its original margin remain usable.
	complete(start(linux, "accepted peak + measured uncertainty", linuxPeak+minimumHostMemoryGB), linuxPeak, false)
}

func TestMemoryEnvelopeCrossMachineEstimate(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&first, true)
	if err := json.Unmarshal([]byte(`{"memory_peak":{"host_gb":2}}`), &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	m.ID, m.HostID = "other-machine", "other-host"
	m.Session = "session-other-machine"
	e := first.Extensions[0]
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "other-check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	s.AllowedMachines = []string{m.ID}
	// No bootstrap declaration; recipe defaults are not submitter hints.
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &s.Needs); err != nil {
		t.Fatal(err)
	}
	queued := submit(t, q, s)
	next := check(t, q, m, nil).Job
	if next == nil {
		t.Fatalf("cross-machine candidate refused: %s", job(t, q, queued.ID).Reason)
	}
	data, _ := json.Marshal(next)
	if next.Spec.Needs.HostMemoryGB != 32-minimumHostMemoryGB || !strings.Contains(string(data), "transfer error unavailable") {
		t.Fatalf("uncalibrated same-family transfer must remain physical exploration: %s", data)
	}
}

func TestMemoryEnvelopeSiblingHostBudget(t *testing.T) {
	for _, peak := range []float64{7, 13.08499526977539} {
		t.Run(fmt.Sprint(peak), func(t *testing.T) {
			q, clock, m, s := floorFreeFitFixture(t, 24)
			m.CapabilityProfile.SystemMemoryBytes = 31 << 30
			m.HostAvailableMemoryGB = 31
			sibling := m.Devices[0]
			sibling.ID, sibling.Identity = "1", "sibling-card"
			sibling.ExtensionFingerprint = "sibling-backend-v1"
			e := registeredExtension(t, q, s.Extension)
			target := sibling.machine(m)
			r := newExtensionReport(e, &target, "extension:sample")
			r.Setup, r.Qualified, r.CheckID = true, true, "sibling-check"
			sibling.ExtensionReports = []ExtensionReport{r}
			m.Devices = append(m.Devices, sibling)
			machineCheck(t, q, m)
			for id, p := range q.s.WorkObservations {
				delete(q.s.WorkObservations, id)
				p.Key.Component, p.Key.Hardware, p.Key.Fingerprint = sibling.ID, sibling.Identity, sibling.ExtensionFingerprint
				p.MemoryPeak.HostGB = &peak
				q.s.WorkObservations[identityHash(p.Key)] = p
			}
			s.RequireTarget = true
			s.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
			first := submit(t, q, s)
			a := assignedJob(machineCheck(t, q, m))
			if a == nil || a.ID != first.ID || math.Abs(a.Spec.Needs.HostMemoryGB-(peak+minimumHostMemoryGB)) > 1e-9 {
				t.Fatalf("same-host estimate was not admitted: job=%+v", a)
			}
			clock.Advance(time.Second)
			s.Needs.GPU = &GPUSelection{Devices: []string{"1"}}
			second := submit(t, q, s)
			b := assignedJob(machineCheck(t, q, m, a))
			wantReserved := (peak + minimumHostMemoryGB)
			if 2*wantReserved > 24 {
				if b != nil || job(t, q, second.ID).State != "queued" {
					t.Fatalf("sibling spent RAM beyond the shared allowance: %+v", b)
				}
			} else {
				if b == nil || b.ID != second.ID || math.Abs(b.Spec.Needs.HostMemoryGB-(peak+minimumHostMemoryGB)) > 1e-9 {
					t.Fatalf("fitting sibling did not get its own reservation: %+v", b)
				}
				wantReserved *= 2
			}
			if got := q.s.Machines[m.ID].HostMemoryInUseGB; math.Abs(got-wantReserved) > 1e-9 {
				t.Fatalf("host ledger charged %.12f GiB, want %.12f", got, wantReserved)
			}
			finish(t, q, a, true)
			if b == nil {
				b = assignedJob(machineCheck(t, q, m))
				if b == nil || b.ID != second.ID {
					t.Fatal("sibling did not start after the first reservation was released")
				}
			}
			if got := q.s.Machines[m.ID].HostMemoryInUseGB; math.Abs(got-(peak+minimumHostMemoryGB)) > 1e-9 {
				t.Fatalf("release removed the sibling's reservation: %.12f GiB", got)
			}
		})
	}
}

func TestMemoryEnvelopeExploratoryFirstRun(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	setLearnedMemory(t, q, false) // Host-wide exploration fences (#585 bounds by default).
	s.Needs.Memory = "auto"
	m.HostAvailableMemoryGB = 0 // No measured physical headroom.
	j := submit(t, q, s)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("exploration admitted without large free headroom")
	}
	// Old release custody remains protected even with half the host free.
	m.HostAvailableMemoryGB = 48
	q.s.HostReleases = map[string]HostRelease{"memory-heavy": {Host: m.HostID, Job: "old-attempt", HostChargeGB: 16}}
	if check(t, q, m, nil).Job != nil || !strings.Contains(job(t, q, j.ID).Reason, "unconfirmed memory-heavy release") {
		t.Fatal("exploration ignored unconfirmed memory-heavy custody")
	}
	delete(q.s.HostReleases, "memory-heavy")
	m.HostAvailableMemoryGB = 32 // This telemetry already excludes the host reserve.
	first := check(t, q, m, nil).Job

	if first == nil || first.Spec.Needs.HostMemoryGB != 32-minimumHostMemoryGB || !strings.Contains(first.MemoryEnvelope.Source, "exploratory first run") {
		t.Fatalf("no-hint first run must leave launch drift headroom: %+v; %s", first, job(t, q, j.ID).Reason)
	}
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(first, true)
	host := 2.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	next := startObservation(t, q, m, s)

	if next.Spec.Needs.HostMemoryGB != 2+minimumHostMemoryGB || !strings.Contains(next.MemoryEnvelope.Source, "accepted peak + measured uncertainty") {
		t.Fatalf("exploration did not become a measurement: %+v", next)
	}
}

func TestMemoryEnvelopeExploratoryLaunchWithRAMDrift(t *testing.T) {
	q, clock := fixture(t)
	setLearnedMemory(t, q, false) // Host-wide exploration fences (#585 bounds by default).
	m := cpuMachine("drift", "drift-host", "cpu", 8, 32)
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 64, .125
	check(t, q, m, nil)
	s := cpuSpec(1, 0)
	s.Needs.Memory = "auto"
	s.Run.Command = []string{"true"}
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != queued.ID || !strings.Contains(j.MemoryEnvelope.Source, "exploratory") {
		t.Fatal("exploratory job did not dispatch", j)
	}
	w := recoveryWorker(t)
	w.Machine, w.Clock = m, clock
	w.memoryCeilings = fakeCeilings(t)
	free := m.HostAvailableMemoryGB - .0625
	w.HostMemorySource = func(context.Context) (float64, error) { return free, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	var p *process
	await(t, func() bool {
		p, err = w.start(store, *j)
		return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
	})
	if err != nil {
		t.Fatalf("small RAM drift refused exploratory launch: %v", err)
	}
	if f := <-p.done; f.err != nil || !f.result.Success {
		t.Fatalf("exploratory payload failed: %+v", f)
	}
	// Keep the check strict, and retain the exact values for a sub-MiB gap.
	need := hostNeed(&m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	free = need - 1.0/(1<<20)
	err = w.checkHostRAM(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	if !errors.Is(err, errWorkerMemory) || !strings.Contains(err.Error(), "need "+strconv.FormatFloat(need, 'f', -1, 64)+" GiB, available "+strconv.FormatFloat(free, 'f', -1, 64)+" GiB") {
		t.Fatalf("genuine shortage lacks exact values: %v", err)
	}
}

func TestMemoryEnvelopeUnifiedExploratoryGPUDrift(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, .25)
	// Device room, rather than host headroom, limits the exploratory GPU share.
	m.MemoryCeilingSlackGB = 0
	m.Devices[0].BudgetGB, m.Devices[0].FreeMemoryGB = 59, 59
	queued := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m))
	if j == nil || j.ID != queued.ID {
		t.Fatalf("unified exploration not assigned: %s", job(t, q, queued.ID).Reason)
	}
	w := recoveryWorker(t)
	w.Machine, w.Clock = *allocationMachine(&m, j.AllocatedDevices), clock
	d := m.Devices[0]
	w.selectedDevice = &d
	w.DeviceBinding = d.Selector
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return []MachineDevice{d}, nil }
	w.nativeGPUDeviceProbe = syntheticNativeGPUDeviceProbe(d)
	w.memoryCeilings = fakeCeilings(t)
	// The declared 64 GiB allowance leaves a 64 GiB physical reserve.
	w.hostRawMemorySource = func(context.Context) (float64, error) { return 128, nil }
	w.HostMemorySource = func(context.Context) (float64, error) { return 100, nil }
	w.MemorySource = func(context.Context) (float64, error) { return 59 - .0625, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	// Exercise the real strict GPU launch check with a small plain payload.
	payload := *j
	payload.Spec = Spec{Run: RunSpec{Command: []string{"true"}}, Needs: j.Spec.Needs}
	payload.Extensions, payload.Extension = nil, nil
	p, err := startAfterFixtureInputs(t, w, store, payload)
	if err != nil {
		t.Fatalf("small GPU drift refused exploratory launch: %v", err)
	}
	if f := <-p.done; f.err != nil || !f.result.Success {
		t.Fatalf("exploratory payload failed: %+v", f)
	}
}

func unifiedAutoFixture(t *testing.T, memberCap float64) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "cuda"}
	v.Needs = ResourceNeeds{MemoryGB: .0625, HostMemoryGB: .015625}
	v.Execution.Main.MemoryGB = memberCap
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("unified", "shared-host", "cuda", 8, 101)
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 64, .125
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.CapabilityProfile.Arch, m.CapabilityProfile.MemoryLayout = "arm64", "unified"
	m.CapabilityProfile.SystemMemoryBytes = 128 << 30
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 128}
	d := MachineDevice{ID: "0", Platform: "cuda", Identity: "synthetic-gb10", Selector: "0", MemoryMode: "unified", ReserveMemoryGB: 16, BudgetGB: 101, FreeMemoryGB: 101, UnifiedMemory: true, MemoryMeasured: true, ExtensionFingerprint: "unified-v1"}
	target := d.machine(m)
	report := newExtensionReport(e, &target, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "check"
	d.ExtensionReports = []ExtensionReport{report}
	m.Devices = []MachineDevice{d}
	machineCheck(t, q, m)
	s := Spec{Name: "memory", Project: "synthetic", RequireTarget: true, Extension: extensionKey(e), Needs: ResourceNeeds{Memory: "auto"}, Workload: &Workload{InputShape: "small-v1", Units: 10}}
	return q, clock, m, s
}

func TestMemoryEnvelopeUnifiedCappedAutoStarts(t *testing.T) {
	q, _, m, s := unifiedAutoFixture(t, .25)
	queued := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m))
	if j == nil || j.ID != queued.ID {
		t.Fatalf("capped auto job did not qualify and dispatch: %s", job(t, q, queued.ID).Reason)
	}
	if j.MemoryEnvelope == nil || j.MemoryEnvelope.Refusal != "" || !strings.Contains(j.MemoryEnvelope.Source, "exploratory") || j.Spec.Needs.HostMemoryGB < .25 || j.Spec.Needs.MemoryGB <= 0 || j.MemoryEnvelope.HostGB > 64 {
		t.Fatalf("capped exploration exceeds shared allowance: %+v", j)
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "running" || got.StartedAt == nil {
		t.Fatalf("capped auto job did not start: %+v", got)
	}
	// A member floor larger than all shared headroom must still refuse.
	finish(t, q, j, true)
	machineCheck(t, q, m)
	probe := *j
	probe.State = "queued"
	probe.Spec.Run.MemoryGB = 65
	target := allocationMachine(q.s.Machines[m.ID], j.AllocatedDevices)
	if candidate := q.memoryCandidate(&probe, target); candidate.MemoryEnvelope.Refusal == "" {
		t.Fatalf("impossible member floor explored: %+v", candidate.MemoryEnvelope)
	}
}

func TestMemoryEnvelopeUnifiedAutoReusesPeakAcrossLiveDrift(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	for run := 0; run < 2; run++ {
		queued := submit(t, q, s)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("solo CUDA run %d not assigned: %s", run, job(t, q, queued.ID).Reason)
		}
		if run == 1 && (j.MemoryEnvelope.Source != "accepted peak + measured uncertainty" || j.Spec.Needs.HostMemoryGB != .125+minimumHostMemoryGB || j.Spec.Needs.MemoryGB != .25+minimumHostMemoryGB || j.MemoryEnvelope.HostGB != .5+2*minimumHostMemoryGB) {
			t.Fatalf("second CUDA run did not sum learned host + GPU + slack: %+v", j.MemoryEnvelope)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		// Uncapped unified inventory currently reports live room as BudgetGB.
		m.HostAvailableMemoryGB--
		m.Devices[0].BudgetGB, m.Devices[0].FreeMemoryGB = m.HostAvailableMemoryGB, m.HostAvailableMemoryGB
		machineCheck(t, q, m, j)
		clock.Advance(time.Second)
		host, device := .125, .25
		r := result(j, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if timing := job(t, q, j.ID).Timing; timing.Reason != "" {
			t.Fatalf("successful solo CUDA run lost comparability: %+v", timing)
		}
	}
}

func TestMemoryEnvelopeUnifiedHeadroom(t *testing.T) {
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	e.Variants[0].Target.OS = "linux"
	e.Variants[0].Target.Arch = "amd64"
	e.Variants[0].Needs = ResourceNeeds{MemoryGB: 8, HostMemoryGB: 4}
	delete(e.Variants[0].Baselines, "kind:sample")
	e.Variants[0].References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := machine("unified", "fake", "shared-host")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.HostMemoryBudgetGB = true, 32, 32
	m.MemoryCeilingSlackGB = .125
	m.CapabilityProfile.MemoryLayout = "unified"
	d := MachineDevice{ID: "0", Platform: "fake", Identity: "shared-device", Selector: "0", MemoryMode: "unified", TotalMemoryGB: 64, BudgetGB: 64, FreeMemoryGB: 64, UnifiedMemory: true, MemoryMeasured: true, ExtensionFingerprint: "unified-v1"}
	target := m
	target.Device = "0"
	target.ExtensionFingerprint = d.ExtensionFingerprint
	report := newExtensionReport(e, &target, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "check"
	d.ExtensionReports = []ExtensionReport{report}
	m.Devices = []MachineDevice{d}
	machineCheck(t, q, m)
	s := Spec{Name: "memory", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	queuedFirst := submit(t, q, s)
	firstReply := machineCheck(t, q, m)
	if assignedJob(firstReply) == nil {
		t.Fatalf("not assigned: %s", job(t, q, queuedFirst.ID).Reason)
	}
	first := *assignedJob(firstReply)
	if err := q.Started(result(&first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(&first, true)
	if err := json.Unmarshal([]byte(`{"memory_peak":{"host_gb":10,"device_gb":8}}`), &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	auto := s
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &auto.Needs); err != nil {
		t.Fatal(err)
	}
	m.HostAvailableMemoryGB = 18.125
	j := submit(t, q, auto)
	if assignedJob(machineCheck(t, q, m)) != nil {
		t.Fatal("unified peak admitted without existing slack")
	}
	m.HostAvailableMemoryGB = 18.125 + 2*minimumHostMemoryGB
	assigned := assignedJob(machineCheck(t, q, m))
	if assigned == nil || assigned.ID != j.ID {
		t.Fatalf("conservative host + GPU reservation did not fit: %s", job(t, q, j.ID).Reason)
	}
	if assigned.Spec.Needs.MemoryGB != 8+minimumHostMemoryGB || assigned.Spec.Needs.HostMemoryGB != 10+minimumHostMemoryGB {
		t.Fatalf("wrong unified vector: %+v", assigned.Spec.Needs)
	}
	// No auto opt-in: production keeps the existing 8 + 4 + slack declaration.
	finish(t, q, assigned, true)
	s.Priority = "production"
	productionID := submit(t, q, s)
	production := assignedJob(machineCheck(t, q, m))
	if production == nil || production.ID != productionID.ID {
		t.Fatal("explicit production refused")
	}
	if production.Spec.Needs.MemoryGB != 8 || production.Spec.Needs.HostMemoryGB != 4 {
		t.Fatal("production declarations tightened")
	}

	t.Run("MemoryEnvelopeTotalPeakKeepsDeviceSeparate", func(t *testing.T) {

		q, clock, m, s := unifiedAutoFixture(t, 0)
		start := func() *Job {
			t.Helper()
			queued := submit(t, q, s)
			j := assignedJob(machineCheck(t, q, m))
			if j == nil || j.ID != queued.ID {
				t.Fatalf("not assigned: %+v", job(t, q, queued.ID))
			}
			if err := q.Started(result(j, false)); err != nil {
				t.Fatal(err)
			}
			return j
		}
		first := start()
		clock.Advance(time.Second)
		host, total, device := 8.0, 12.0, 4.0
		r := result(first, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, DeviceGB: &device}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		next := start()
		// Unified accounting still adds the separate device reservation and slack.
		if next.MemoryEnvelope.Source != "accepted peak + measured uncertainty" || next.Spec.Needs.HostMemoryGB != 12+minimumHostMemoryGB || next.Spec.Needs.MemoryGB != 4+minimumHostMemoryGB || next.MemoryEnvelope.HostGB != 16.125+2*minimumHostMemoryGB {
			t.Fatalf("total host/device peaks mixed: %+v; %+v", next.MemoryEnvelope, next.Spec.Needs)
		}

	})
}

type envelopePeakFS struct {
	fakeMemoryFS
	oom bool
}

func (f envelopePeakFS) Mkdir(p string) error {
	if err := f.fakeMemoryFS.Mkdir(p); err != nil {
		return err
	}
	if strings.HasPrefix(filepath.Base(p), "attempt-") {
		if err := os.WriteFile(filepath.Join(p, "memory.peak"), []byte("2147483648"), 0600); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(p, "memory.stat"), []byte("anon 2147483648\nkernel 0\nshmem 0\n"), 0600); err != nil {
			return err
		}
		if f.oom {
			return os.WriteFile(filepath.Join(p, "memory.events"), []byte("oom 1\noom_kill 1\n"), 0600)
		}
	}
	return nil
}

func TestMemoryEnvelopeNativeGroupPeak(t *testing.T) {
	_, clock := fixture(t)
	w := recoveryWorker(t)
	w.Clock = clock
	w.memoryCeilings = fakeCeilings(t)
	w.memoryCeilings.fs = envelopePeakFS{}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"true"})
	j.Spec.Resumable = false
	j.Spec.Needs.HostMemoryGB = 4
	j.Spec.Needs.Platform = Platform{Required: []string{"cpu"}}
	j.Spec.Needs.MemoryGB = 0
	j.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "60"}}}
	var p *process
	await(t, func() bool {
		p, err = w.start(store, j)
		return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
	})
	if err != nil {
		t.Fatal(err)
	}
	r := d5Finish(t, p)
	data, _ := json.Marshal(r)
	if !r.Success || r.ReleasePending || !strings.Contains(string(data), `"host_gb":2`) {
		t.Fatalf("whole group peak missing: %s", data)
	}
}

// A previously accepted explicit-cap peak must stop seeding auto once a
// comparable explicit attempt fails at its ceiling.
func TestMemoryEnvelopeExplicitPressureInvalidatesSeed(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.SizeHint.HostMemoryGB = 4
	seed := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	host := 4.0 // Models a pre-#184 cap-bound sample stored as unconstrained.
	r := result(&seed, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	bound := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r = result(&bound, false)
	r.MemoryPeak = envelopeCeilingPeak(t, host)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	s.Needs.Memory = "auto"
	s.SizeHint = SizeHint{} // An old declaration must not act as a submitter hint.
	next := startObservation(t, q, m, s)
	if next.MemoryEnvelope.Source != acceptedMemoryEscalation || next.MemoryEnvelope.HostGB != host+2*minimumHostMemoryGB {
		t.Errorf("constrained explicit attempt left its old seed usable: %+v", next.MemoryEnvelope)
	}
	// Subsequent failed auto pressure still invalidates comparable evidence.
	clock.Advance(time.Second)
	r = result(&next, false)
	r.MemoryPeak = envelopeCeilingPeak(t, next.MemoryEnvelope.HostGB)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	third := startObservation(t, q, m, s)
	// Without measured growth, advance the censored demand by a kernel page;
	// current free RAM cannot replace the invalidated demand estimate.
	if third.MemoryEnvelope.Source != acceptedMemoryEscalation || third.MemoryEnvelope.HostGB != next.MemoryEnvelope.HostGB+minimumHostMemoryGB {
		t.Errorf("auto pressure failed to clear the explicit seed: %+v", third.MemoryEnvelope)
	}
}

func envelopeCeilingPeak(t *testing.T, capGB float64) *AttemptMemoryPeak {
	t.Helper()
	a, err := fakeCeilings(t).create("attempt-bound", int64(capGB*(1<<30)), 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"memory.stat":   "anon " + strconv.FormatInt(int64(capGB*(1<<30)), 10) + "\nkernel 0\nshmem 0\n",
		"memory.events": "max 9907\nhigh 0\noom 0\noom_kill 0\n",
	} {
		if err := os.WriteFile(filepath.Join(a.path, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	peak := a.peak()
	if peak == nil || !peak.Constrained {
		t.Fatal("resident ceiling pressure was not classified")
	}
	return peak
}

// A qualified floor-free variant with an accepted peak on the fake clock.
func floorFreeFitFixture(t *testing.T, allowance float64) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "fake"}
	v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("sized", "sized-host", "fake", 8, 64)
	m.TotalMemoryGB = 8
	m.ExtensionFingerprint = "synthetic-fit-v1"
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.HostMemoryBudgetGB = allowance
	if allowance > 0 {
		m.MemoryEnforcement = ""
	}
	m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "synthetic-card", Fingerprint: "synthetic-card-v1", ExtensionFingerprint: m.ExtensionFingerprint, Selector: "0", MemoryMode: "fixed", MemoryMeasured: true, TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	target := m.Devices[0].machine(m)
	r := newExtensionReport(e, &target, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "synthetic-check"
	m.Devices[0].ExtensionReports = []ExtensionReport{r}
	machineCheck(t, q, m)
	s := Spec{Name: "floor-free", Project: "synthetic", Priority: "experiment", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	probe := &Job{State: "queued", Spec: s, Extensions: []Extension{e}, Machine: m.ID, Session: m.Session, AllocatedDevices: []string{"0"}}
	target = m.Devices[0].machine(*q.s.Machines[m.ID])
	candidates := extensionJobs(probe, &target, true)
	if len(candidates) != 1 {
		t.Fatal("synthetic variant must qualify")
	}
	probe = candidates[0]
	key, why := q.workObservationKey(probe)
	if key == nil {
		t.Fatal(why)
	}
	host, device := 2.0, 3.0
	q.s.WorkObservations = map[string]WorkObservation{identityHash(key): {
		Key: *key, Source: "accepted work", Samples: 1, RecordedAt: clock.Now(), MemoryRecordedAt: clock.Now(), MemoryLayout: "separate",
		MemoryConfiguration: memoryConfiguration(probe.Spec.Needs, probe.Spec.Args, probe.Spec.Program), MemoryPeak: &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device},
	}}
	return q, clock, m, s
}

func TestMemoryEnvelopePerItemProfile(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	m.CPUExtensionFingerprint = "profile-cpu-v1"
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 16, 0.1
	machineCheck(t, q, m)
	s := cpuSpec(1, 0)
	s.Name, s.Project, s.RequireTarget, s.Needs.Memory = "items", "synthetic", true, "auto"
	s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
	s.ProfileMemory = strings.Repeat("a", 64)
	// Distinct source/item bytes and appended commands, with a shared base digest.
	for i := 0; i < 2; i++ {
		data := []byte(fmt.Sprintf("item-%d", i))
		path := filepath.Join(t.TempDir(), "item")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
			t.Fatal(err)
		}
		s.Files = FileManifest{".myce-source.tar": ref, ".myce-inputs/item": ref}
		s.Run.Command = []string{"sh", "-ec", fmt.Sprintf("process item-%d", i)}
		queued := submit(t, q, s)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatal("profile not assigned", queued)
		}
		expected := "accepted peak + measured uncertainty"
		if i == 0 {
			expected = physicalMemoryProbe
		}
		if j.MemoryEnvelope == nil || j.MemoryEnvelope.Source != expected {
			t.Fatalf("item %d estimate: %+v", i, j.MemoryEnvelope)
		}
		if err = q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		done := result(j, true)
		peak := 2.0
		done.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
		if err = q.Complete(done); err != nil {
			t.Fatal(err)
		}
	}

	s.ProfileMemory = strings.Repeat("b", 64) // Changed base configuration or per_item identity.
	queued := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m))
	if j == nil || j.ID != queued.ID || j.MemoryEnvelope == nil || j.MemoryEnvelope.Source != "exploratory first run (bounded: accepted family peaks + measured uncertainty)" || j.MemoryEnvelope.Basis != "exploratory" {
		t.Fatalf("changed profile reused old memory: %+v", j.MemoryEnvelope)
	}
}

// #303: the enforced parent charges file-backed memory too. Resident-only
// headroom reserved less than an already measured total, then accepted the
// ceiling-bound successor because its resident peak stayed near 80% of the cap.
func TestMemoryEnvelopeTotalPeakAndFileBackedPressure(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
	s.Needs.Memory = "auto"
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	host, total := 8.0, 12.0
	r := result(&first, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	bound := startObservation(t, q, m, s)
	if bound.MemoryEnvelope.Source != "accepted peak + measured uncertainty" || bound.MemoryEnvelope.HostGB != 12+minimumHostMemoryGB {
		t.Errorf("envelope below measured total with headroom: %+v", bound.MemoryEnvelope)
	}
	clock.Advance(time.Second)
	total = bound.MemoryEnvelope.HostGB
	r = result(&bound, true)
	// An older worker can send max events with Constrained still false.
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, Events: &MemoryEventCounts{Max: 2004}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if job(t, q, bound.ID).State != "succeeded" {
		t.Fatal("pressure changed successful outcome")
	}
	next := startObservation(t, q, m, s)
	if next.MemoryEnvelope.Source != "accepted peak + measured uncertainty" || next.MemoryEnvelope.HostGB != 12+minimumHostMemoryGB {
		t.Errorf("successful constrained sample changed accepted envelope: %+v", next.MemoryEnvelope)
	}
	for _, point := range q.s.WorkObservations {
		if point.MemoryRecordedAt.IsZero() || point.Forbidden || !validRate(point.Rate) || *point.MemoryPeak.CgroupGB != 12 {
			t.Errorf("rejected sample must preserve accepted memory and throughput: %+v", point)
		}
	}
	clock.Advance(time.Second)
	r = result(&next, true)
	total = 12
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, Events: &MemoryEventCounts{}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	relearned := startObservation(t, q, m, s)
	if relearned.MemoryEnvelope.Source != "accepted peak + measured uncertainty" || relearned.MemoryEnvelope.HostGB != 12+minimumHostMemoryGB {
		t.Errorf("unconstrained exploration did not re-teach total peak: %+v", relearned.MemoryEnvelope)
	}

	t.Run("MemoryEnvelopeStoredFileBackedPressure", func(t *testing.T) {

		q, clock, m, s := observationFixture(t)
		m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
		s.Needs.Memory = "auto"
		first := startObservation(t, q, m, s)
		clock.Advance(time.Second)
		host, total := 8.0, 10.0
		r := result(&first, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		// Model the persisted curve accepted by the previous resident-only rule.
		for id, point := range q.s.WorkObservations {
			point.MemoryPeak.Events = &MemoryEventCounts{Max: 2004}
			q.s.WorkObservations[id] = point
		}
		next := startObservation(t, q, m, s)
		if next.MemoryEnvelope.Source != "exploratory (previous peak constrained)" {
			t.Errorf("stored cap-bound curve was reused: %+v", next.MemoryEnvelope)
		}
		clock.Advance(time.Second)
		r = result(&next, true)
		total = 12
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, Events: &MemoryEventCounts{}}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		relearned := startObservation(t, q, m, s)
		if relearned.MemoryEnvelope.Source != "accepted peak + measured uncertainty" || relearned.MemoryEnvelope.HostGB != 12+minimumHostMemoryGB {
			t.Errorf("stored pressure poisoned fresh learning: %+v", relearned.MemoryEnvelope)
		}

	})

	t.Run("MemoryEnvelopeGPUKeepsTotalPeakWithComponentSamples", func(t *testing.T) {

		q, clock, m, s := unifiedAutoFixture(t, 0)
		start := func() *Job {
			t.Helper()
			queued := submit(t, q, s)
			j := assignedJob(machineCheck(t, q, m))
			if j == nil || j.ID != queued.ID {
				t.Fatal("GPU attempt not assigned")
			}
			if err := q.Started(result(j, false)); err != nil {
				t.Fatal(err)
			}
			return j
		}
		for range 2 {
			j := start()
			clock.Advance(time.Second)
			r := result(j, true)
			host, total, device := 2.0, 19.918118, 22.318359
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &total, DeviceGB: &device, Events: &MemoryEventCounts{}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
		}
		next := start()
		if got, want := next.Spec.Needs.HostMemoryGB, 19.918118+minimumHostMemoryGB; math.Abs(got-want) > 1e-9 {
			t.Fatalf("GPU host-backed storage discarded: got %v want %v", got, want)
		}

	})
}

// Real-shaped resident/total pairs replayed on CPU work. GPU work separately
// retains total-peak sizing until driver host-memory accounting is qualified.
func TestMemoryEnvelopeMeasuredRangeAdmissionAndPressure(t *testing.T) {
	q, clock, m, base := observationFixture(t)
	m.Devices = []MachineDevice{}
	m.CPUExtensionFingerprint = m.ExtensionFingerprint
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 24, 28
	budget := 24.0
	if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "drain", MemoryBudgetGB: &budget}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance(hostID(&m), HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	base.Needs.Memory = "auto"
	specs := []Spec{base, base}
	specs[0].Args, specs[1].Args = []string{"separation"}, []string{"asr"}
	samples := [][2]float64{{7.645931243896484, 7.8166961669921875}, {3.725048065185547, 4.041187286376953}}
	totals := []float64{13.08499526977539, 9.590682983398438}
	wants := []float64{7.8166961669921875 + (7.8166961669921875 - 7.645931243896484), 4.041187286376953 + (4.041187286376953 - 3.725048065185547)}
	for i, spec := range specs {
		for n, host := range samples[i] {
			j := startObservation(t, q, m, spec)
			if n == 1 && j.Spec.Needs.HostMemoryGB != totals[i]+minimumHostMemoryGB {
				t.Fatalf("single peak cannot measure variance: %+v", j.MemoryEnvelope)
			}
			clock.Advance(time.Second)
			r := result(&j, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &totals[i], Events: &MemoryEventCounts{}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			facts := job(t, q, j.ID).MemoryAttempts
			if len(facts) != 1 || facts[0].MemoryPeak.NonreclaimableGB == nil || *facts[0].MemoryPeak.NonreclaimableGB != host {
				t.Fatal("completion lost attempt-specific component peak")
			}
		}
	}
	// Round-trip through SQLite, not just an in-memory aggregate.
	var database string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(database, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	q = reopened
	check(t, q, m, nil) // Fresh inventory revalidates persisted memory identities.
	running := make([]Job, 2)
	for i, spec := range specs {
		queued := submit(t, q, spec)
		var reports []*Job
		if i > 0 {
			reports = append(reports, &running[0])
		}
		assigned := assignedJob(machineCheck(t, q, m, reports...))
		if assigned == nil || assigned.ID != queued.ID {
			t.Fatalf("concurrent work not admitted: %+v", assigned)
		}
		if err := q.Started(result(assigned, false)); err != nil {
			t.Fatal(err)
		}
		running[i] = *assigned
		got := running[i].Spec.Needs.HostMemoryGB
		if math.Abs(got-wants[i]) > 1e-9 || !strings.Contains(running[i].MemoryEnvelope.Source, "measured range") {
			t.Fatalf("wrong measured envelope: %+v; want %.9f", running[i].MemoryEnvelope, wants[i])
		}
	}
	if got := q.s.Machines[m.ID].HostMemoryInUseGB; got < wants[0]+wants[1] || got > 24 {
		t.Fatalf("shared host ledger did not admit both inside allowance: %.9f", got)
	}
	clock.Advance(time.Second)
	r := result(&running[0], false)
	host := samples[0][1]
	// A failed old-worker attempt can omit the flag even when max events prove
	// pressure. Failed-attempt invalidation still discards the reduced range.
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &totals[0], Events: &MemoryEventCounts{Max: 2004}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	probe := *q.s.Jobs[running[0].ID]
	probe.State = "queued"
	estimate := q.memoryEstimate(&probe, q.s.Machines[m.ID])
	if estimate.host == nil || !estimate.escalation || estimate.samples != 0 || estimate.measuredHostGB < totals[0] || estimate.hostFactor != 1 {
		t.Fatalf("pressure preserved a reduced estimate: %+v", estimate)
	}
	for _, point := range q.s.WorkObservations {
		if point.MemoryPeak.constrained() && point.MemoryRange != nil {
			t.Fatal("pressure retained range")
		}
	}
}
