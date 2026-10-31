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

func TestMemoryEnvelopeMeasuredAccepted(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(2 * time.Second)
	r := result(&first, true)
	if err := json.Unmarshal([]byte(`{"memory_peak":{"host_gb":2}}`), &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// Omitted mode preserves explicit production/bootstrap declarations.
	auto := s
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &auto.Needs); err != nil {
		t.Fatal(err)
	}
	next := startObservation(t, q, m, auto)
	if next.Spec.Needs.HostMemoryGB != 2.5 {
		t.Fatalf("peak × 1.25 not reserved: %+v", next.Spec.Needs)
	}
}

func TestMemoryEnvelopeCeilingBoundThenExploratory(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		success    bool
	}{
		{"auto success", "auto", true},
		{"auto failure", "auto", false},
		{"explicit success", "", true},
	} {
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
			if tc.mode == "auto" && bound.MemoryEnvelope.Source != "accepted peak × 1.25" {
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
			wantSource := "exploratory first run"
			if tc.mode == "auto" {
				wantSource = "exploratory (previous peak constrained)"
			}
			if tc.success && tc.mode == "auto" {
				if next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.Spec.Needs.HostMemoryGB != 2.5 {
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
	first := start("exploratory first run", 0)
	if owner := q.s.Machines[m.ID]; len(owner.Devices) != 2 || owner.ReservedMemoryGB != 1 {
		t.Fatalf("missing multi-device GPU reserve: %+v", owner)
	}
	complete(first, 2)
	next := start("accepted peak × 1.25", 2.5)
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
	complete(start("exploratory first run", 0), 8)
	start("accepted peak × 1.25", 10)
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
		if timing := q.s.Jobs[j.ID].Timing; timing != nil && (timing.PredictedSeconds != nil || timing.ActualSeconds != nil) {
			t.Fatalf("plain profile learned timing: %+v", timing)
		}
	}
	setSource("commit one")
	first := start("exploratory first run", 0)
	busy := submit(t, q, s)
	if busy.State != "queued" || assignedJob(machineCheck(t, q, m, first)) != nil {
		t.Fatal("busy auto profile must queue without dispatching", busy)
	}
	if err := q.Cancel(busy.ID); err != nil {
		t.Fatal(err)
	}
	complete(first, 2)
	complete(start("accepted peak × 1.25", 2.5), 2)
	setSource("commit two")
	oom := start("accepted peak × 1.25", 2.5)
	r := result(oom, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(oom.ID); err != nil {
		t.Fatal(err)
	}
	complete(start("exploratory first run", 0), 4)
	complete(start("accepted peak × 1.25", 5), 4)
	s.Run.Command = []string{"sh", "-ec", "tar -xf .myce-source.tar\nbash scripts/other-suite.sh"}
	complete(start("exploratory first run", 0), 1)
	for _, point := range q.s.WorkObservations {
		if point.StaleReason != "" || point.Rate != 0 || point.Seconds != 0 || point.Units != 0 || point.LatencySeconds != 0 {
			t.Fatalf("profile memory observation contains stale/timing data: %+v", point)
		}
	}
	s.Needs.Memory, s.Needs.HostMemoryGB = "", 3
	complete(start("declared", 3), 1)
	// A first run can fit while its learned peak plus safety margin cannot.
	s.Needs.Memory, s.Needs.HostMemoryGB = "auto", 0
	s.Run.Command = []string{"sh", "-ec", "bash scripts/memory-heavy-suite.sh"}
	complete(start("exploratory first run", 0), 13)
	for _, mode := range []string{"auto", ""} {
		s.Needs.Memory = mode
		s.Needs.HostMemoryGB = 17
		if mode == "auto" {
			s.Needs.HostMemoryGB = 0 // The rejection must come from the learned peak.
		}
		_, err := q.Submit(s)
		var api *HTTPError
		if !errors.As(err, &api) || api.Code != "no_qualified_target" {
			t.Fatalf("oversized %q memory must retain exit-78 rejection: %v", mode, err)
		}
	}
}

// #519: an accepted Linux cgroup peak times 2.5 used to strand Darwin
// despite its only local peak being constrained (and therefore no upper bound).
func TestMemoryEnvelopeCPUForeignOSPeak(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(q.files.blob(ref), []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	linux := cpuMachine("linux-worker", "linux-host", "cpu", 8, 32)
	darwin := cpuMachine("darwin-worker", "darwin-host", "cpu", 8, 32)
	darwin.CapabilityProfile.OS, darwin.CapabilityProfile.Arch = "darwin", "arm64"
	for _, m := range []*Machine{&linux, &darwin} {
		m.ExtensionFingerprint = "cpu-v1"
		m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 8, .125
		check(t, q, *m, nil)
	}
	s := cpuSpec(2, 0)
	s.Name, s.RequireTarget, s.Needs.Memory = "gate-full", true, "auto"
	s.Files = FileManifest{".myce-source.tar": ref}
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
	const linuxPeak = 3.634326934814453
	complete(start(linux, "exploratory first run", 7.75), linuxPeak, false)
	// A foreign peak alone must not prevent a qualified local exploration.
	first := start(darwin, "exploratory first run", 7.75)
	complete(first, 2, true)
	if facts := job(t, q, first.ID).MemoryAttempts; len(facts) != 1 || facts[0].LearningReason != "constrained" || *facts[0].MemoryPeak.HostGB != 2 {
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
	check(t, q, darwin, nil)
	// Restart must neither revive the foreign estimate nor turn 2 GiB into a cap.
	complete(start(darwin, "exploratory first run", 7.75), 4, false)
	complete(start(darwin, "accepted peak × 1.25", 5), 2, true)
	// Rejected local success cannot erase the accepted 4 GiB peak (#490).
	complete(start(darwin, "accepted peak × 1.25", 5), 6.5, false)
	_, err = q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "no_qualified_target" {
		t.Fatalf("real local requirement must still refuse: %v", err)
	}
	for _, want := range []string{"no qualified target can fit this job", "8.25 GiB", "allowance 8 GiB", "reduce the workload", "clean target-platform measurement", "operator approve", "retrying unchanged cannot fix capacity"} {
		if !strings.Contains(api.Message, want) {
			t.Errorf("refusal %q missing %q", api.Message, want)
		}
	}
	// The accepted source-OS peak and its original margin remain usable.
	complete(start(linux, "accepted peak × 1.25", linuxPeak*1.25), linuxPeak, false)
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
	if next.Spec.Needs.HostMemoryGB != 3.75 || !strings.Contains(string(data), "cross-machine estimate") {
		t.Fatalf("same-family peak must use 1.25 × 1.5: %s", data)
	}
	// The same accepted recipe on a different family gets the larger margin.
	for id, point := range q.s.WorkObservations {
		point.Key.Target.Arch = "arm64"
		q.s.WorkObservations[id] = point
	}
	probe := *q.s.Jobs[next.ID]
	probe.State = "queued"
	estimate := q.memoryCandidate(&probe, q.s.Machines[m.ID])
	if estimate.Spec.Needs.HostMemoryGB != 5 {
		t.Fatalf("other-family peak must use 1.25 × 2: %+v", estimate.Spec.Needs)
	}
}

func TestMemoryEnvelopeSiblingHostRAM(t *testing.T) {
	const resident, total, device = 7.719688415527344, 13.08499526977539, 4.374309539794922
	for _, variant := range []string{"same host", "different host", "changed fingerprint", "changed environment"} {
		t.Run(variant, func(t *testing.T) {
			q, clock, m, s := floorFreeFitFixture(t, 24)
			sibling := m.Devices[0]
			sibling.ID, sibling.Identity = "1", "sibling-card"
			sibling.ExtensionFingerprint = "sibling-kernel-v1"
			m.Devices = append(m.Devices, sibling)
			machineCheck(t, q, m)
			for id, p := range q.s.WorkObservations {
				delete(q.s.WorkObservations, id)
				p.Key.Component, p.Key.Hardware, p.Key.Fingerprint = sibling.ID, sibling.Identity, sibling.ExtensionFingerprint
				host, cgroup, gpu := resident, total, device
				p.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &cgroup, DeviceGB: &gpu}
				switch variant {
				case "different host":
					p.Key.Host = "other-host"
				case "changed fingerprint":
					p.Key.Fingerprint = "sibling-previous-kernel"
				case "changed environment":
					p.Key.Environment = "other-environment"
				}
				q.s.WorkObservations[identityHash(p.Key)] = p
			}
			clock.Advance(time.Second)
			target := m.Devices[0].machine(*q.s.Machines[m.ID])
			probe := &Job{State: "queued", Spec: s, Extensions: []Extension{registeredExtension(t, q, s.Extension)}}
			probe = extensionJobs(probe, &target, true)[0]
			estimate := q.memoryCandidate(probe, &target)
			factor := 1.25 * 1.5
			if variant == "same host" {
				factor = 1.25
				if !strings.Contains(estimate.MemoryEnvelope.Source, "same-host") || !hostFits(&target, estimate.Spec.Needs.MemoryGB, estimate.Spec.Needs.HostMemoryGB) {
					t.Fatalf("same-host sibling must fit the 24 GiB allowance: %+v", estimate.MemoryEnvelope)
				}
			} else if estimate.MemoryEnvelope.Source != "cross-machine estimate" {
				t.Fatalf("nonmatching evidence lost its conservative tier: %+v", estimate.MemoryEnvelope)
			}
			if got := estimate.Spec.Needs.HostMemoryGB; math.Abs(got-total*factor) > 1e-9 {
				t.Fatalf("host RAM = %.12f, want %.12f", got, total*factor)
			}
			if got := estimate.Spec.Needs.MemoryGB; math.Abs(got-min(device*1.25*1.5, target.TotalMemoryGB)) > 1e-9 {
				t.Fatalf("sibling VRAM lost its transfer margin: %.12f", got)
			}
			if variant == "same host" {
				// Exact-device VRAM wins independently; host RAM retains the larger
				// same-host peak rather than dropping to this card's smaller sample.
				key, _ := q.workObservationKey(&Job{Spec: probe.Spec, Extensions: probe.Extensions, Machine: m.ID, Session: m.Session, AllocatedDevices: []string{"0"}})
				host, gpu := 2.0, 1.0
				q.s.WorkObservations[identityHash(key)] = WorkObservation{Key: *key, MemoryLayout: "separate", MemoryConfiguration: memoryConfiguration(probe.Spec.Needs, probe.Spec.Args, probe.Spec.Program), MemoryRecordedAt: clock.Now(), MemoryPeak: &AttemptMemoryPeak{HostGB: &host, DeviceGB: &gpu}}
				estimate = q.memoryCandidate(probe, &target)
				if math.Abs(estimate.Spec.Needs.HostMemoryGB-total*1.25) > 1e-9 || estimate.Spec.Needs.MemoryGB != 1.25 {
					t.Fatalf("host/device tiers were coupled: %+v", estimate.MemoryEnvelope)
				}
			}
		})
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
			if a == nil || a.ID != first.ID || math.Abs(a.Spec.Needs.HostMemoryGB-peak*1.25) > 1e-9 {
				t.Fatalf("same-host estimate was not admitted: job=%+v", a)
			}
			clock.Advance(time.Second)
			s.Needs.GPU = &GPUSelection{Devices: []string{"1"}}
			second := submit(t, q, s)
			b := assignedJob(machineCheck(t, q, m, a))
			wantReserved := peak * 1.25
			if 2*wantReserved > 24 {
				if b != nil || job(t, q, second.ID).State != "queued" {
					t.Fatalf("sibling spent RAM beyond the shared allowance: %+v", b)
				}
			} else {
				if b == nil || b.ID != second.ID || math.Abs(b.Spec.Needs.HostMemoryGB-peak*1.25) > 1e-9 {
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
			if got := q.s.Machines[m.ID].HostMemoryInUseGB; math.Abs(got-peak*1.25) > 1e-9 {
				t.Fatalf("release removed the sibling's reservation: %.12f GiB", got)
			}
		})
	}
}

func TestMemoryEnvelopeExploratoryFirstRun(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	plain := cpuSpec(1, 0)
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &plain.Needs); err != nil {
		t.Fatal(err)
	}
	if err := plain.Validate(); err != nil {
		t.Fatal("auto must not require a bootstrap declaration:", err)
	}
	if err := json.Unmarshal([]byte(`{"memory":"auto"}`), &s.Needs); err != nil {
		t.Fatal(err)
	}
	m.HostAvailableMemoryGB = 15 // Less than half the physical 64 GiB allowance.
	j := submit(t, q, s)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("exploration admitted without large free headroom")
	}
	if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "exploratory") {
		t.Fatalf("missing exploratory refusal: %s", reason)
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
	data, _ := json.Marshal(first)
	if first == nil || first.Spec.Needs.HostMemoryGB != 31.875 || !strings.Contains(string(data), "exploratory first run") {
		t.Fatalf("no-hint first run must leave launch drift headroom: %+v; %s", first, job(t, q, j.ID).Reason)
	}
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(first, true)
	if err := json.Unmarshal([]byte(`{"memory_peak":{"host_gb":2}}`), &r); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	next := startObservation(t, q, m, s)
	data, _ = json.Marshal(next)
	if next.Spec.Needs.HostMemoryGB != 2.5 || !strings.Contains(string(data), "accepted peak × 1.25") {
		t.Fatalf("exploration did not become a measurement: %s", data)
	}
}

func TestMemoryEnvelopeExploratoryLaunchWithRAMDrift(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("drift", "drift-host", "cpu", 8, 32)
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 64, .125
	check(t, q, m, nil)
	s := cpuSpec(1, 0)
	s.Needs.Memory = "auto"
	s.Run.Command = []string{"true"}
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != queued.ID || j.MemoryEnvelope.Source != "exploratory first run" {
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

func TestMemoryEnvelopeRefusalReason(t *testing.T) {
	q, clock := fixture(t)
	m := machine("small", "fake")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 2
	check(t, q, m, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 4
	j := submit(t, q, s)
	clock.Advance(time.Second)
	if check(t, q, m, nil).Job != nil {
		t.Fatal("admitted above live host memory")
	}
	reason := job(t, q, j.ID).Reason
	if !strings.Contains(reason, "available") || !strings.Contains(reason, "4.0") || !strings.Contains(reason, "2.0") {
		t.Fatalf("missing envelope/live capacity diagnostic: %s", reason)
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
	if j.MemoryEnvelope == nil || j.MemoryEnvelope.Refusal != "" || j.MemoryEnvelope.Source != "exploratory first run" || j.Spec.Needs.HostMemoryGB < .25 || j.Spec.Needs.MemoryGB <= 0 || j.MemoryEnvelope.HostGB > 64 {
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
	var allowance string
	for run := 0; run < 2; run++ {
		queued := submit(t, q, s)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("solo CUDA run %d not assigned: %s", run, job(t, q, queued.ID).Reason)
		}
		if run == 1 && (j.MemoryEnvelope.Source != "accepted peak × 1.25" || j.Spec.Needs.HostMemoryGB != .15625 || j.Spec.Needs.MemoryGB != .3125 || j.MemoryEnvelope.HostGB != .59375) {
			t.Fatalf("second CUDA run did not sum learned host + GPU + slack: %+v", j.MemoryEnvelope)
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		key := job(t, q, j.ID).Timing.Key
		if run == 0 {
			allowance = key.Allowance
		} else if key.Allowance != allowance {
			t.Fatal("live RAM drift changed the allowance hash")
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
	// Resolved auto reservations share one timing identity across live drift.
	if len(q.s.WorkObservations) != 1 {
		t.Fatalf("expected both solo CUDA runs to share one peak record, got %d", len(q.s.WorkObservations))
	}
	for _, point := range q.s.WorkObservations {
		if point.Samples != 2 || point.StaleReason != "" || point.MemoryPeak == nil || point.MemoryLayout != "unified" {
			t.Fatalf("CUDA peaks not accepted: %+v", point)
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
	m.HostAvailableMemoryGB = 22.5
	j := submit(t, q, auto)
	if assignedJob(machineCheck(t, q, m)) != nil {
		t.Fatal("unified peak admitted without existing slack")
	}
	m.HostAvailableMemoryGB = 22.625
	assigned := assignedJob(machineCheck(t, q, m))
	if assigned == nil || assigned.ID != j.ID {
		t.Fatalf("conservative host + GPU reservation did not fit: %s", job(t, q, j.ID).Reason)
	}
	if assigned.Spec.Needs.MemoryGB != 10 || assigned.Spec.Needs.HostMemoryGB != 12.5 {
		t.Fatalf("wrong unified vector: %+v", assigned.Spec.Needs)
	}
	// No auto opt-in: production keeps the existing 8 + 4 + slack declaration.
	finish(t, q, assigned, true)
	for id, point := range q.s.WorkObservations {
		point.MemoryPeak.DeviceGB = nil
		q.s.WorkObservations[id] = point
	}
	if estimate := q.memoryEstimate(assigned, &m); estimate.host != nil {
		t.Fatal("unified exact estimate used an unknown device peak")
	}
	// On a separate-memory target, a host estimate is still useful, but the
	// retained GPU declaration must not masquerade as a measured GPU peak.
	separateTarget := allocationMachine(q.s.Machines[m.ID], assigned.AllocatedDevices)
	separateTarget.UnifiedMemory = false
	for id, point := range q.s.WorkObservations {
		point.MemoryLayout = "separate"
		q.s.WorkObservations[id] = point
	}
	probe := *assigned
	probe.State = "queued"
	if candidate := q.memoryCandidate(&probe, separateTarget); !strings.Contains(candidate.MemoryEnvelope.Source, "GPU recipe default") {
		t.Fatalf("unknown GPU peak has misleading source: %+v", candidate.MemoryEnvelope)
	}
	s.Priority = "production"
	productionID := submit(t, q, s)
	production := assignedJob(machineCheck(t, q, m))
	if production == nil || production.ID != productionID.ID {
		t.Fatal("explicit production refused")
	}
	if production.Spec.Needs.MemoryGB != 8 || production.Spec.Needs.HostMemoryGB != 4 {
		t.Fatal("production declarations tightened")
	}
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

func TestMemoryEnvelopeOOMIsJobCause(t *testing.T) {
	q, clock := fixture(t)
	w := recoveryWorker(t)
	w.Clock = clock
	w.memoryCeilings = fakeCeilings(t)
	w.memoryCeilings.fs = envelopePeakFS{oom: true}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	m := w.Machine
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	s := spec("production")
	s.Resumable = false
	s.Needs.HostMemoryGB = 4
	s.Run.Command = []string{"true"}
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("not assigned")
	}
	if err := q.Started(result(j, false)); err != nil {
		t.Fatal(err)
	}
	store.state.Session = j.Session
	var p *process
	await(t, func() bool {
		p, err = w.start(store, *j)
		return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
	})
	if err != nil {
		t.Fatal(err)
	}
	r := d5Finish(t, p)
	data, _ := json.Marshal(r)
	if !r.OutOfMemory || r.Interrupted || !strings.Contains(string(data), `"host_gb":2`) || !strings.Contains(r.Error, "memory ceiling") {
		t.Fatalf("lost whole-attempt OOM/peak: %s", data)
	}
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, queued.ID)
	if got.State != "failed" || got.InfrastructureRetries != 0 || !strings.Contains(got.Reason, "memory ceiling") {
		t.Fatalf("OOM not reported against job: %+v", got)
	}
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
	if next.MemoryEnvelope.Source != "exploratory (previous peak constrained)" {
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
	if third.MemoryEnvelope.Source != "exploratory (previous peak constrained)" {
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
	for i := 0; i < 3; i++ {
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
		expected := "accepted peak × 1.25"
		if i == 0 {
			expected = "exploratory first run"
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
	if len(q.s.WorkObservations) != 1 {
		t.Fatalf("learned %d classes for one profile", len(q.s.WorkObservations))
	}
	s.ProfileMemory = strings.Repeat("b", 64) // Changed base configuration or per_item identity.
	queued := submit(t, q, s)
	j := assignedJob(machineCheck(t, q, m))
	if j == nil || j.ID != queued.ID || j.MemoryEnvelope == nil || j.MemoryEnvelope.Source != "exploratory first run" {
		t.Fatalf("changed profile reused old memory: %+v", j)
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
	if bound.MemoryEnvelope.Source != "accepted peak × 1.25" || bound.MemoryEnvelope.HostGB != 15 {
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
	if next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.MemoryEnvelope.HostGB != 15 {
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
	if relearned.MemoryEnvelope.Source != "accepted peak × 1.25" || relearned.MemoryEnvelope.HostGB != 15 {
		t.Errorf("unconstrained exploration did not re-teach total peak: %+v", relearned.MemoryEnvelope)
	}
}

func TestMemoryEnvelopeStoredFileBackedPressure(t *testing.T) {
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
	if relearned.MemoryEnvelope.Source != "accepted peak × 1.25" || relearned.MemoryEnvelope.HostGB != 15 {
		t.Errorf("stored pressure poisoned fresh learning: %+v", relearned.MemoryEnvelope)
	}
}

func TestMemoryEnvelopeTotalPeakKeepsDeviceSeparate(t *testing.T) {
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
	if next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.Spec.Needs.HostMemoryGB != 15 || next.Spec.Needs.MemoryGB != 5 || next.MemoryEnvelope.HostGB != 20.125 {
		t.Fatalf("total host/device peaks mixed: %+v; %+v", next.MemoryEnvelope, next.Spec.Needs)
	}
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
	wants := []float64{7.8166961669921875 * 1.25, 4.041187286376953 * 1.25}
	for i, spec := range specs {
		for n, host := range samples[i] {
			j := startObservation(t, q, m, spec)
			if n == 1 && j.Spec.Needs.HostMemoryGB != totals[i]*1.25 {
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
	if estimate.host != nil || estimate.measuredHostGB != 0 || !strings.Contains(estimate.source, "constrained") {
		t.Fatalf("pressure preserved a reduced estimate: %+v", estimate)
	}
	for _, point := range q.s.WorkObservations {
		if point.MemoryPeak.constrained() && point.MemoryRange != nil {
			t.Fatal("pressure retained range")
		}
	}
}

func TestMemoryEnvelopeMeasuredRangeFloorsAndExpiry(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	for _, host := range []float64{8, 8} {
		j := startObservation(t, q, m, s)
		clock.Advance(time.Second)
		r := result(&j, true)
		total := 12.0
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &total, Events: &MemoryEventCounts{}}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	probe := &Job{State: "queued", Spec: s, Extensions: []Extension{registeredExtension(t, q, s.Extension)}}
	candidate := extensionJobs(probe, &m, true)[0]
	if got := q.memoryCandidate(candidate, &m).Spec.Needs.HostMemoryGB; got != 10 {
		t.Fatalf("narrow range lost 25%% growth allowance: %v", got)
	}
	for id, point := range q.s.WorkObservations {
		// Even a prior larger anon/resident peak cannot be discounted by new data.
		host := 9.0
		point.MemoryPeak.HostGB = &host
		q.s.WorkObservations[id] = point
	}
	if got := q.memoryCandidate(candidate, &m).Spec.Needs.HostMemoryGB; got < 11.25 {
		t.Fatalf("fell below observed resident peak: %v", got)
	}
	clock.Advance(workCurveMaxAge + time.Second)
	if estimate := q.memoryEstimate(candidate, &m); estimate.host != nil || estimate.measuredHostGB != 0 {
		t.Fatalf("expired range reused: %+v", estimate)
	}
}

func TestMemoryEnvelopeGPUKeepsTotalPeakWithComponentSamples(t *testing.T) {
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
	if got, want := next.Spec.Needs.HostMemoryGB, 19.918118*1.25; math.Abs(got-want) > 1e-9 {
		t.Fatalf("GPU host-backed storage discarded: got %v want %v", got, want)
	}
}

func TestMemoryEnvelopeMeasuredRangeVariationAndTinyWork(t *testing.T) {
	for _, tc := range []struct {
		name  string
		peaks [2]float64
		want  float64
	}{
		{"observed variation exceeds 25 percent", [2]float64{4, 8}, 12},
		{"tiny command retains launch drift", [2]float64{.1, .1}, .225},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			s.Needs.Memory = "auto"
			for _, host := range tc.peaks {
				j := startObservation(t, q, m, s)
				clock.Advance(time.Second)
				r := result(&j, true)
				total := 12.0
				r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &total, Events: &MemoryEventCounts{}}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			probe := &Job{State: "queued", Spec: s, Extensions: []Extension{registeredExtension(t, q, s.Extension)}}
			candidate := extensionJobs(probe, &m, true)[0]
			if got := q.memoryCandidate(candidate, &m).Spec.Needs.HostMemoryGB; math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("wrong margin: got %v want %v", got, tc.want)
			}
			// A later successful attempt without complete counters restores the
			// conservative total peak, rather than refreshing a reduced range.
			j := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			r := result(&j, true)
			host, total := tc.peaks[1], 12.0
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, CgroupGB: &total, Events: &MemoryEventCounts{}}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if got := q.memoryCandidate(candidate, &m).Spec.Needs.HostMemoryGB; got != 15 {
				t.Fatalf("missing counters preserved reduced range: %v", got)
			}
		})
	}
}

func TestMemoryEnvelopeIncompleteLocalHistoryKeepsTotalPeak(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	s.Needs.Memory = "auto"
	for range 2 {
		j := startObservation(t, q, m, s)
		clock.Advance(time.Second)
		r := result(&j, true)
		host, total := 4.0, 12.0
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, NonreclaimableGB: &host, CgroupGB: &total, Events: &MemoryEventCounts{}}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	// Allowance changes can leave more than one comparable row for a local
	// component. The legacy row has no complete decomposition of its total.
	for _, point := range q.s.WorkObservations {
		point.Key.Allowance = "previous-allowance"
		point.MemoryRange = nil
		point.MemoryPeak = &AttemptMemoryPeak{HostGB: point.MemoryPeak.HostGB, CgroupGB: point.MemoryPeak.CgroupGB}
		q.s.WorkObservations[identityHash(point.Key)] = point
		break
	}
	probe := &Job{State: "queued", Spec: s, Extensions: []Extension{registeredExtension(t, q, s.Extension)}}
	candidate := extensionJobs(probe, &m, true)[0]
	if got := q.memoryCandidate(candidate, &m).Spec.Needs.HostMemoryGB; got != 15 {
		t.Fatalf("complete row discounted incomplete local history: %v", got)
	}
}
