package myce

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func unifiedCUDAAdmissionMachine() Machine {
	m := machine("unified", "cuda", "unified-host")
	m.UnifiedMemory, m.HostMemoryMeasured = true, true
	m.CapabilityProfile.MemoryLayout = "unified"
	m.CapabilityProfile.SystemMemoryBytes = 121 << 30
	m.FreeMemoryGB, m.TotalMemoryGB = 96, 96
	m.HostMemoryBudgetGB, m.HostAvailableMemoryGB = 96, 90
	m.ReservedMemoryGB = 25 // The physical floor is the reserve; the allowance caps charges.
	m.MemoryCeilingSlackGB = .125
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 15}
	return m
}

func TestUnifiedCUDAAdmissionPhysicalFloor(t *testing.T) {
	for _, mode := range []string{"unified", "discrete-cuda", "other-unified"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := unifiedCUDAAdmissionMachine()
			if mode == "discrete-cuda" {
				m.UnifiedMemory, m.CapabilityProfile.MemoryLayout = false, "separate"
			}
			if mode == "other-unified" {
				m.GPU = "fake"
			}
			m.HostAvailableMemoryGB = 20
			m.MemoryObservation.UsedGB = 97 // 24 GiB raw free, below the 25 GiB floor.
			s := spec("experiment")
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = 2, 1
			j := submit(t, q, s)
			reply := check(t, q, m, nil)
			if mode != "unified" {
				if reply.Job == nil || reply.Job.ID != j.ID {
					t.Fatal("other host admission changed", reply)
				}
				return
			}
			if reply.Job != nil {
				t.Fatal("CUDA admitted below physical floor")
			}
			if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "physical RAM floor 25.000 GiB") {
				t.Fatal("physical refusal unexplained", reason)
			}
			observation := m.MemoryObservation
			m.MemoryObservation = nil
			if run := check(t, q, m, nil).Job; run != nil {
				t.Fatal("missing physical telemetry admitted CUDA", run)
			}
			m.MemoryObservation = observation
			clock.Advance(time.Second)
			m.MemoryObservation.UsedGB = 89 // 32 - 25 = 7 GiB; charge 3.125 fits.
			if run := check(t, q, m, nil).Job; run == nil || run.ID != j.ID {
				t.Fatal("fresh physical headroom did not restore admission", run)
			}
		})
	}
}

func TestUnifiedCUDAAdmissionModelLaunchRechecksFloor(t *testing.T) {
	_, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	available := 8.0 // +16 reserve = 24 GiB raw, below the 25 GiB floor.
	w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16,
		HostMemorySource: func(context.Context) (float64, error) { return available, nil },
		MemorySource:     func(context.Context) (float64, error) { return 96, nil },
	}
	rt, err := startModels(w, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rt.close(); err != nil {
			t.Error(err)
		}
	})
	loading := &workerModel{model: Model{Name: "synthetic", MemoryGB: 24, HostMemoryGB: 8}}
	if err := rt.checkLoadMemory(t.Context(), loading); err == nil || !strings.Contains(err.Error(), "insufficient host RAM") {
		t.Fatal("model bypassed live physical floor", err)
	}
	available = 59 // 75 raw - 25 reserve = 50 GiB, enough for 32.125 GiB.
	if err := rt.checkLoadMemory(t.Context(), loading); err != nil {
		t.Fatal("physical recovery did not permit model", err)
	}
	// The runtime reads a changed allowance at once. An authored allowance
	// caps the charge; it is never a floor on free RAM.
	rt.mu.Lock()
	rt.memoryMachine.HostMemoryBudgetGB = 64
	rt.mu.Unlock()
	if err := rt.checkLoadMemory(t.Context(), loading); err != nil {
		t.Fatal("authored allowance became a physical floor", err)
	}
	rt.mu.Lock()
	rt.memoryMachine.HostMemoryBudgetGB = 30 // Below the 32.125 GiB charge.
	rt.mu.Unlock()
	if err := rt.checkLoadMemory(t.Context(), loading); admissionDetail(err, Job{}) == nil || admissionDetail(err, Job{}).Subcheck != "configured_host_allowance" {
		t.Fatal("model retained a stale allowance", err)
	}
	// Legacy admission-only models may omit host RAM; CUDA still uses physical RAM.
	rt.mu.Lock()
	rt.memoryMachine.HostMemoryBudgetGB = 0
	rt.mu.Unlock()
	available, loading.model.HostMemoryGB = 8, 0
	if err := rt.checkLoadMemory(t.Context(), loading); err == nil || !strings.Contains(err.Error(), "insufficient host RAM") {
		t.Fatal("zero host vector bypassed CUDA physical floor", err)
	}
}

func TestUnifiedCUDAAdmissionUnknownDeviceRemainsFull(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	check(t, q, m, nil)
	// Current cgroup RAM exceeds the host vector. It cannot cancel the
	// device vector, whose telemetry is absent (the incident's default).
	j := &Job{ID: "holder", State: "running", Machine: m.ID, Session: m.Session, Attempt: 1, Spec: spec("experiment")}
	j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB = 32, 21
	q.s.Jobs[j.ID] = j
	q.hostMemorySamples[m.ID] = map[attemptKey]hostMemorySample{jobKey(*j): {gb: 45, at: clock.Now()}}
	m.HostAvailableMemoryGB = 50
	m.MemoryObservation.UsedGB = 46 // 75 raw - 25 reserve = 50 usable.
	q.s.Machines[m.ID].HostAvailableMemoryGB = m.HostAvailableMemoryGB
	q.s.Machines[m.ID].MemoryObservation = m.MemoryObservation
	q.refreshHostMemory()
	if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 18 {
		t.Fatalf("device growth discounted: got %g GiB, want 50 - 32 = 18", got)
	}
	clock.Advance(q.workerArrivalAllowance(q.s.Machines[m.ID]))
	q.refreshHostMemory()
	if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 0 {
		t.Fatal("stale physical sample admitted CUDA", got)
	}
	// Worker launch uses the same device floor and a new raw measurement.
	w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16, HostMemorySource: func(context.Context) (float64, error) { return 59, nil }}
	w.hostMemorySamples = map[attemptKey]hostMemorySample{jobKey(*j): {gb: 45, at: clock.Now()}}
	next := Job{Spec: cpuSpec(1, 20)}
	if err := localAttemptAdmission(w, []Job{*j}, next, nil); err == nil {
		t.Fatal("worker discounted unknown CUDA device usage")
	}
	w.HostMemorySource = func(context.Context) (float64, error) { return 16, nil } // raw 32; usable 7.
	if free, err := w.sharedHostLiveMemory(context.Background()); err != nil || free != 7 {
		t.Fatalf("worker floor not rechecked: free=%g err=%v", free, err)
	}
}

func TestUnifiedCUDAAdmissionProtectsExpectedProduction(t *testing.T) {
	q, clock, source, _, producer, _ := productionAdmissionFixture(t)
	parent := submit(t, q, producer)
	wave := check(t, q, source, nil).Job
	if wave == nil {
		t.Fatal("missing production producer")
	}
	if err := q.Started(result(wave, false)); err != nil {
		t.Fatal(err)
	}
	m := unifiedCUDAAdmissionMachine()
	m.HostAvailableMemoryGB = 90
	m.MemoryObservation.UsedGB = 16 // 105 raw - 25 reserve = 80 usable.
	check(t, q, m, nil)
	prod := spec("production")
	prod.Project = producer.Project
	prod.Needs.MemoryGB, prod.Needs.HostMemoryGB = 24, 8
	prod.AllowedMachines = []string{m.ID}
	prod.After = &PieceDependency{Job: parent.ID, Piece: "item-0", Outputs: map[string]string{"input": "out"}}
	demand := submit(t, q, prod)
	experiment := spec("experiment")
	experiment.Needs.MemoryGB, experiment.Needs.HostMemoryGB = 32, 21
	experiment.AllowedMachines = []string{m.ID}
	j := submit(t, q, experiment)
	clock.Advance(time.Second)
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("large CUDA experiment consumed expected production headroom", r.Job)
	}
	if reason := job(t, q, j.ID).Reason; !strings.Contains(reason, "unified CUDA production headroom") {
		t.Fatal("missing production headroom reason", reason)
	}
	q.mu.Lock()
	wait := q.waitHold(q.s.Jobs[j.ID])
	q.mu.Unlock()
	if wait.Kind != "unified_cuda_headroom" {
		t.Fatalf("production headroom hold kind: %+v", wait)
	}
	productionCandidate := *job(t, q, j.ID)
	productionCandidate.Spec.Priority = "production"
	small := *job(t, q, j.ID)
	small.Spec.Needs.MemoryGB, small.Spec.Needs.HostMemoryGB = 8, 4
	func() {
		// Internal policy helpers and their borrowed records share the same
		// mutex as asynchronous admission-evidence publication.
		q.mu.Lock()
		defer q.mu.Unlock()
		if reason := holdDetail(q.productionAdmissionHold(&productionCandidate, q.s.Machines[m.ID])); reason != "" {
			t.Fatal("co-tenant guard refused production itself", reason)
		}
		// A smaller co-tenant fits with the production envelope and physical floor.
		target := q.s.Machines[m.ID]
		if reason := holdDetail(q.productionAdmissionHold(&small, target)); reason != "" {
			t.Fatal("small co-tenant withheld", reason)
		}
		// Rechecking an assigned, unstarted offer must not charge it twice.
		small.State, small.Machine, small.Session = "running", m.ID, m.Session
		copy := *target
		copy.FreeHostMemoryGB = hostNeed(target, 24, 8) // Exactly the production envelope remains.
		if reason := holdDetail(q.productionAdmissionHold(&small, &copy)); reason != "" {
			t.Fatal("unstarted offer charged twice", reason)
		}
	}()
	admissionReason := func() string {
		q.mu.Lock()
		defer q.mu.Unlock()
		return holdDetail(q.productionAdmissionHold(q.s.Jobs[j.ID], q.s.Machines[m.ID]))
	}
	// A qualified alternative host removes sole-host protection.
	other := unifiedCUDAAdmissionMachine()
	other.ID, other.HostID, other.Session = "other", "other-host", "other-session"
	check(t, q, other, nil)
	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.s.Jobs[demand.ID].Spec.AllowedMachines = []string{m.ID, other.ID}
		if reason := holdDetail(q.productionAdmissionHold(q.s.Jobs[j.ID], q.s.Machines[m.ID])); reason != "" {
			t.Fatal("qualified alternative did not remove sole-host protection", reason)
		}
	}()
	if _, err := q.ChangeHostMaintenance(other.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	if reason := admissionReason(); reason == "" {
		t.Fatal("unavailable alternative bypassed production headroom")
	}
	clock.Advance(100 * time.Second)
	if reason := admissionReason(); reason != "" {
		t.Fatal("overrunning producer permanently withheld CUDA", reason)
	}
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	if r := check(t, q, m, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("withdrawn production demand still withholds CUDA", r.Job)
	}
}

func TestUnifiedCUDAAdmissionMeasuredProductionEnvelope(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	first := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, m))
	if run == nil || run.ID != first.ID {
		t.Fatal("missing envelope seed")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	r := result(run, true)
	host, device := 8.0, 24.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	machineCheck(t, q, m)
	s.Priority = "production"
	demand := submit(t, q, s)
	experiment := &Job{State: "queued", Spec: spec("experiment")}
	experiment.Spec.Needs.MemoryGB, experiment.Spec.Needs.HostMemoryGB = 16, 10
	// Admission can still be committing after Submit or Cancel returns. The
	// private policy helper, like its production callers, requires q.mu.
	admissionReason := func() string {
		q.mu.Lock()
		defer q.mu.Unlock()
		target := allocationMachine(q.s.Machines[m.ID], []string{"0"})
		return holdDetail(q.productionAdmissionHold(experiment, target))
	}
	reason := admissionReason()
	// The measured bound no longer withholds a universal 25% margin.
	if reason != "" {
		t.Fatal("fitting measured demand withheld headroom", reason)
	}
	experiment.Spec.Needs.HostMemoryGB = 18
	reason = admissionReason()
	// Retain both measured vectors, page rounding and explicit launch slack.
	if !strings.Contains(reason, "32.125 GiB production envelope") {
		t.Fatal("measured production envelope or margin lost", reason)
	}
	if err := q.Cancel(demand.ID); err != nil {
		t.Fatal(err)
	}
	if reason := admissionReason(); reason != "" {
		t.Fatal("cancelled production still protects an envelope", reason)
	}
}

func TestUnifiedCUDAAdmissionImpossibleProductionDoesNotStarve(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	check(t, q, m, nil)
	production := spec("production")
	production.Needs.MemoryGB, production.Needs.HostMemoryGB = 24, 80
	production.AllowedMachines = []string{m.ID}
	demand := submit(t, q, production) // 104.125 GiB can never fit the 96 GiB allowance.
	experiment := spec("experiment")
	experiment.Needs.MemoryGB, experiment.Needs.HostMemoryGB = 2, 1
	experiment.AllowedMachines = []string{m.ID}
	waiting := submit(t, q, experiment)
	clock.Advance(time.Second)
	if run := check(t, q, m, nil).Job; run == nil || run.ID != waiting.ID {
		t.Fatal("impossible production permanently withheld a fitting co-tenant", run, job(t, q, waiting.ID).Reason)
	}
	if job(t, q, demand.ID).State != "queued" {
		t.Fatal("impossible production was admitted")
	}
}

// Slow healthy check-ins used to book the entire outstanding growth as debt
// after five seconds, independently of the accepted observation's validity.
func TestUnifiedCUDAAdmissionAdaptiveObservation(t *testing.T) {
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.MemoryObservation.UsedGB = 3.9
	m.HostAvailableMemoryGB = 92.1 // 121 total - 3.9 used - 25 reserve.
	observation := &ProbeObservation{PeriodSeconds: 12, CostSeconds: .1, ValidSeconds: 24.1}
	sampleAt := clock.Now()
	report := func(age float64) {
		m.HostHealth = &HostHealth{Probe: observation, AgeSeconds: age,
			History: []HostHealthSample{{At: sampleAt}}}
		if _, err := q.Checkin(Checkin{Machine: m, period: 12 * time.Second}); err != nil {
			t.Fatal(err)
		}
	}
	live := func(m *Machine) float64 {
		capacity, _, _, _ := q.hostLiveCapacity(m)
		return capacity
	}
	report(0)
	clock.Advance(10700 * time.Millisecond)
	accepted := q.s.Machines[m.ID]
	if !q.workerFresh(accepted, clock.Now()) || !q.hostHealthEvidenceValid(accepted, clock.Now()) {
		t.Fatal("incident fixture lacks healthy adaptive evidence")
	}
	// 102 GiB cannot fit a 96 GiB allowance. Refuse only the real shortfall,
	// rather than manufacturing 102 GiB debt from a fixed freshness cutoff.
	if got := live(accepted) - 102; math.Abs(got-(-9.9)) > 1e-9 {
		t.Fatalf("healthy slow observation charged fictitious debt: room=%g, want -9.9", got)
	}
	// Once #1117 removes the unused cold-probe promotion, the smaller real
	// outstanding demand fits under the same physical observation and reserve.
	if got := live(accepted) - 72; math.Abs(got-20.1) > 1e-9 {
		t.Fatalf("healthy slow observation refused fitting growth: room=%g", got)
	}
	// A continuing heartbeat carrying the same expired sensor observation
	// cannot renew its validity, even with a new accepted delivery receipt.
	clock.Advance(14 * time.Second)
	report(clock.Now().Sub(sampleAt).Seconds())
	accepted = q.s.Machines[m.ID]
	if !q.workerFresh(accepted, clock.Now()) {
		t.Fatal("stopped sensor fixture also lost worker contact")
	}
	if got := live(accepted) - 72; got != -72 {
		t.Fatalf("expired observation granted physical credit: %g", got)
	}
	// New successful evidence restores room; invalid physical data does not.
	sampleAt = clock.Now()
	report(0)
	accepted = q.s.Machines[m.ID]
	accepted.HostHealth.History[0].Error = "sensor failed"
	if got := live(accepted) - 72; got != -72 {
		t.Fatalf("failed sensor granted physical credit: %g", got)
	}
	accepted.HostHealth.History[0].Error = ""
	accepted.MemoryObservation = nil
	if got := live(accepted) - 72; got != -72 {
		t.Fatalf("missing physical observation granted credit: %g", got)
	}
	accepted.MemoryObservation = m.MemoryObservation
	clock.Advance(q.workerArrivalAllowance(accepted))
	if got := live(accepted) - 72; got != -72 {
		t.Fatalf("silent worker granted physical credit: %g", got)
	}
}

// Live numbers from the measured unified host (2026-11-11): 121.06 GiB, idle
// maximum 5.00, explicit reserve 16, allowance 100.06. Live used RAM already
// holds the idle footprint, so with a measured or authored allowance the floor
// is the reserve alone (not 21). Only the shared-interactive host keeps 21.
func TestUnifiedCUDAFloorMeasuredAllowanceCountsBaselineOnce(t *testing.T) {
	m := unifiedCUDAAdmissionMachine()
	total := 121.06
	m.CapabilityProfile.SystemMemoryBytes = uint64(total * (1 << 30))
	m.ReservedMemoryGB, m.HostMemoryBudgetGB = 16, 100.06
	for _, tc := range []struct {
		name     string
		measured bool
		shared   bool
		floor    float64
	}{
		{"measured", true, false, 16},
		{"authored", false, false, 16},
		{"measured-shared", true, true, 21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m.HostMemoryBudgetMeasured, m.SharedHost = tc.measured, nil
			if tc.shared {
				m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 1}
			}
			floor, ok := unifiedCUDAFloor(&m)
			if !ok || math.Abs(floor-tc.floor) > 1e-6 {
				t.Fatal(floor, ok)
			}
		})
	}
}
