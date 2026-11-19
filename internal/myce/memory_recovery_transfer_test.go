package myce

import (
	"math"
	"strings"
	"testing"
	"time"
)

// An accepted foreign-family row can coexist with an invalidated local row.
// Unknown transfer error must not turn that local recovery into two independent
// whole-host offers on unified memory. These are equivalent production bounds,
// with synthetic work and machine identities and an injected clock.
func TestMemoryRecoveryLocalPriorBeforeUnknownTransfer(t *testing.T) {
	q, clock, m, s := unifiedAutoFixture(t, 0)
	clock.Advance(time.Second)
	q.mu.Lock()
	defer q.mu.Unlock()
	const capacity = 121.06455612182617
	owner := q.s.Machines[m.ID]
	owner.HostMemoryBudgetGB, owner.EffectiveHostMemoryBudgetGB = 96, 96
	owner.HostAvailableMemoryGB = capacity
	owner.Devices[0].TotalMemoryGB = capacity
	owner.Devices[0].BudgetGB, owner.Devices[0].FreeMemoryGB = capacity, capacity
	target := owner.Devices[0].machine(*owner)
	physical := q.qualificationCapacity(&target)
	capacityQueue := Queue{clock: clock, s: q.s, memoryOrigin: q, memoryPlanning: newMemoryPlanningCache()}
	capacityQueue.s.Jobs, capacityQueue.s.ModelSlots, capacityQueue.s.HostReleases = nil, nil, nil
	capacityQueue.s.Machines = map[string]*Machine{owner.ID: &physical}
	var extensions []Extension
	for _, e := range q.s.Extensions {
		extensions = append(extensions, e)
	}
	j := &Job{ID: "pending", State: "queued", Spec: s, Extensions: extensions, Machine: owner.ID, Session: owner.Session, AllocatedDevices: []string{"0"}}
	candidates := extensionJobs(j, &physical, true)
	if len(candidates) != 1 {
		t.Fatal("expected one qualified CUDA variant")
	}
	j = candidates[0]
	key, why := q.workObservationKey(j)
	if key == nil {
		t.Fatal(why)
	}
	configuration := memoryConfiguration(j.Spec.Needs, j.Spec.Args, j.Spec.Program)
	ptr := func(v float64) *float64 { return &v }
	prior := &AttemptMemoryPeak{
		HostGB: ptr(4.994926452636719), CgroupGB: ptr(10.575447082519531), NonreclaimableGB: ptr(7.213115692138672),
		CombinedHostGB: ptr(21.220745086669922), GrowthGB: ptr(.6700734494210527),
		DeviceGB: ptr(10.1015625), DeviceGrowthGB: ptr(5.7509765625),
		GPUHost: &GPUHostMemory{SiblingAdjusted: true, DriverGB: ptr(14.24050521850586), HostDropGB: ptr(14.826972961425781), OutsideGB: ptr(14.24050521850586)},
	}
	local := WorkObservation{Key: *key, MemoryLayout: "unified", MemoryConfiguration: configuration,
		MemoryPeak:       &AttemptMemoryPeak{HostGB: ptr(7.53076171875), CgroupGB: ptr(10.621837615966797), Constrained: true},
		MemoryEscalation: &MemoryEscalationPrior{Peak: prior, RecordedAt: clock.Now().Add(-time.Second)},
	}
	foreign := local
	foreign.Key.Host, foreign.Key.Hardware, foreign.Key.Fingerprint = "other-host", "other-card", "other-driver"
	foreign.Key.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "xpu"}
	foreign.MemoryLayout, foreign.MemoryEscalation, foreign.MemoryRecordedAt = "separate", nil, clock.Now()
	foreign.MemorySamples = 22
	foreign.MemoryPeak = &AttemptMemoryPeak{HostGB: ptr(7.677909851074219), CgroupGB: ptr(11.169681549072266), DeviceGB: ptr(10.121585845947266)}
	q.putWorkObservation("local", local)
	q.putWorkObservation("foreign", foreign)
	// Retain the exact old projection as a check of the triggering evidence,
	// rather than encoding a second, approximate recovery implementation.
	uncalibrated := capacityQueue.memoryEstimateForState(j, &physical, key, configuration, false)
	device := *uncalibrated.device.DeviceGB * uncalibrated.deviceFactor
	if !uncalibrated.uncalibrated || math.Abs(device-capacity) > 1e-9 || math.Abs(uncalibrated.measuredHostGB-95.75) > 1e-9 || math.Abs(device+uncalibrated.measuredHostGB-216.81455612182617) > 1e-9 {
		t.Fatalf("did not reproduce whole-device plus whole-host projection: %+v device=%g", uncalibrated, device)
	}
	check := func(probe *Job, wantHost, wantDevice float64, source string) {
		t.Helper()
		got := capacityQueue.memoryCandidate(probe, &physical)
		e := got.MemoryEnvelope
		if e == nil || e.Refusal != "" || math.Abs(e.HostGB-wantHost) > 1e-9 || math.Abs(e.DeviceGB-wantDevice) > 1e-9 || !strings.Contains(e.Source, source) {
			t.Fatalf("recovery envelope=%+v want host=%g device=%g source=%s", e, wantHost, wantDevice, source)
		}
	}
	check(j, 37.868418633747225, 15.8525390625, acceptedMemoryEscalation)
	// The existing supported hint contract becomes reachable again, with
	// the local peak retained even if the author supplies a smaller figure.
	hinted := *j
	hinted.MemoryFallback = &SizeHint{HostMemoryGB: 22, MemoryGB: 16}
	check(&hinted, 38.125, 16, "submitter hint")
	hinted.MemoryFallback = &SizeHint{HostMemoryGB: 1, MemoryGB: 16}
	check(&hinted, 37.34574508666992, 16, "submitter hint")
	// Attempt-specific retry floors remain binding after evidence selection.
	retry := *j
	retry.MemoryOOMFloor = &SizeHint{HostMemoryGB: 30, MemoryGB: 20}
	check(&retry, 50.125, 20, acceptedMemoryEscalation)
	// A fresh accepted exact-target peak still wins over the old prior.
	local.MemoryPeak, local.MemoryRecordedAt = prior, clock.Now()
	q.putWorkObservation("local", local)
	capacityQueue.memoryPlanning = newMemoryPlanningCache()
	e := capacityQueue.memoryEstimateFor(j, &physical, false)
	if !e.exact || e.escalation || e.uncalibrated {
		t.Fatalf("fresh exact evidence replaced by recovery: %+v", e)
	}
	// With no retained local measurement, an unknown transfer is still unknown.
	local.MemoryPeak, local.MemoryRecordedAt, local.MemoryEscalation = nil, time.Time{}, nil
	q.putWorkObservation("local", local)
	capacityQueue.memoryPlanning = newMemoryPlanningCache()
	e = capacityQueue.memoryEstimateFor(j, &physical, false)
	if !e.uncalibrated || e.escalation {
		t.Fatalf("invented local recovery evidence: %+v", e)
	}
}
