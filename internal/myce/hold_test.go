package myce

import (
	"strings"
	"testing"
	"time"
)

// Status shows each hold as one reason and one next action, so every kind
// must supply both, plus who clears it and when.
func TestHoldKindsAreComplete(t *testing.T) {
	for kind, k := range holdKinds {
		if k.owner == "" || k.reason == "" || k.clears == "" || k.next == "" || strings.Contains(k.reason, ";") {
			t.Fatalf("%s: incomplete or multi-part hold kind %+v", kind, k)
		}
	}
	h := newHold("not-a-kind", "evidence")
	if h.Owner != ownerOperator || h.Detail != "evidence" {
		t.Fatalf("unknown kind must default to an operator-owned hold: %+v", h)
	}
	until := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := pauseHold(&Job{ID: "job-1", State: "paused", Pause: &JobPause{Reason: "waiting on data", Until: &until}})
	if p.Kind != "paused" || p.Next != "myce resume job-1 when it should run" || p.Expires == nil || !strings.Contains(p.Text(), "; next: ") {
		t.Fatalf("pause hold: %+v", p)
	}
}

// holdDetail is a per-target check's evidence, or "" when it passes.
func holdDetail(h *Hold) string {
	if h == nil {
		return ""
	}
	return h.Detail
}

// A per-target check's refusal records its hold row on the real check-in
// decision, without the detail; a boolean predicate or a later reset clears it.
func TestAdmissionDecisionRecordsHold(t *testing.T) {
	q, clock := fixture(t)
	m := &Machine{ID: "worker", Session: "s1", Device: "0"}
	j := &Job{ID: "job-1"}
	reset := func() {
		q.admissionTrace = &admissionTrace{machine: m.ID, session: m.Session, at: clock.Now(), selecting: true,
			jobs: map[string]map[string]AdmissionDecision{j.ID: {targetKey(m): {Device: m.Device, Predicate: predicateSelectionSkipped}}}}
		q.beginAdmissionCandidate(j, m)
	}
	reset()
	if q.admissionHold(j, m, nil, predicateDiskHeadroom) != true || q.admissionTrace.active.Hold != nil {
		t.Fatal("passing check recorded a hold")
	}
	if q.admissionHold(j, m, newHold("disk_headroom", "volume /private/x short"), predicateDiskHeadroom) {
		t.Fatal("refusing check passed")
	}
	d := q.admissionTrace.active
	if d.Predicate != predicateDiskHeadroom || d.Hold == nil || d.Hold.Kind != "disk_headroom" || d.Hold.Next == "" || d.Hold.Detail != "" {
		t.Fatalf("decision hold: %+v", d)
	}
	same := *d
	same.Hold = &Hold{Kind: "disk_headroom", Owner: d.Hold.Owner, Reason: d.Hold.Reason, Clears: d.Hold.Clears, Next: d.Hold.Next}
	other := *d
	other.Hold = newHold("storage_root", "")
	if !sameAdmissionDecision(d, &same) || sameAdmissionDecision(d, &other) {
		t.Fatal("decision comparison ignores the hold's value")
	}
	q.endAdmissionCandidate(j, m, nil, nil, "")
	if got := q.admissionTrace.jobs[j.ID][targetKey(m)]; got.Hold == nil || got.Outcome != AdmissionRejected {
		t.Fatalf("rejection lost its hold: %+v", got)
	}
	q.beginAdmissionCandidate(j, m)
	if q.admissionTrace.active.Hold != nil {
		t.Fatal("a new evaluation kept the previous hold")
	}
	q.admissionPass(j, m, false, predicateTools)
	if q.admissionTrace.active.Predicate != predicateTools || q.admissionTrace.active.Hold != nil {
		t.Fatalf("boolean predicate carried a hold: %+v", q.admissionTrace.active)
	}
	q.admissionTrace = nil
}

// A refused launch whose launching attempt carried measured helper charge is
// the helper_charge hold; older reports without it stay launch_capacity.
func TestLaunchCapacityHoldNamesHelperCharge(t *testing.T) {
	available := 3.0
	wait := func(helper float64) *Job {
		return &Job{Attempt: 2, LaunchCapacityWait: map[string]LaunchCapacityWait{"worker/0": {Attempt: 2, Admission: &AdmissionDetail{Subcheck: "live_host_growth", Unit: "GiB", Required: 5, Available: &available,
			Contributors: []HostGrowthContributor{{Kind: "attempt", Launching: true, ChargeGB: 5, PendingGB: 5, HelperGB: helper}}}}}}
	}
	if h := launchCapacityHold(wait(0)); h.Kind != "launch_capacity" {
		t.Fatalf("no helper evidence: %+v", h)
	}
	if h := launchCapacityHold(wait(1.5)); h.Kind != "helper_charge" || !strings.Contains(h.Detail, "1.500 GiB beyond the reservation") {
		t.Fatalf("helper evidence: %+v", h)
	}
	stale := wait(1.5)
	stale.Attempt = 3
	if h := launchCapacityHold(stale); h.Kind != "launch_capacity" {
		t.Fatalf("an earlier attempt's helper evidence named this wait: %+v", h)
	}
}
