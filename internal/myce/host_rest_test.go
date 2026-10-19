package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

func restEnvelope(clock Clock, expect string) HostEnvelope {
	e := UnknownHostEnvelope("status", clock)
	e.Host, e.Boot, e.Operation = hostPtr("fixture-host"), hostPtr("fixture-boot"), hostPtr("fixture-operation")
	state := "closed"
	if expect == "ready" {
		state = "open"
	}
	e.Desired = HostEnvelopeState{hostPtr(state), hostPtr(uint64(3))}
	e.Observed = e.Desired
	e.QueueRevision = hostPtr(uint64(3))
	e.LocalExclusion, e.Settlement = "PASS", "confirmed"
	e.LastClose = &HostClose{HostRevision: HostRevision{3, "fixture-operation"}, Settlement: "confirmed", SettledAt: hostPtr(clock.Now())}
	for key := range e.Sources {
		e.Sources[key] = hostPtr(clock.Now())
	}
	e.Counts = HostEnvelopeCounts{Active: hostPtr(0), Preparing: hostPtr(0), Stopping: hostPtr(0), PendingDaemon: hostPtr(0), Containers: hostPtr(0), GPUHolders: hostPtr(0), Unsettled: hostPtr(0), Queued: hostPtr(0), FencedQueued: hostPtr(0), UnresolvedPlacement: hostPtr(0)}
	e.Ready, e.RestartSafe = hostPtr(true), hostPtr(true)
	e.Units, e.Attempts = []ExclusionUnit{}, []HostEnvelopeAttempt{}
	e.RestEvidence = HostRestEvidence{QueueReachable: hostPtr(true), MemoryAllowance: hostPtr(int64(2 << 30)), QueueAt: hostPtr(clock.Now()), UnfencedQueued: hostPtr(0)}
	return e
}

func TestHostRestClosedMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, verdict, reason string
		change                func(*HostEnvelope)
	}{
		{"settled", "PASS", "", func(e *HostEnvelope) {}},
		{"local-only", "UNKNOWN", "queue_acknowledgement_unknown", func(e *HostEnvelope) {
			e.QueueRevision = nil
			e.Sources["queue"] = nil
			e.RestEvidence.QueueReachable = hostPtr(false)
		}},
		{"queue-pending", "UNKNOWN", "close_settlement_pending", func(e *HostEnvelope) {
			e.Settlement = "pending"
			e.LastClose.Settlement = "pending"
			e.LastClose.SettledAt = nil
		}},
		{"previous-close", "UNKNOWN", "close_settlement_pending", func(e *HostEnvelope) { e.LastClose.Revision-- }},
		{"local-pass-release-proof-pending", "UNKNOWN", "release_pending", func(e *HostEnvelope) {
			e.Settlement, e.LastClose.Settlement = "pending", "pending"
			e.LastClose.SettledAt = nil
			e.Counts.Unsettled = hostPtr(1)
		}},
		{"fenced", "PASS", "queued_work_fenced", func(e *HostEnvelope) { e.Counts.Queued = hostPtr(3); e.Counts.FencedQueued = hostPtr(3) }},
		{"unfenced", "FAIL", "eligible_unfenced_work", func(e *HostEnvelope) { e.Counts.Queued = hostPtr(1); e.RestEvidence.UnfencedQueued = hostPtr(1) }},
		{"queue-open", "FAIL", "queue_admission_open", func(e *HostEnvelope) {
			e.QueueRevision = nil
			e.Sources["queue"] = nil
			e.RestEvidence.OpenAdmission = true
		}},
		{"unknown-placement", "UNKNOWN", "placement_unknown", func(e *HostEnvelope) { e.Counts.UnresolvedPlacement = hostPtr(1) }},
		{"active", "FAIL", "active_attempts", func(e *HostEnvelope) { e.Counts.Active = hostPtr(1) }},
		{"preparing", "FAIL", "preparing_attempts", func(e *HostEnvelope) { e.Counts.Preparing = hostPtr(1) }},
		{"stopping", "FAIL", "stopping_attempts", func(e *HostEnvelope) { e.Counts.Stopping = hostPtr(1) }},
		{"terminal-unreleased", "UNKNOWN", "release_pending", func(e *HostEnvelope) { e.Counts.Unsettled = hostPtr(1) }},

		{"unresolved-daemon-disposition", "UNKNOWN", "local_exclusion_unknown", func(e *HostEnvelope) { e.LocalExclusion = "UNKNOWN"; e.Counts.PendingDaemon = hostPtr(1) }},
		{"gpu-holder", "FAIL", "local_exclusion_failed", func(e *HostEnvelope) { e.LocalExclusion = "FAIL"; e.Counts.GPUHolders = hostPtr(1) }},
		{"restart", "FAIL", "local_exclusion_failed", func(e *HostEnvelope) { e.LocalExclusion = "FAIL"; e.RestartSafe = hostPtr(false) }},
		{"local-fail-and-queue-unknown", "FAIL", "queue_acknowledgement_unknown", func(e *HostEnvelope) { e.LocalExclusion = "FAIL"; e.Sources["queue"] = nil }},
		{"partial-active-and-unknown", "FAIL", "active_attempts", func(e *HostEnvelope) {
			e.Sources["queue"] = nil
			e.RestEvidence.ClosedViolations = []string{"active_attempts", "release_pending"}
		}},
		{"one-queue-release-another-unknown", "UNKNOWN", "release_pending", func(e *HostEnvelope) {
			e.Sources["queue"] = nil
			e.Counts.Unsettled = nil
			e.RestEvidence.ClosedViolations = []string{"release_pending"}
		}},
		{"timeout", "UNKNOWN", "timeout", func(e *HostEnvelope) { e.Errors = append(e.Errors, HostEnvelopeError(context.DeadlineExceeded)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &envelopeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			e := restEnvelope(clock, "closed")
			tc.change(&e)
			e.ApplyRestVerdict("closed", clock.Now())
			if e.Verdict != tc.verdict || tc.reason != "" && !slices.Contains(e.Reasons, tc.reason) {
				t.Fatalf("got %+v", e.HostRestVerdict)
			}
			if len(e.Reasons) != len(slices.Compact(slices.Clone(e.Reasons))) {
				t.Fatal("duplicate reasons")
			}
		})
	}
}

func TestHostRestReadyMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, verdict, reason string
		change                func(*HostEnvelope)
	}{
		{"ready-with-active-work", "PASS", "", func(e *HostEnvelope) {
			e.Counts.Active = hostPtr(1)
			e.LocalExclusion = "FAIL"
			e.Counts.Containers = hostPtr(2)
			e.Counts.GPUHolders = hostPtr(1)
			e.Units = []ExclusionUnit{{Unit: "fixture-unit", Populated: hostPtr(true)}}
		}},
		{"missing-required-worker", "UNKNOWN", "required_workers_not_ready", func(e *HostEnvelope) { e.Ready = hostPtr(false) }},
		{"unhealthy-required-worker", "FAIL", "required_worker_unhealthy", func(e *HostEnvelope) { e.Ready = hostPtr(false); e.RestEvidence.ReadyViolation = true }},
		{"closed", "FAIL", "desired_state_mismatch", func(e *HostEnvelope) { e.Desired.State = hostPtr("closed") }},
		{"stale-revision", "FAIL", "observed_state_mismatch", func(e *HostEnvelope) { e.Observed.Revision = hostPtr(uint64(2)) }},
		{"unreleased", "UNKNOWN", "release_pending", func(e *HostEnvelope) { e.Counts.Unsettled = hostPtr(1) }},
		{"partial-unreleased", "UNKNOWN", "release_pending", func(e *HostEnvelope) {
			e.Ready = nil
			e.Sources["queue"] = nil
			e.RestEvidence.ClosedViolations = []string{"release_pending"}
		}},
		{"queue-missing", "UNKNOWN", "queue_acknowledgement_unknown", func(e *HostEnvelope) { e.Sources["queue"] = nil; e.QueueRevision = nil }},
		{"fail-and-unknown", "FAIL", "required_workers_not_ready", func(e *HostEnvelope) { e.Ready = nil; e.RestEvidence.ReadyViolation = true; e.Sources["queue"] = nil }},
		{"app-and-local-exclusion-unavailable", "PASS", "", func(e *HostEnvelope) {
			e.LocalExclusion = "UNKNOWN"
			e.Sources["gpu_holders"] = nil
			e.Sources["docker"] = nil
			e.Sources["cgroups"] = nil
			e.Errors = []ExclusionError{{Component: "gpu_holders", Code: "permission_denied"}}
		}},
		{"timeout", "UNKNOWN", "timeout", func(e *HostEnvelope) { e.Errors = append(e.Errors, HostEnvelopeError(ErrHostDeadline)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &envelopeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			e := restEnvelope(clock, "ready")
			tc.change(&e)
			e.ApplyRestVerdict("ready", clock.Now())
			if e.Verdict != tc.verdict || tc.reason != "" && !slices.Contains(e.Reasons, tc.reason) {
				t.Fatalf("got %+v", e.HostRestVerdict)
			}
		})
	}
}

func TestHostRestFreshnessAndRevision(t *testing.T) {
	for _, expect := range []string{"closed", "ready"} {
		for _, delta := range []time.Duration{HostObservationWindow, HostObservationWindow + time.Nanosecond} {
			clock := &envelopeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			e := restEnvelope(clock, expect)
			clock.at = clock.at.Add(delta)
			e.FinishObservation(clock.Now())
			e.ApplyRestVerdict(expect, clock.Now())
			want := "PASS"
			if delta > HostObservationWindow {
				want = "UNKNOWN"
			}
			if e.Verdict != want {
				t.Fatalf("%s age %v: %+v", expect, delta, e.HostRestVerdict)
			}
		}
	}
	for _, kind := range []string{"revision", "boot"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "host")
			clock := &envelopeClock{RealClock{}.Now().UTC()}
			s, err := InitializeHostIntent(dir, "fixture-host", clock)
			if err != nil {
				t.Fatal(err)
			}
			boot, _ := HostBootID()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				current, readErr := ReadHostIntent(dir)
				if readErr != nil {
					t.Error(readErr)
				}
				q := HostQueueObservation{Host: s.Host, HostRevision: current.HostRevision, Boot: boot, Desired: current.Desired, At: clock.Now(), Pending: []HostObligation{}, Placement: &HostPlacementObservation{}}
				if kind == "revision" {
					changed := current
					changed.Revision++
					changed.Operation = "new-operation"
					changed.PreviousClose = changed.Revision
					changed.LastClose = &HostClose{HostRevision: changed.HostRevision, Settlement: "pending"}
					if err := atomicHostJSON(dir, hostStateFile, changed, nil); err != nil {
						t.Error(err)
					}
				} else {
					q.Boot = "previous-boot"
				}
				json.NewEncoder(w).Encode(q)
			}))
			defer server.Close()
			if err := AdoptHostWorker(dir, HostWorker{State: root, Host: s.Host, Name: "fixture-worker", Queue: server.URL, Session: "fixture-session"}); err != nil {
				t.Fatal(err)
			}
			t.Setenv("MYCE_TOKEN", "fixture-token")
			for _, expect := range []string{"closed", "ready"} {
				e, err := HostSnapshot(context.Background(), HostInvocation{StateDir: dir, Config: ServiceConfig{Name: "fixture-worker"}}, clock)
				if kind == "revision" && err != ErrHostRevision {
					t.Fatal("cross-revision collection accepted", err)
				}
				if err != nil {
					e.Errors = append(e.Errors, HostEnvelopeError(err))
				}
				e.ApplyRestVerdict(expect, clock.Now())
				if e.Verdict == "PASS" {
					t.Fatal("cross-fence PASS", kind, expect)
				}
			}
		})
	}
}

func TestHostRestPlacementObservation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		allowed []string
		closed  bool
		want    HostPlacementObservation
	}{
		{"local-fenced", []string{"local"}, true, HostPlacementObservation{Queued: 1, Fenced: 1}},
		{"local-unfenced", []string{"local"}, false, HostPlacementObservation{Queued: 1, Unfenced: 1}},
		{"unrestricted-fenced", nil, true, HostPlacementObservation{Queued: 1, Fenced: 1}},
		{"foreign-only", []string{"foreign"}, true, HostPlacementObservation{}},
		{"unknown-only", []string{"unknown"}, true, HostPlacementObservation{Queued: 1, Unresolved: 1}},
		{"foreign-plus-unknown", []string{"foreign", "unknown"}, true, HostPlacementObservation{Queued: 1, Unresolved: 1}},
		{"local-plus-unknown", []string{"local", "unknown"}, true, HostPlacementObservation{Queued: 1, Fenced: 1, Unresolved: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			q.mu.Lock()
			q.s.Machines["local"] = &Machine{ID: "local", HostID: "fixture-host"}
			q.s.Machines["foreign"] = &Machine{ID: "foreign", HostID: "foreign-host"}
			if tc.closed {
				q.s.HostIntents = map[string]QueueHostIntent{"fixture-host": {Desired: "closed"}}
			}
			q.s.Jobs["fixture-job"] = &Job{ID: "fixture-job", State: "queued", Spec: Spec{AllowedMachines: tc.allowed}}
			before, _ := json.Marshal(q.s)
			q.mu.Unlock()
			got, err := q.ReadHostObservation(context.Background(), "fixture-host")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*got.Placement, tc.want) {
				t.Fatalf("got %+v want %+v", got.Placement, tc.want)
			}
			clock.Advance(time.Second)
			q.mu.Lock()
			after, _ := json.Marshal(q.s)
			q.mu.Unlock()
			if string(before) != string(after) {
				t.Fatal("read changed queue")
			}
		})
	}
	t.Run("terminal-unreleased-and-retained", func(t *testing.T) {
		q, _ := fixture(t)
		q.mu.Lock()
		q.s.Machines["local"] = &Machine{ID: "local", HostID: "fixture-host"}
		j := &Job{ID: "terminal", State: "failed", Copies: []Copy{{Machine: "local", Session: "old-session", Attempt: 1}}}
		q.s.Jobs[j.ID] = j
		q.mu.Unlock()
		got, err := q.ReadHostObservation(context.Background(), "fixture-host")
		if err != nil || len(got.Pending) != 1 {
			t.Fatal(got, err)
		}
	})
}

func TestHostRestCollectorEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*HostQueueObservation)
	}{
		{"healthy", "PASS", func(q *HostQueueObservation) {}},
		{"unhealthy", "FAIL", func(q *HostQueueObservation) { q.Workers[0].Healthy = false }},
		{"worker-missing", "UNKNOWN", func(q *HostQueueObservation) { q.Workers = []HostReadyWorker{} }},
		{"worker-stale", "UNKNOWN", func(q *HostQueueObservation) { q.Workers[0].At = q.At.Add(-HostObservationWindow - time.Nanosecond) }},
		{"worker-rebooted", "UNKNOWN", func(q *HostQueueObservation) { q.Workers[0].Boot = "previous-boot" }},
		{"worker-revision", "UNKNOWN", func(q *HostQueueObservation) { q.Workers[0].Revision-- }},
		{"queue-stale", "UNKNOWN", func(q *HostQueueObservation) { q.At = q.At.Add(-HostObservationWindow - time.Nanosecond) }},
		{"queue-future", "UNKNOWN", func(q *HostQueueObservation) { q.At = q.At.Add(time.Nanosecond) }},
		{"queue-revision", "UNKNOWN", func(q *HostQueueObservation) { q.Revision-- }},
		{"queue-rebooted", "UNKNOWN", func(q *HostQueueObservation) { q.Boot = "previous-boot" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "host")
			clock := &envelopeClock{RealClock{}.Now().UTC()}
			s, err := InitializeHostIntent(dir, "fixture-host", clock)
			if err != nil {
				t.Fatal(err)
			}
			// Historical passive text must have no operational effect on readiness.
			s.Holds = []string{"obsolete cooldown diagnostic", "unmatched local result"}
			s.Desired = "open"
			s.Revision++
			s.Operation = "open-operation"
			if err := atomicHostJSON(dir, hostStateFile, s, nil); err != nil {
				t.Fatal(err)
			}
			boot, _ := HostBootID()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				q := HostQueueObservation{Host: s.Host, HostRevision: s.HostRevision, Boot: boot, Desired: "open", At: clock.Now(), Pending: []HostObligation{}, Workers: []HostReadyWorker{{Name: "fixture-worker", HostRevision: s.HostRevision, Boot: boot, At: clock.Now(), Healthy: true}}, Placement: &HostPlacementObservation{}}
				tc.change(&q)
				json.NewEncoder(w).Encode(q)
			}))
			defer server.Close()
			if err := AdoptHostWorker(dir, HostWorker{State: root, Host: s.Host, Name: "fixture-worker", Queue: server.URL, Session: "fixture-session"}); err != nil {
				t.Fatal(err)
			}
			t.Setenv("MYCE_TOKEN", "fixture-token")
			e, err := HostSnapshot(context.Background(), HostInvocation{StateDir: dir, Config: ServiceConfig{Name: "fixture-worker"}}, clock)
			if err != nil {
				t.Fatal(err)
			}
			e.ApplyRestVerdict("ready", clock.Now())
			if e.Verdict != tc.want {
				t.Fatalf("got %+v", e.HostRestVerdict)
			}
		})
	}
}
