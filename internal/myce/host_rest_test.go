package myce

import (
	"context"
	"encoding/json"
	"fmt"
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
	e.Sources["required_workers"] = hostPtr(clock.Now())
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
			e.RestEvidence.QueueFacts = []HostRestItem[HostRestQueueFacts]{{e.RestEvidence.QueueAt, HostObservationWindow, &HostRestQueueFacts{OpenAdmission: true}}}
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
			e.RestEvidence.QueueFacts = []HostRestItem[HostRestQueueFacts]{{e.RestEvidence.QueueAt, HostObservationWindow, &HostRestQueueFacts{ClosedViolations: []string{"active_attempts", "release_pending"}}}}
		}},
		{"one-queue-release-another-unknown", "UNKNOWN", "release_pending", func(e *HostEnvelope) {
			e.Sources["queue"] = nil
			e.Counts.Unsettled = nil
			e.RestEvidence.QueueFacts = []HostRestItem[HostRestQueueFacts]{{e.RestEvidence.QueueAt, HostObservationWindow, &HostRestQueueFacts{ClosedViolations: []string{"release_pending"}}}}
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
		{"unhealthy-required-worker", "FAIL", "required_worker_unhealthy", func(e *HostEnvelope) {
			e.Ready = hostPtr(false)
			e.RestEvidence.WorkerHealth = []HostRestWorkerHealth{{e.Sources["required_workers"], HostObservationWindow, hostPtr(false)}}
		}},
		{"closed", "FAIL", "desired_state_mismatch", func(e *HostEnvelope) { e.Desired.State = hostPtr("closed") }},
		{"stale-revision", "FAIL", "observed_state_mismatch", func(e *HostEnvelope) { e.Observed.Revision = hostPtr(uint64(2)) }},
		{"unreleased", "UNKNOWN", "release_pending", func(e *HostEnvelope) { e.Counts.Unsettled = hostPtr(1) }},
		{"partial-unreleased", "UNKNOWN", "release_pending", func(e *HostEnvelope) {
			e.Ready = nil
			e.Sources["queue"] = nil
			e.RestEvidence.QueueFacts = []HostRestItem[HostRestQueueFacts]{{e.RestEvidence.QueueAt, HostObservationWindow, &HostRestQueueFacts{ClosedViolations: []string{"release_pending"}}}}
		}},
		{"queue-missing", "UNKNOWN", "queue_acknowledgement_unknown", func(e *HostEnvelope) { e.Sources["queue"] = nil; e.QueueRevision = nil }},
		{"fail-and-unknown", "FAIL", "required_workers_not_ready", func(e *HostEnvelope) {
			e.Ready = nil
			e.RestEvidence.WorkerHealth = []HostRestWorkerHealth{{e.Sources["required_workers"], HostObservationWindow, hostPtr(false)}}
			e.Sources["queue"] = nil
		}},
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
		{"worker-future", "UNKNOWN", func(q *HostQueueObservation) { q.Workers[0].At = q.At.Add(time.Nanosecond) }},
		{"newer-unhealthy-wins", "FAIL", func(q *HostQueueObservation) {
			older := q.Workers[0]
			older.At = older.At.Add(-time.Millisecond)
			q.Workers[0].Healthy = false
			q.Workers = append(q.Workers, older)
		}},
		{"newer-healthy-wins", "PASS", func(q *HostQueueObservation) {
			older := q.Workers[0]
			older.At, older.Healthy = older.At.Add(-time.Millisecond), false
			q.Workers = append([]HostReadyWorker{older}, q.Workers...)
		}},
		{"equal-time-unhealthy", "FAIL", func(q *HostQueueObservation) {
			other := q.Workers[0]
			other.Healthy = false
			q.Workers = append(q.Workers, other)
		}},
		{"worker-rebooted", "UNKNOWN", func(q *HostQueueObservation) { q.Workers[0].Boot = "previous-boot" }},
		{"worker-revision", "UNKNOWN", func(q *HostQueueObservation) { q.Workers[0].Revision-- }},
		{"queue-stale", "UNKNOWN", func(q *HostQueueObservation) { q.At = q.At.Add(-HostObservationWindow - time.Nanosecond) }},
		{"queue-future", "UNKNOWN", func(q *HostQueueObservation) { q.At = q.At.Add(time.Nanosecond) }},
		{"queue-error", "UNKNOWN", func(q *HostQueueObservation) {}},
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
				if tc.name == "queue-error" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
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
			if tc.name == "queue-error" && !slices.Contains(e.Reasons, "collection_failed") {
				t.Fatal(e.HostRestVerdict)
			}
		})
	}
}

func TestHostRestRequiredHeartbeatExpiresDuringCollection(t *testing.T) {
	for _, healthy := range []bool{true, false} {
		t.Run(fmt.Sprintf("healthy=%v", healthy), func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "host")
			clock := &envelopeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			s, err := InitializeHostIntent(dir, "fixture-host", clock)
			if err != nil {
				t.Fatal(err)
			}
			s.Desired = "open"
			s.Revision++
			s.Operation = "open-operation"
			if err := atomicHostJSON(dir, hostStateFile, s, nil); err != nil {
				t.Fatal(err)
			}
			boot, err := HostBootID()
			if err != nil {
				t.Fatal(err)
			}
			heartbeat := clock.Now().Add(-HostObservationWindow + time.Millisecond)
			observation := HostQueueObservation{Host: s.Host, HostRevision: s.HostRevision, Boot: boot, Desired: "open", At: clock.Now(), Workers: []HostReadyWorker{{Name: "fixture-worker", HostRevision: s.HostRevision, Boot: boot, At: heartbeat, Healthy: healthy}}, Placement: &HostPlacementObservation{}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(observation)
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
			wantFresh := "PASS"
			if !healthy {
				wantFresh = "FAIL"
			}
			if e.Ready == nil || *e.Ready != healthy || e.Verdict != wantFresh {
				t.Fatalf("fresh heartbeat not ready: %+v", e.HostRestVerdict)
			}
			// Preserve the evidence through the real dropped-child serialization boundary.
			data, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			var final HostEnvelope
			if err := json.Unmarshal(data, &final); err != nil {
				t.Fatal(err)
			}
			clock.at = clock.at.Add(2 * time.Millisecond)
			// A caller evaluating later than FinishObservation must also use that later
			// instant for the heartbeat, even while the queue observation is still fresh.
			e.ApplyRestVerdict("ready", clock.Now())
			if e.Verdict != "UNKNOWN" || !slices.Contains(e.Reasons, "required_workers_not_ready") {
				t.Fatalf("later evaluation accepted expired heartbeat: %+v", e.HostRestVerdict)
			}
			final.FinishObservation(clock.Now())
			final.ApplyRestVerdict("ready", clock.Now())
			if final.Ready != nil && *final.Ready || final.Verdict != "UNKNOWN" || !slices.Contains(final.Reasons, "required_workers_not_ready") {
				t.Fatalf("heartbeat age %v accepted: ready=%v verdict=%+v", clock.Now().Sub(heartbeat), final.Ready, final.HostRestVerdict)
			}
		})
	}
}

// Each row represents an independently consumed evidence type. Expiration is
// tested for both signs, not inferred from a healthy-only stale test. Optional
// diagnostics and facts outside an expectation's predicate remain nonblocking.
func TestHostRestEvidenceNormalizationMatrix(t *testing.T) {
	type row struct {
		name, source                string
		set                         func(*HostEnvelope, string, *time.Time)
		unhealthy, missing, expired [2]string // closed, ready
	}
	both := func(v string) [2]string { return [2]string{v, v} }
	closed := func(v string) [2]string { return [2]string{v, "PASS"} }
	rows := []row{
		{"observation", "", func(e *HostEnvelope, state string, at *time.Time) {
			e.At = *at
			if state != "healthy" {
				e.At = time.Time{}
			}
		}, both("UNKNOWN"), both("UNKNOWN"), both("UNKNOWN")},
		{"identity", "local_intent", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.Desired.State = hostPtr("invalid")
			}
			if state == "missing" {
				e.Host = nil
			}
		}, both("FAIL"), both("UNKNOWN"), both("UNKNOWN")},
		{"desired_state", "local_intent", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.Desired.State = hostPtr("invalid")
			}
			if state == "missing" {
				e.Desired.State = nil
			}
		}, both("FAIL"), both("UNKNOWN"), both("UNKNOWN")},
		{"desired_revision", "local_intent", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.Desired.Revision = hostPtr(uint64(2))
			}
			if state == "missing" {
				e.Desired.Revision = nil
			}
		}, both("FAIL"), both("UNKNOWN"), both("UNKNOWN")},
		{"observed_state", "observed_state", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.Observed.State = hostPtr("releasing")
			}
			if state == "missing" {
				e.Observed = HostEnvelopeState{}
			}
		}, both("FAIL"), both("UNKNOWN"), both("UNKNOWN")},
		{"queue_revision", "queue", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.QueueRevision = hostPtr(uint64(2))
			}
			if state == "missing" {
				e.QueueRevision = nil
			}
		}, both("FAIL"), both("UNKNOWN"), both("UNKNOWN")},
		{"observed_revision", "observed_state", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.Observed.Revision = hostPtr(uint64(2))
			}
			if state == "missing" {
				e.Observed.Revision = nil
			}
		}, both("FAIL"), both("UNKNOWN"), both("UNKNOWN")},
		{"local_exclusion", "local_intent", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.LocalExclusion = "FAIL"
			}
			if state == "missing" {
				e.LocalExclusion = "UNKNOWN"
			}
		}, closed("FAIL"), closed("UNKNOWN"), both("UNKNOWN")},
		{"queue_contact", "", func(e *HostEnvelope, state string, at *time.Time) {
			e.RestEvidence.QueueAt = at
			if state == "unhealthy" {
				e.RestEvidence.QueueReachable = hostPtr(false)
			}
			if state == "missing" {
				e.RestEvidence.QueueReachable = nil
			}
		}, both("UNKNOWN"), both("UNKNOWN"), both("UNKNOWN")},
		{"memory_allowance", "local_intent", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.RestEvidence.MemoryAllowance = hostPtr(int64(0))
			}
			if state == "missing" {
				e.RestEvidence.MemoryAllowance = nil
			}
		}, both("PASS"), both("PASS"), both("UNKNOWN")},
		{"worker_readiness", "required_workers", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.Ready = hostPtr(false)
			}
			if state == "missing" {
				e.Ready = nil
			}
		}, [2]string{"PASS", "UNKNOWN"}, [2]string{"PASS", "UNKNOWN"}, [2]string{"PASS", "UNKNOWN"}},
		{"worker_health", "", func(e *HostEnvelope, state string, at *time.Time) {
			value := hostPtr(state != "unhealthy")
			if state == "missing" {
				value = nil
				e.Ready = nil
			}
			e.RestEvidence.WorkerHealth = []HostRestWorkerHealth{{at, HostObservationWindow, value}}
			if state == "unhealthy" {
				e.Ready = hostPtr(false)
			}
		}, [2]string{"PASS", "FAIL"}, [2]string{"PASS", "UNKNOWN"}, [2]string{"PASS", "UNKNOWN"}},
		{"settlement", "local_intent", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.Settlement = "pending"
			}
			if state == "missing" {
				e.LastClose = nil
			}
		}, closed("UNKNOWN"), closed("UNKNOWN"), both("UNKNOWN")},
	}
	for _, component := range []string{"cgroups", "docker", "gpu_holders"} {
		rows = append(rows, row{component, component, func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				e.LocalExclusion = "FAIL"
				e.Errors = append(e.Errors, ExclusionError{Component: component, Code: "fixture_negative", Contrary: true})
			}
			if state == "missing" {
				e.Sources[component] = nil
			}
		}, closed("FAIL"), closed("UNKNOWN"), closed("UNKNOWN")})
	}
	for _, diagnostic := range []struct {
		name, source string
		set          func(*HostEnvelope, string)
		negative     [2]string
	}{
		{"cgroup_units", "cgroups", func(e *HostEnvelope, state string) {
			e.Units = []ExclusionUnit{{Unit: "myce-worker.service", Populated: hostPtr(state == "unhealthy")}}
			if state == "missing" {
				e.Units = nil
			}
		}, closed("FAIL")},
		{"running_containers", "docker", func(e *HostEnvelope, state string) {
			if state == "unhealthy" {
				e.Counts.Containers = hostPtr(1)
			}
			if state == "missing" {
				e.Counts.Containers = nil
			}
		}, closed("FAIL")},
		{"gpu_holder_count", "gpu_holders", func(e *HostEnvelope, state string) {
			if state == "unhealthy" {
				e.Counts.GPUHolders = hostPtr(1)
			}
			if state == "missing" {
				e.Counts.GPUHolders = nil
			}
		}, closed("FAIL")},
		{"pending_daemon", "docker", func(e *HostEnvelope, state string) {
			if state == "unhealthy" {
				e.Counts.PendingDaemon = hostPtr(1)
			}
			if state == "missing" {
				e.Counts.PendingDaemon = nil
			}
		}, closed("UNKNOWN")},
	} {
		rows = append(rows, row{diagnostic.name, diagnostic.source, func(e *HostEnvelope, state string, at *time.Time) {
			diagnostic.set(e, state)
			if state != "healthy" {
				e.LocalExclusion = "UNKNOWN"
			}
			if state == "unhealthy" && diagnostic.negative[0] == "FAIL" {
				e.LocalExclusion = "FAIL"
				e.Errors = append(e.Errors, ExclusionError{Component: diagnostic.source, Code: "fixture_negative", Contrary: true})
			}
		}, diagnostic.negative, closed("UNKNOWN"), closed("UNKNOWN")})
	}
	rows = append(rows, row{"restart_safety", "docker", func(e *HostEnvelope, state string, at *time.Time) {
		if state == "unhealthy" {
			e.RestartSafe = hostPtr(false)
			e.LocalExclusion = "FAIL"
			e.Errors = append(e.Errors, ExclusionError{Component: "docker", Code: "unsafe_restart", Contrary: true})
		}
		if state == "missing" {
			e.RestartSafe = nil
			e.LocalExclusion = "UNKNOWN"
		}
	}, closed("FAIL"), closed("UNKNOWN"), closed("UNKNOWN")})
	for _, count := range []struct {
		name             string
		value            func(*HostEnvelope) **int
		negative, absent [2]string
	}{
		{"active", func(e *HostEnvelope) **int { return &e.Counts.Active }, closed("FAIL"), closed("UNKNOWN")},
		{"preparing", func(e *HostEnvelope) **int { return &e.Counts.Preparing }, closed("FAIL"), closed("UNKNOWN")},
		{"stopping", func(e *HostEnvelope) **int { return &e.Counts.Stopping }, closed("FAIL"), closed("UNKNOWN")},
		{"release", func(e *HostEnvelope) **int { return &e.Counts.Unsettled }, both("UNKNOWN"), both("UNKNOWN")},
		{"queued", func(e *HostEnvelope) **int { return &e.Counts.Queued }, closed("PASS"), closed("UNKNOWN")},
		{"fenced", func(e *HostEnvelope) **int { return &e.Counts.FencedQueued }, closed("PASS"), closed("UNKNOWN")},
		{"unfenced", func(e *HostEnvelope) **int { return &e.RestEvidence.UnfencedQueued }, closed("FAIL"), closed("UNKNOWN")},
		{"unresolved_placement", func(e *HostEnvelope) **int { return &e.Counts.UnresolvedPlacement }, closed("UNKNOWN"), closed("UNKNOWN")},
	} {
		rows = append(rows, row{count.name, "queue", func(e *HostEnvelope, state string, at *time.Time) {
			if state == "unhealthy" {
				*count.value(e) = hostPtr(1)
			}
			if state == "missing" {
				*count.value(e) = nil
			}
		}, count.negative, count.absent, both("UNKNOWN")})
	}
	for _, kind := range []string{"admission", "partial_active", "partial_release"} {
		negative := closed("FAIL")
		if kind == "partial_release" {
			negative = both("UNKNOWN")
		}
		rows = append(rows, row{kind, "", func(e *HostEnvelope, state string, at *time.Time) {
			facts := &HostRestQueueFacts{}
			if state == "unhealthy" {
				switch kind {
				case "admission":
					facts.OpenAdmission = true
				case "partial_active":
					facts.ClosedViolations = []string{"active_attempts"}
				case "partial_release":
					facts.ClosedViolations = []string{"release_pending"}
				}
			}
			if state == "missing" {
				facts = nil
			}
			e.RestEvidence.QueueFacts = []HostRestItem[HostRestQueueFacts]{{at, HostObservationWindow, facts}}
		}, negative, both("UNKNOWN"), both("UNKNOWN")})
	}
	rows = append(rows, row{"exclusion_report", "exclusion_report", func(e *HostEnvelope, state string, at *time.Time) {
		if state == "unhealthy" {
			e.LocalExclusion = "FAIL"
			e.Errors = append(e.Errors, ExclusionError{Component: "gpu_holders", Code: "fixture_negative", Contrary: true})
		}
		if state == "missing" {
			e.Sources["exclusion_report"] = nil
		}
	}, closed("FAIL"), closed("UNKNOWN"), closed("UNKNOWN")})
	// 33 types x 3 ages x 3 signs/presence x 2 expectations = 594 cases.
	if len(rows) != 33 {
		t.Fatalf("update matrix size: %d", len(rows))
	}
	for _, tc := range rows {
		for _, age := range []time.Duration{HostObservationWindow - time.Nanosecond, HostObservationWindow, HostObservationWindow + time.Nanosecond} {
			for _, state := range []string{"healthy", "unhealthy", "missing"} {
				for expectationIndex, expect := range []string{"closed", "ready"} {
					t.Run(tc.name+"/"+age.String()+"/"+state+"/"+expect, func(t *testing.T) {
						clock := &envelopeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
						e := restEnvelope(clock, expect)
						at := hostPtr(clock.Now().Add(-age))
						if tc.source != "" {
							e.Sources[tc.source] = at
						}
						tc.set(&e, state, at)
						// The wire boundary must retain each fact's sign and observation time.
						data, err := json.Marshal(e)
						if err != nil {
							t.Fatal(err)
						}
						if err = json.Unmarshal(data, &e); err != nil {
							t.Fatal(err)
						}
						before, _ := json.Marshal(e)
						_, n := normalizeHostRestEvidence(e, clock.Now())
						verdict := restVerdict(n, expect)
						want := "PASS"
						if state == "unhealthy" {
							want = tc.unhealthy[expectationIndex]
						}
						if state == "missing" {
							want = tc.missing[expectationIndex]
						}
						if age > HostObservationWindow {
							want = tc.expired[expectationIndex]
						}
						if verdict.Verdict != want {
							t.Fatalf("got %+v, want %s", verdict, want)
						}
						// No clock or mutable collection state can change the normalized result.
						clock.at = clock.at.Add(time.Hour)
						if again := restVerdict(n, expect); !reflect.DeepEqual(verdict, again) {
							t.Fatalf("impure verdict: %+v -> %+v", verdict, again)
						}
						after, _ := json.Marshal(e)
						if string(before) != string(after) {
							t.Fatal("normalization mutated raw evidence")
						}
					})
				}
			}
		}
	}
}

func TestHostRestExpiredNegativeAndFreshNegative(t *testing.T) {
	clock := &envelopeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	for _, expect := range []string{"closed", "ready"} {
		for _, freshNegative := range []bool{false, true} {
			e := restEnvelope(clock, expect)
			old := hostPtr(clock.Now().Add(-HostObservationWindow - time.Nanosecond))
			e.Sources["gpu_holders"] = old
			e.Errors = []ExclusionError{{Component: "gpu_holders", Code: "owned_gpu_holder", Contrary: true}}
			e.LocalExclusion = "FAIL"
			e.Ready = hostPtr(false)
			e.RestEvidence.WorkerHealth = []HostRestWorkerHealth{{old, HostObservationWindow, hostPtr(false)}}
			if freshNegative {
				e.RestEvidence.WorkerHealth = append(e.RestEvidence.WorkerHealth, HostRestWorkerHealth{hostPtr(clock.Now()), HostObservationWindow, hostPtr(false)})
				e.RestEvidence.QueueFacts = []HostRestItem[HostRestQueueFacts]{{hostPtr(clock.Now()), HostObservationWindow, &HostRestQueueFacts{ClosedViolations: []string{"active_attempts"}}}}
			}
			e.FinishObservation(clock.Now())
			e.ApplyRestVerdict(expect, clock.Now())
			want := "UNKNOWN"
			if freshNegative {
				want = "FAIL"
			}
			if e.Verdict != want {
				t.Fatalf("%s fresh-negative=%v: %+v", expect, freshNegative, e.HostRestVerdict)
			}
		}
	}
}

func TestHostRestExclusionSignsExpireTogether(t *testing.T) {
	for _, component := range []string{"cgroups", "docker", "gpu_holders"} {
		for _, negative := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/negative=%v", component, negative), func(t *testing.T) {
				clock := &envelopeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
				e := restEnvelope(clock, "closed")
				at := clock.Now().Add(-HostObservationWindow + time.Millisecond)
				zero, yes := 0, true
				sources := map[string]*time.Time{}
				for _, key := range []string{"local_intent", "cgroups", "docker", "gpu_holders"} {
					sources[key] = hostPtr(clock.Now())
				}
				sources[component] = &at
				report := HostExclusionReport{HostRevision: HostRevision{*e.Desired.Revision, *e.Operation}, Boot: *e.Boot, At: clock.Now(), Verdict: "PASS", RestartSafe: &yes, Units: []ExclusionUnit{{Unit: "myce-worker.service", Populated: hostPtr(false)}}, RunningContainers: &zero, GPUHolders: &zero, PendingOperations: &zero, Sources: sources}
				if negative {
					report.Verdict = "FAIL"
					report.Errors = []ExclusionError{{Component: component, Code: "fixture_negative", Contrary: true}}
				}
				e.ApplyExclusion(&report, clock.Now())
				e.ApplyRestVerdict("closed", clock.Now())
				want := "PASS"
				if negative {
					want = "FAIL"
				}
				if e.Verdict != want {
					t.Fatalf("fresh: %+v", e.HostRestVerdict)
				}
				data, err := json.Marshal(e)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &e); err != nil {
					t.Fatal(err)
				}
				clock.at = clock.at.Add(2 * time.Millisecond)
				e.FinishObservation(clock.Now())
				e.ApplyRestVerdict("closed", clock.Now())
				if e.Verdict != "UNKNOWN" {
					t.Fatalf("expired: %+v", e.HostRestVerdict)
				}
				for _, problem := range e.Errors {
					if problem.Contrary {
						t.Fatal("expired contrary fact survived normalization")
					}
				}
			})
		}
	}
}

func TestHostRestFinishDoesNotRenewEvidence(t *testing.T) {
	clock := &envelopeClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	for _, expect := range []string{"closed", "ready"} {
		e := restEnvelope(clock, expect)
		original := e.At
		e.FinishObservation(original.Add(5 * time.Second))
		if !e.At.Equal(original.Add(5*time.Second)) || e.Sources["observation"] == nil || !e.Sources["observation"].Equal(original) {
			t.Fatal("completion changed source time")
		}
		e.FinishObservation(original.Add(HostObservationWindow + time.Nanosecond))
		e.ApplyRestVerdict(expect, original.Add(HostObservationWindow+time.Nanosecond))
		if e.Verdict != "UNKNOWN" || !slices.Contains(e.Reasons, "observation_stale") {
			t.Fatal("projection renewed observation", e.HostRestVerdict)
		}
	}
}
