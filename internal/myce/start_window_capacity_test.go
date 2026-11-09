package myce

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Reproduce a production recovery whose entire relative window was consumed
// by an uncharged live-host-growth refusal, including retained helper custody.
func TestStartWindowCapacityRecovery(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		m := machine("worker", "fake", "host")
		m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
		s := spec("production")
		s.StartWindow, s.SubmissionKey = "600s", "capacity-recovery"
		j := submit(t, q, s)
		first := check(t, q, m, nil).Job
		if err := q.Started(result(first, true)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Minute)
		finishInterrupted(t, q, first)
		recovery := check(t, q, m, nil).Job
		if recovery == nil || recovery.ID != j.ID || recovery.Spec.StartBy.Sub(clock.Now()) != 600*time.Second {
			t.Fatal("missing bounded recovery", recovery)
		}
		// The refusal can arrive after durable start authorization, before main exec.
		if err := q.Started(result(recovery, true)); err != nil {
			t.Fatal(err)
		}
		if err := q.Complete(capacityResult(recovery, true)); err != nil {
			t.Fatal(err)
		}
		before := job(t, q, j.ID)
		clock.Advance(30 * time.Minute)
		if next := check(t, q, m, nil).Job; next != nil {
			t.Fatal("unreleased helper reused", next)
		}
		q = reopenCancellationQueue(t, q, clock)
		waiting := job(t, q, j.ID)
		if waiting.State != "queued" || waiting.Losses != before.Losses || waiting.InfrastructureRetries != before.InfrastructureRetries || submit(t, q, s).ID != j.ID {
			t.Fatalf("capacity wait expired, charged, or duplicated: state=%s reason=%s", waiting.State, waiting.Reason)
		}
		if !strings.Contains(waiting.Reason, "600s left to start (paused for capacity)") {
			t.Fatal("missing paused start budget", waiting.Reason)
		}
		request := httptest.NewRequest(http.MethodGet, "/api/jobs/"+j.ID, nil)
		request.Header.Set("Authorization", "Bearer test")
		reply := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(reply, request)
		var observed Job
		if reply.Code != http.StatusOK || json.Unmarshal(reply.Body.Bytes(), &observed) != nil {
			t.Fatal("job observation failed", reply.Code)
		}
		if !strings.Contains(observed.Reason, "600s left to start (paused for capacity)") || !strings.Contains(JobReason(&observed), "600s left to start (paused for capacity)") {
			t.Fatal("API/status display lost paused budget", observed.Reason, JobReason(&observed))
		}
		release := result(recovery, false)
		release.ReleaseOnly = true
		if err := q.Complete(release); err != nil {
			t.Fatal(err)
		}
		m.HostAvailableMemoryGB++
		next := check(t, q, m, nil).Job
		if next == nil || next.ID != j.ID || next.Spec.StartBy.Sub(clock.Now()) != 600*time.Second {
			t.Fatal("capacity recovery did not restore the start budget", next)
		}
		if err := q.Started(result(next, true)); err != nil {
			t.Fatal(err)
		}
		finish(t, q, next, true)
		requireState(t, q, j.ID, "succeeded")

	})

	t.Run("StartAuthorizationRefusalRestoresRelativeWindow", func(t *testing.T) {
		q, clock := fixture(t)
		m := machine("worker", "fake")
		s := spec("production")
		s.StartWindow = "600s"
		submit(t, q, s)
		a := check(t, q, m, nil).Job
		if err := q.Started(result(a, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		if err := q.update(func() error { q.s.Machines[m.ID].Leaving = true; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := q.AuthorizeLaunch(HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)}); !errors.Is(err, ErrStale) {
			t.Fatal("missing refusal", err)
		}
		r := result(a, false)
		r.Interrupted = true
		r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "admission_changed", Predicate: "launch_authorization"}
		r.ExecutionEvidence = unstartedExecution(a.Attempt)
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, a.ID)
		if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || got.Spec.StartBy.Sub(clock.Now()) != 600*time.Second {
			t.Fatalf("relative recovery budget lost: state=%s losses=%d retries=%d cutoff=%v", got.State, got.Losses, got.InfrastructureRetries, got.Spec.StartBy)
		}

	})

	t.Run("StartWindowCapacityRecoveryStillBoundsStuckStart", func(t *testing.T) {
		for _, mode := range []string{"preparation", "unreachable"} {
			t.Run(mode, func(t *testing.T) {
				q, clock := fixture(t)
				m := machine("worker", "fake", "host")
				m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 1
				check(t, q, m, nil)
				s := spec("production")
				s.StartWindow, s.Needs.HostMemoryGB = "600s", 4
				j := submit(t, q, s)
				clock.Advance(30 * time.Minute)
				if got := job(t, q, j.ID); got.State != "queued" || !strings.Contains(got.Reason, "paused for capacity") {
					t.Fatal("initial capacity wait expired", got.State, got.Reason)
				}
				m.HostAvailableMemoryGB = 16
				offer := check(t, q, m, nil).Job
				if offer == nil || offer.ID != j.ID {
					t.Fatal("no offer after capacity recovered", offer)
				}
				clock.Advance(599 * time.Second)
				if mode == "preparation" {
					check(t, q, m, offer)
				}
				if got := job(t, q, j.ID); got.State != "running" || strings.Contains(got.Reason, "paused for capacity") || !strings.Contains(got.Reason, "1s left to start") {
					t.Fatal("assigned startup is not counting down", got.State, got.Reason)
				}
				clock.Advance(time.Second)
				if got := job(t, q, j.ID); got.State != "failed" || !strings.Contains(got.Reason, "start deadline exceeded") {
					t.Fatal("stuck startup escaped its window", got.State, got.Reason)
				}
				if err := q.Started(result(offer, true)); err == nil {
					t.Fatal("expired start was authorized")
				}
			})
		}

	})

	t.Run("StartWindowCapacityAbsoluteCutoffs", func(t *testing.T) {
		for _, kind := range []string{"start_by", "give_up_at", "deadline"} {
			t.Run(kind, func(t *testing.T) {
				q, clock := fixture(t)
				m := machine("worker", "fake", "host")
				m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
				s := spec("production")
				s.StartWindow = "600s"
				cutoff := clock.Now().Add(time.Minute)
				switch kind {
				case "start_by":
					s.StartBy = &cutoff
				case "give_up_at":
					s.GiveUpAt = &cutoff
				case "deadline":
					s.Deadline = &cutoff // Existing ordering hint, not an expiry policy.
				}
				j := submit(t, q, s)
				offer := check(t, q, m, nil).Job
				if err := q.Complete(capacityResult(offer, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Minute)
				got := job(t, q, j.ID)
				if kind == "deadline" {
					if got.State != "queued" || got.Spec.Deadline == nil || !got.Spec.Deadline.Equal(cutoff) {
						t.Fatal("ordering deadline changed", got.State, got.Spec.Deadline)
					}
				} else if got.State != "failed" || kind == "start_by" && !got.Spec.StartBy.Equal(cutoff) || kind == "give_up_at" && !got.Spec.GiveUpAt.Equal(cutoff) {
					t.Fatal("absolute cutoff moved or was ignored", got.State, got.Reason)
				}
			})
		}

	})

	t.Run("StartWindowSubmissionAndGET", func(t *testing.T) {
		q, clock := fixture(t)
		s := spec("production")
		s.StartWindow = "600s"
		j := submit(t, q, s)
		if !j.Spec.StartBy.Equal(clock.Now().Add(600 * time.Second)) {
			t.Fatal("relative submission cutoff", j.Spec.StartBy)
		}
		request := httptest.NewRequest(http.MethodGet, "/api/jobs/"+j.ID, nil)
		request.Header.Set("Authorization", "Bearer test")
		reply := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(reply, request)
		var got Job
		if err := json.Unmarshal(reply.Body.Bytes(), &got); err != nil || reply.Code != http.StatusOK {
			t.Fatalf("GET: %d %s: %v", reply.Code, reply.Body.String(), err)
		}
		if got.Spec.StartWindow != "600s" || got.StartWindow != 600*time.Second || !got.Spec.StartBy.Equal(*j.Spec.StartBy) {
			t.Fatalf("GET lost effective window: %+v", got)
		}

	})
}
