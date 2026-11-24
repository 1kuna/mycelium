package myce

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestStartWindowSubmissionAndGET(t *testing.T) {
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
}

func TestStartWindowFanOutDisabledFleetFailsPromptly(t *testing.T) {
	q, clock, m, s := fanOutFixture(t, 1)
	if err := setMaintenance(t, q, hostID(&m), "disable"); err != nil {
		t.Fatal(err)
	}
	s.Priority = "production"
	parent := submit(t, q, s)
	got := job(t, q, parent.ID)
	if got.State != "failed" || !strings.Contains(got.Reason, "no eligible host is enabled") || got.Attempt != 0 || !got.FinishedAt.Equal(clock.Now()) {
		t.Fatalf("disabled-only fleet did not reject unassigned fan-out immediately: %+v", got)
	}
}

func TestStartWindowFanOutLaterWave(t *testing.T) {
	for _, absolute := range []bool{false, true} {
		t.Run(map[bool]string{false: "relative", true: "absolute"}[absolute], func(t *testing.T) {
			q, clock, m, s := fanOutFixture(t, 2)
			s.Priority, s.StartWindow = "production", "600s"
			s.FanOut.MaxConcurrency = 1
			cutoff := clock.Now().Add(300 * time.Second)
			if absolute {
				s.StartBy = &cutoff
			}
			parent := submit(t, q, s)
			first := check(t, q, m, nil).Job
			if first == nil || first.Batch == nil || len(first.Batch.Pieces) != 1 {
				t.Fatal("expected first single-piece wave")
			}
			if err := q.Started(result(first, true)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(601 * time.Second)
			check(t, q, m, first)
			if running := job(t, q, first.ID); running.State != "running" || running.StartedAt == nil {
				t.Fatalf("deadline killed an accepted command: %+v", running)
			}
			finish(t, q, first, true)
			next := check(t, q, m, nil).Job
			if absolute {
				if got := job(t, q, parent.ID); got.State != "failed" || next != nil {
					t.Fatalf("expired absolute tail dispatched: %+v %+v", got, next)
				}
			} else if next == nil || next.StartWindow != 600*time.Second || !next.Spec.StartBy.Equal(clock.Now().Add(600*time.Second)) {
				t.Fatalf("later wave lost its full relative budget: %+v", next)
			}
		})
	}
}

func TestStartWindowValidation(t *testing.T) {
	for _, value := range []string{"-1s", "0s", "1h1ns", "600", "forever", "999999999999999999999h"} {
		t.Run(value, func(t *testing.T) {
			s := spec("production")
			s.StartWindow = value
			if err := s.Validate(); err == nil {
				t.Fatal("invalid window accepted")
			}
		})
	}
	s := spec("experiment")
	s.StartWindow = "600s"
	if err := s.Validate(); err == nil {
		t.Fatal("experiment window accepted")
	}
	s.Priority = "production"
	for _, value := range []string{"1ns", "600s", "1h"} {
		if err := yaml.Unmarshal([]byte("start_window: "+value), &s); err != nil {
			t.Fatal(err)
		}
		if err := s.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
