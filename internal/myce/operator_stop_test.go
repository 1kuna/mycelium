package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func operatorResult(r Result) Result {
	data, _ := json.Marshal(r)
	var obj map[string]any
	_ = json.Unmarshal(data, &obj)
	obj["operator_stop"] = true
	data, _ = json.Marshal(obj)
	_ = json.Unmarshal(data, &r)
	return r
}
func TestOperatorStopRefundsExactLossOnce(t *testing.T) {
	q, c := fixture(t)
	m := machine("operator", "fake")
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	running := check(t, q, m, nil).Job
	c.Advance(10 * time.Minute)
	check(t, q, observer(), nil)
	if e := q.Tick(); e != nil {
		t.Fatal(e)
	}
	if job(t, q, j.ID).Losses != 1 {
		t.Fatal("loss not charged")
	}
	wrong := operatorResult(result(running, false))
	wrong.Session = "wrong"
	_ = q.Complete(wrong)
	if job(t, q, j.ID).Losses != 1 {
		t.Fatal("wrong session refunded")
	}
	r := operatorResult(result(running, false))
	r.Interrupted = true
	if e := q.Complete(r); e != nil {
		t.Fatal(e)
	}
	if got := job(t, q, j.ID); got.Losses != 0 || got.State != "queued" {
		t.Fatalf("not refunded: %+v", got)
	}
	if e := q.Complete(r); e != nil {
		t.Fatal(e)
	}
	if job(t, q, j.ID).Losses != 0 {
		t.Fatal("duplicate changed losses")
	}
}
func TestOperatorStopBeforeLossIsFree(t *testing.T) {
	q, _ := fixture(t)
	m := machine("operator", "fake")
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	running := check(t, q, m, nil).Job
	r := operatorResult(result(running, false))
	r.Interrupted = true
	if e := q.Complete(r); e != nil {
		t.Fatal(e)
	}
	if got := job(t, q, j.ID); got.Losses != 0 || got.State != "queued" {
		t.Fatalf("operator stop charged: %+v", got)
	}
}

func TestOperatorStopMarkerSurvivesStartupAndIsAttemptSpecific(t *testing.T) {
	w := recoveryWorker(t)
	s, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	j := recoveryJob(t, []string{"echo", "done"})
	s.state = workerState{Session: "old", Identity: "owned", Job: &j, Log: "owned.log"}
	if e = s.save(); e != nil {
		t.Fatal(e)
	}
	if e = saveWorkerJSON(s.dir, "operator-stop.json", s.state); e != nil {
		t.Fatal(e)
	}
	s.close()
	s, e = w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	data, _ := json.Marshal(s.state)
	if !strings.Contains(string(data), `"operator_stopped":true`) {
		t.Fatalf("marker not imported: %s", data)
	}
	s.state.Identity = "new-attempt"
	data, _ = json.Marshal(s.state)
	var state map[string]any
	_ = json.Unmarshal(data, &state)
	state["operator_stopped"] = false
	data, _ = json.Marshal(state)
	_ = json.Unmarshal(data, &s.state)
	if e = s.save(); e != nil {
		t.Fatal(e)
	}
	s.close()
	s, e = w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	data, _ = json.Marshal(s.state)
	if strings.Contains(string(data), `"operator_stopped":true`) {
		t.Fatal("stale marker applied to another attempt")
	}
}

func TestOperatorStopMarkerReconcilesWorkerWithQueue(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			q, c := fixture(t)
			m := machine("operator", "fake", "test-host")
			check(t, q, m, nil)
			j := submit(t, q, spec("experiment"))
			old := check(t, q, m, nil).Job
			w := recoveryWorker(t)
			w.Machine = m
			w.CheckpointRoot = q.root
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			w.Client = Client{URL: server.URL, Token: "test"}
			s, e := w.openStore()
			if e != nil {
				t.Fatal(e)
			}
			s.state = workerState{Session: m.Session, Identity: "operator-old", Job: old, Log: "operator-old.log", CheckpointRoot: q.root}
			if e = s.save(); e != nil {
				t.Fatal(e)
			}
			data, e := os.ReadFile(filepath.Join(s.dir, "state.json"))
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(s.dir, "operator-stop.json"), data, 0600); e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(s.dir, "operator-old.log"), nil, 0600); e != nil {
				t.Fatal(e)
			}
			if success {
				r := result(old, true)
				if e = saveWorkerJSON(s.dir, "operator-old.result.json", r); e != nil {
					t.Fatal(e)
				}
			}
			s.close()
			c.Advance(LossAfter)
			check(t, q, observer(), nil)
			if job(t, q, j.ID).Losses != 1 {
				t.Fatal("queue did not charge lost worker")
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			await(t, func() bool {
				got := job(t, q, j.ID)
				if success {
					return got.State == "succeeded"
				}
				return got.Losses == 0
			})
			if job(t, q, j.ID).Losses != 0 {
				cancel()
				<-done
				t.Fatal("successful completion raced operator stop and retained its loss")
			}
			cancel()
			select {
			case e := <-done:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("worker did not finish")
			}
		})
	}
}

func TestOperatorStopRefundRevivesThirdLossAndPreservesOtherLosses(t *testing.T) {
	q, c := fixture(t)
	m := machine("operator", "fake")
	s := spec("experiment")
	s.Resumable = false
	j := submit(t, q, s)
	var old *Job
	for i := 0; i < 3; i++ {
		old = check(t, q, m, nil).Job
		if old == nil {
			t.Fatal("no assignment")
		}
		c.Advance(LossAfter)
		check(t, q, observer(), nil)
	}
	if got := job(t, q, j.ID); got.Losses != 3 || got.State != "failed" {
		t.Fatalf("not third loss: %+v", got)
	}
	r := operatorResult(result(old, false))
	r.Interrupted = true
	if e := q.Complete(r); e != nil {
		t.Fatal(e)
	}
	if got := job(t, q, j.ID); got.Losses != 2 || got.State != "queued" {
		t.Fatalf("refund did not revive: %+v", got)
	}
	if e := q.Complete(r); e != nil {
		t.Fatal(e)
	}
	if got := job(t, q, j.ID); got.Losses != 2 {
		t.Fatal("duplicate refunded another attempt")
	}
}

func TestOperatorStopMarkerRejectsForeignDocuments(t *testing.T) {
	for _, marker := range []string{`{}`, `null`, `{"state":"running","pid":999999}`, `{"session":"old","fresh":false,"offset":0,"attempt_token":"foreign"}`} {
		t.Run(marker, func(t *testing.T) {
			w := recoveryWorker(t)
			s, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			if err = s.save(); err != nil {
				s.close()
				t.Fatal(err)
			}
			s.close()
			statePath := filepath.Join(w.StateDir, "state.json")
			before, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			markerPath := filepath.Join(w.StateDir, "operator-stop.json")
			if err = os.WriteFile(markerPath, []byte(marker), 0600); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				store, err := w.openStore()
				if err == nil {
					store.close()
					t.Fatal("invalid marker accepted")
				}
				if errors.Is(err, ErrLegacyState) || !strings.Contains(err.Error(), "read operator-stop marker") {
					t.Fatal("expected ordinary current-state decode refusal", err)
				}
			}
			after, err := os.ReadFile(statePath)
			if err != nil || string(after) != string(before) {
				t.Fatal("rejected marker changed state")
			}
			after, err = os.ReadFile(markerPath)
			if err != nil || string(after) != marker {
				t.Fatal("rejected marker changed")
			}
		})
	}
}
