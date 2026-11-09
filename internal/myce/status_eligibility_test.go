package myce

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func statusJobs(t *testing.T, q *Queue) map[string]map[string]json.RawMessage {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/status", nil)
	req.Header.Set("Authorization", "Bearer fixture-secret")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "fixture-secret"}.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status code %d", w.Code)
	}
	for _, secret := range []string{"fixture-secret", "private-command", "private-env", "private-body", "private-checkpoint"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("status leaked %s", secret)
		}
	}
	var s struct {
		Jobs []map[string]json.RawMessage `json:"jobs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]json.RawMessage{}
	for _, j := range s.Jobs {
		var id string
		if err := json.Unmarshal(j["id"], &id); err != nil {
			t.Fatal(err)
		}
		out[id] = j
	}
	return out
}

func assertEligibleStatus(t *testing.T, q *Queue, id, state string, allowed, hosts []string, unrestricted bool) {
	t.Helper()
	j := statusJobs(t, q)[id]
	if string(j["state"]) != `"`+state+`"` {
		t.Fatalf("state=%s want %s", j["state"], state)
	}
	var s Spec
	if err := json.Unmarshal(j["spec"], &s); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.AllowedMachines, allowed) {
		t.Fatalf("allowed=%v want %v", s.AllowedMachines, allowed)
	}
	if unrestricted {
		if string(j["eligible_unrestricted"]) != "true" {
			t.Fatalf("unrestricted fields: %v", j)
		}
		if hosts == nil {
			if j["eligible_hosts"] != nil {
				t.Fatal("unoccupied unrestricted job must omit hosts")
			}
		} else {
			var got []string
			if err := json.Unmarshal(j["eligible_hosts"], &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, hosts) {
				t.Fatalf("eligible=%v want %v", got, hosts)
			}
		}
	} else {
		var got []string
		if err := json.Unmarshal(j["eligible_hosts"], &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, hosts) || j["eligible_unrestricted"] != nil {
			t.Fatalf("eligible=%v want %v", got, hosts)
		}
	}
}

func TestStatusEligibilityKeepsRequestPrivate(t *testing.T) {
	q, _, m := modelFixture(t)
	received := make(chan string, 1)
	release := make(chan struct{})
	upstream := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		received <- string(body)
		<-release
		w.Write([]byte(`{}`))
	}))
	defer upstream.Close()
	defer close(release)
	m.ModelURL = upstream.URL
	check(t, q, m, nil)
	seed := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	q.finishCall(seed.ID)
	s := spec("experiment")
	s.AllowedMachines = []string{m.ID}
	s.Run.Command = []string{"private-command"}
	s.Run.Env = map[string]string{"SECRET": "private-env"}
	// Exercise a queued batch's private fields without competing for the idle
	// model's memory: same-tier demand is allowed to reclaim that residency.
	s.Needs.MemoryGB = 16
	j := submit(t, q, s)
	if err := q.update(func() error { q.s.Jobs[j.ID].Checkpoint = "private-checkpoint"; return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := httptest.NewRequest("POST", "/models/test/ocr", strings.NewReader(`{"model":"test","input":"private-body"}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer fixture-secret")
	req.Header.Set("X-Myce-Project", "test")
	req.Header.Set("X-Myce-Priority", "production")
	done := make(chan struct{})
	go func() {
		defer close(done)
		API{Queue: q, Token: "fixture-secret"}.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}()
	defer func() { cancel(); <-done }()
	select {
	case body := <-received:
		if !strings.Contains(body, "private-body") {
			t.Fatal("request body not exercised")
		}
	case <-ctx.Done():
		t.Fatal("request did not reach upstream")
	}
	assertEligibleStatus(t, q, j.ID, "queued", s.AllowedMachines, []string{m.ID}, false)
	// All nonterminal model-call summaries also carry an explicit marker.
	jobs := statusJobs(t, q)
	if len(jobs) != 2 {
		t.Fatalf("expected batch and in-flight call, got %d", len(jobs))
	}
	for id, call := range jobs {
		if id != j.ID && string(call["eligible_unrestricted"]) != "true" {
			t.Fatal("call missing eligibility")
		}
	}
}

func assertUnknownMachines(t *testing.T, q *Queue, id string, want []string) {
	t.Helper()
	raw := statusJobs(t, q)[id]["eligible_unknown_machines"]
	if len(want) == 0 {
		if raw != nil {
			t.Fatalf("empty unknown machines must be omitted: %s", raw)
		}
		return
	}
	var got []string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unknown machines=%v want %v", got, want)
	}
}
