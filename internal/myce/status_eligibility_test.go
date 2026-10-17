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

func TestStatusEligibleHostsLifecycle(t *testing.T) {
	q, c := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	a.HostID, b.HostID = "host-a", "host-b"
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.AllowedMachines = []string{"a", "unknown"}
	s.Run.Command = []string{"private-command"}
	s.Run.Env = map[string]string{"SECRET": "private-env"}
	j := submit(t, q, s)
	assertEligibleStatus(t, q, j.ID, "queued", s.AllowedMachines, []string{"host-a"}, false)
	running := check(t, q, a, nil).Job
	if running == nil {
		t.Fatal("no assignment")
	}
	assertEligibleStatus(t, q, j.ID, "running", s.AllowedMachines, []string{"host-a"}, false)
	c.Advance(LossAfter)
	check(t, q, b, nil)
	check(t, q, observer(), nil)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	requeued := job(t, q, j.ID)
	if requeued.Machine != "" || requeued.Losses != 1 {
		t.Fatalf("infra retry: machine=%q losses=%d", requeued.Machine, requeued.Losses)
	}
	assertEligibleStatus(t, q, j.ID, "queued", s.AllowedMachines, []string{"host-a"}, false)
	if check(t, q, a, nil).Job == nil {
		t.Fatal("no retry assignment")
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	assertEligibleStatus(t, q, j.ID, "stopping", s.AllowedMachines, []string{"host-a"}, false)
	if err := q.Complete(result(job(t, q, j.ID), false)); err != nil {
		t.Fatal(err)
	}
	terminalJob := statusJobs(t, q)[j.ID]
	if string(terminalJob["state"]) != `"cancelled"` || terminalJob["eligible_hosts"] != nil || terminalJob["eligible_unrestricted"] != nil || terminalJob["eligible_unknown_machines"] != nil {
		t.Fatal("terminal eligibility must be omitted")
	}
	stored := job(t, q, j.ID)
	if stored.EligibleHosts != nil || stored.EligibleUnrestricted || stored.EligibleUnknownMachines != nil || stored.Spec.Run.Env["SECRET"] != "private-env" || stored.Spec.Run.Command[0] != "private-command" {
		t.Fatal("status mutated persisted job")
	}
}

func TestStatusEligibleHostsPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		allowed, project, hosts []string
		unrestricted            bool
	}{
		{name: "unrestricted", unrestricted: true},
		{name: "unknown", allowed: []string{"unknown"}, hosts: []string{}},
		{name: "intersection", allowed: []string{"a", "b", "unknown"}, project: []string{"b", "unknown"}, hosts: []string{"host-b"}},
		{name: "project only", project: []string{"b"}, hosts: []string{"host-b"}},
		{name: "deduplicate and sort", allowed: []string{"a", "b", "c", "d"}, hosts: []string{"d", "host-a", "host-b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			for _, pair := range [][2]string{{"a", "host-a"}, {"b", "host-b"}, {"c", "host-a"}, {"d", ""}} {
				m := machine(pair[0], "fake")
				m.HostID = pair[1]
				check(t, q, m, nil)
			}
			if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: tc.project}}); err != nil {
				t.Fatal(err)
			}
			s := spec("experiment")
			s.AllowedMachines = tc.allowed
			j := submit(t, q, s)
			assertEligibleStatus(t, q, j.ID, "queued", tc.allowed, tc.hosts, tc.unrestricted)
		})
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

func TestStatusEligibleUnknownMachines(t *testing.T) {
	for _, tc := range []struct {
		name         string
		job, project []string
	}{
		{name: "project only", project: []string{"unknown-z", "other", "unknown-a", "unknown-z"}},
		{name: "job only", job: []string{"unknown-z", "other", "unknown-a", "unknown-z"}},
		{name: "intersection", job: []string{"unknown-z", "job-only", "other", "unknown-a", "unknown-z"}, project: []string{"project-only", "unknown-a", "other", "unknown-z"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("other", "fake")
			m.HostID = "host-other"
			check(t, q, m, nil)
			if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: tc.project}}); err != nil {
				t.Fatal(err)
			}
			s := spec("experiment")
			s.AllowedMachines = tc.job
			j := submit(t, q, s)
			assertEligibleStatus(t, q, j.ID, "queued", tc.job, []string{"host-other"}, false)
			assertUnknownMachines(t, q, j.ID, []string{"unknown-a", "unknown-z"})
			// A joining worker resolves only its own unknown name into a host ID.
			joined := machine("unknown-a", "fake")
			joined.HostID = "host-new"
			joined.Leaving = true
			check(t, q, joined, nil)
			assertEligibleStatus(t, q, j.ID, "queued", tc.job, []string{"host-new", "host-other"}, false)
			assertUnknownMachines(t, q, j.ID, []string{"unknown-z"})
			if err := q.Cancel(j.ID); err != nil {
				t.Fatal(err)
			}
			assertUnknownMachines(t, q, j.ID, nil)
		})
	}
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	assertUnknownMachines(t, q, j.ID, nil)
}

func TestStatusEligibleHostsRetainActiveAssignment(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("a", "fake"), machine("b", "fake")
	a.HostID, b.HostID = "host-a", "host-b"
	check(t, q, a, nil)
	check(t, q, b, nil)
	j := submit(t, q, spec("experiment"))
	if check(t, q, a, nil).Job == nil {
		t.Fatal("no assignment")
	}
	assertEligibleStatus(t, q, j.ID, "running", nil, []string{"host-a"}, true)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{"b"}}}); err != nil {
		t.Fatal(err)
	}
	assertEligibleStatus(t, q, j.ID, "running", nil, []string{"host-a", "host-b"}, false)
	if err := q.update(func() error { q.s.Jobs[j.ID].ProductionHost = "host-production"; return nil }); err != nil {
		t.Fatal(err)
	}
	assertEligibleStatus(t, q, j.ID, "running", nil, []string{"host-a", "host-b", "host-production"}, false)
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	assertEligibleStatus(t, q, j.ID, "stopping", nil, []string{"host-a", "host-b", "host-production"}, false)
}

func TestStatusEligibleHostsRetainInflightModelCall(t *testing.T) {
	for _, restricted := range []bool{false, true} {
		t.Run(stringBool(restricted), func(t *testing.T) {
			q, _, m := modelFixture(t)
			m.HostID = "host-current"
			check(t, q, m, nil)
			other := machine("other", "fake")
			other.HostID = "host-other"
			check(t, q, other, nil)
			call := addModelCall(t, q, "production")
			reportModel(t, q, m, "ready")
			if call.Machine != m.ID {
				t.Fatal("call not in flight")
			}
			if restricted {
				if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{"other"}}}); err != nil {
					t.Fatal(err)
				}
			}
			if err := q.update(func() error { call.ProductionHost = "host-production"; return nil }); err != nil {
				t.Fatal(err)
			}
			raw := statusJobs(t, q)[call.ID]
			var hosts []string
			if err := json.Unmarshal(raw["eligible_hosts"], &hosts); err != nil {
				t.Fatal(err)
			}
			want := []string{"host-current", "host-production"}
			if restricted {
				want = []string{"host-current", "host-other", "host-production"}
			}
			if !reflect.DeepEqual(hosts, want) {
				t.Fatalf("hosts=%v want %v", hosts, want)
			}
			if (string(raw["eligible_unrestricted"]) == "true") == restricted {
				t.Fatal("incorrect unrestricted marker")
			}
			assertUnknownMachines(t, q, call.ID, nil)
		})
	}
}
