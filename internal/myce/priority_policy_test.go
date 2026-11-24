package myce

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func examplePriorityPolicy() PriorityPolicy {
	return PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "urgent", Selectors: []string{"verso/production"}}, {Name: "shared", Selectors: []string{"atlas/*", "verso/experiment"}}, {Name: "other", Selectors: []string{"*/*"}}}}
}

func TestPriorityPolicyClassNormalization(t *testing.T) {
	q, _ := fixture(t)
	for _, class := range []string{"", "experiment", "production", "preview"} {
		s := spec("experiment")
		s.Priority = ""
		s.Class = class
		j := submit(t, q, s)
		want := class
		if want == "" {
			want = "experiment"
		}
		if j.Spec.Class != want {
			t.Fatalf("class = %q", j.Spec.Class)
		}
		if err := j.Spec.Validate(); err != nil {
			t.Fatalf("normalized spec invalid: %v", err)
		}
	}
	for _, s := range []Spec{func() Spec { s := spec("production"); s.Class = "experiment"; return s }(), func() Spec { s := spec("experiment"); s.Class = "bad/class"; return s }(), func() Spec { s := spec("experiment"); s.Project = "bad/project"; return s }(), func() Spec { s := spec("deadline"); s.Deadline = nil; return s }()} {
		if _, err := q.Submit(s); err == nil {
			t.Fatal("invalid class/legacy input accepted")
		}
	}
}

func TestPriorityPolicyValidation(t *testing.T) {
	q, _ := fixture(t)
	for _, change := range []func(*PriorityPolicy){func(p *PriorityPolicy) { p.Version = 2 }, func(p *PriorityPolicy) { p.Tiers[2].Selectors = []string{"third/*"} }, func(p *PriorityPolicy) { p.Tiers[0].Selectors = []string{"bad"} }, func(p *PriorityPolicy) { p.Tiers[0].Selectors = []string{"*/"} }, func(p *PriorityPolicy) { p.Tiers[0].Selectors = []string{"[bad/*"} }, func(p *PriorityPolicy) { p.Tiers[0].Name = "shared" }, func(p *PriorityPolicy) { p.Projects = map[string]ProjectPolicy{"x": {Weight: -1}} }} {
		p := examplePriorityPolicy()
		change(&p)
		if _, err := q.SetPriorityPolicy(0, p); err == nil {
			t.Fatal("invalid policy accepted")
		}
		if q.PriorityPolicy().Revision != 0 {
			t.Fatal("invalid update mutated revision")
		}
	}
	p := examplePriorityPolicy()
	p.Tiers[0].Selectors = append(p.Tiers[0].Selectors, "*/*")
	record, err := q.SetPriorityPolicy(0, p)
	if err != nil || len(record.Warnings) != 3 {
		t.Fatalf("shadow warnings: %+v %v", record, err)
	}
}

func TestPriorityPolicyAPI(t *testing.T) {
	q, _ := fixture(t)
	handler := API{Queue: q, Token: "test-owner"}.Handler()
	request := func(method, body, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/api/policy", strings.NewReader(body))
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := request("GET", "", "wrong"); w.Code != 401 {
		t.Fatalf("unauthorized: %d", w.Code)
	}
	body, _ := json.Marshal(map[string]any{"expected_revision": 0, "policy": examplePriorityPolicy()})
	if w := request("PUT", string(body), "test-owner"); w.Code != 200 {
		t.Fatalf("set: %d %s", w.Code, w.Body)
	}
	if w := request("PUT", string(body), "test-owner"); w.Code != 409 || !strings.Contains(w.Body.String(), "policy_revision_conflict") {
		t.Fatalf("CAS: %d %s", w.Code, w.Body)
	}
	if w := request("PUT", `{"policy":{"version":1}}`, "test-owner"); w.Code != 400 {
		t.Fatalf("missing revision: %d", w.Code)
	}
	w := request("GET", "", "test-owner")
	var record PriorityPolicyRecord
	if err := json.Unmarshal(w.Body.Bytes(), &record); err != nil || record.Revision != 1 {
		t.Fatalf("get: %+v %v", record, err)
	}
}

func TestPriorityPolicyStatus(t *testing.T) {
	q, _ := fixture(t)
	p := examplePriorityPolicy()
	p.Projects = map[string]ProjectPolicy{"verso": {}, "atlas": {Weight: 2}}
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	if q.PriorityPolicy().Policy.Projects["verso"].Weight != 1 {
		t.Fatal("missing weight did not default")
	}
	s := spec("experiment")
	s.Project = "verso"
	j := submit(t, q, s)
	handler := API{Queue: q, Token: "owner-test"}.Handler()
	for _, url := range []string{"/api/status?view=snapshot", "/"} {
		r := httptest.NewRequest("GET", url, nil)
		r.Header.Set("Authorization", "Bearer owner-test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("status %s: %d", url, w.Code)
		}
		if url != "/" {
			var snapshot Snapshot
			if err := json.Unmarshal(w.Body.Bytes(), &snapshot); err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Jobs) != 1 || snapshot.Jobs[0].ID != j.ID || snapshot.Jobs[0].Spec.Class != "experiment" || snapshot.Jobs[0].PriorityPolicy.Tier != "shared" || snapshot.PriorityPolicy.Revision != 1 {
				t.Fatalf("status: %s", w.Body)
			}
		} else if !strings.Contains(w.Body.String(), "tier shared") || !strings.Contains(w.Body.String(), "verso/experiment") || !strings.Contains(w.Body.String(), "policy revision 1") {
			t.Fatal("tier/class/revision missing from HTML")
		}
	}
}

func TestPriorityPolicyRejectsSlashProject(t *testing.T) {
	q, _, _ := modelFixture(t)
	const project = "team/subproject"
	t.Run("submit", func(t *testing.T) {
		s := spec("experiment")
		s.Project = project
		if _, err := q.Submit(s); err == nil {
			t.Fatal("slash project accepted")
		}
	})
	for _, defaults := range []bool{false, true} {
		name := "generic route"
		if defaults {
			name = "entry default"
		}
		t.Run(name, func(t *testing.T) {
			c := &ModelCall{Model: "test", Project: project, Priority: "experiment"}
			if defaults {
				c.Project = ""
			}
			if err := q.callDefaults(c, Model{Project: project}, nil, defaults); err == nil {
				t.Fatal("slash project accepted")
			}
		})
	}
	t.Run("call admission", func(t *testing.T) {
		if err := q.addCall(&ModelCall{Model: "test", Project: project, Priority: "experiment"}); err == nil {
			t.Fatal("slash project accepted")
		}
	})
	t.Run("model config", func(t *testing.T) {
		config := q.models
		config.Models = append([]Model(nil), config.Models...)
		config.Models[0].Project = project
		if err := validateModels(config); err == nil {
			t.Fatal("slash project accepted")
		}
	})
	t.Run("pin", func(t *testing.T) {
		if err := q.Pin(ModelPin{Project: project, Model: "test", Machine: "one"}); err == nil {
			t.Fatal("slash project accepted")
		}
	})
	t.Run("legacy identity", func(t *testing.T) {
		got := q.priorityMatch(Spec{Project: project, Class: "experiment"})
		if got.Tier != "experiment" || got.Selector != "*/*" {
			t.Fatalf("legacy identity did not fall back safely: %+v", got)
		}
	})
	t.Run("project settings", func(t *testing.T) {
		if err := q.ConfigureProjects(map[string]ProjectPolicy{project: {Weight: 1}}); err == nil {
			t.Fatal("slash project accepted")
		}
	})
	t.Run("priority policy", func(t *testing.T) {
		p := defaultPriorityPolicy()
		p.Projects = map[string]ProjectPolicy{project: {Weight: 1}}
		if _, err := q.SetPriorityPolicy(0, p); err == nil {
			t.Fatal("slash project accepted")
		}
	})
	if _, err := q.Snapshot(); err != nil {
		t.Fatal(err)
	}
}

func TestPriorityPolicyChangeReclaimsAssignedAttemptBeforeStarted(t *testing.T) {
	q, _ := fixture(t)
	policy := PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "top", Selectors: []string{"alpha/production"}}, {Name: "rest", Selectors: []string{"*/*"}}}}
	if _, err := q.SetPriorityPolicy(0, policy); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.Project = "alpha"
	alpha := submit(t, q, s)
	assigned := check(t, q, machine("one", "fake"), nil).Job
	if assigned == nil || assigned.ID != alpha.ID || assigned.StartedAt != nil || assigned.PriorityPolicy.Revision != 1 {
		t.Fatalf("expected unstarted revision-1 assignment: %+v", assigned)
	}
	s.Project = "beta"
	waiting := submit(t, q, s)
	policy.Tiers[0].Selectors = []string{"beta/production"}
	if _, err := q.SetPriorityPolicy(1, policy); err != nil {
		t.Fatal(err)
	}
	if q.comparePriority(waiting.Spec, assigned.Spec) >= 0 {
		t.Fatal("waiting job must now outrank assigned attempt")
	}
	if err := q.Started(result(assigned, true)); err != ErrStale {
		t.Fatalf("stale lower-tier assignment started: %v", err)
	}
	started := q.s.Jobs[assigned.ID]
	if started.State != "stopping" || started.StartedAt != nil || priorityStop(started) == nil || priorityStop(started).Policy.Revision != 2 {
		t.Fatalf("assigned attempt not reclaimed with current policy: %+v", started)
	}
	if got := q.s.Jobs[waiting.ID]; got.State != "queued" {
		t.Fatalf("higher-tier job should still wait: %+v", got)
	}
	// The demand keeps the host it selected for preemption (#662), even if
	// another worker joins before exact release. Future placement on that
	// selected host must still use the new policy.
	if next := check(t, q, machine("two", "fake"), nil).Job; next != nil && next.ID == waiting.ID {
		t.Fatal("demand escaped its selected preemption host")
	}
	finishInterrupted(t, q, assigned)
	next := check(t, q, machine("one", "fake"), nil).Job
	if next == nil || next.ID != waiting.ID || next.PriorityPolicy.Revision != 2 {
		t.Fatalf("future placement did not use new policy: %+v", next)
	}
}
