package myce

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func examplePriorityPolicy() PriorityPolicy {
	return PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "urgent", Selectors: []string{"verso/production"}}, {Name: "shared", Selectors: []string{"atlas/*", "verso/experiment"}}, {Name: "other", Selectors: []string{"*/*"}}}}
}

func TestPriorityPolicyDefaultAndSelectors(t *testing.T) {
	q, _ := fixture(t)
	for _, tc := range []struct{ project, class, tier, selector string }{{"a", "production", "production", "*/production"}, {"a", "deadline", "deadline", "*/deadline"}, {"a", "experiment", "experiment", "*/*"}, {"a", "preview", "experiment", "*/*"}} {
		got := q.priorityMatch(Spec{Project: tc.project, Class: tc.class})
		if got.Tier != tc.tier || got.Selector != tc.selector || got.Revision != 0 {
			t.Fatalf("default: %+v", got)
		}
	}
	p := examplePriorityPolicy()
	p.Tiers[0].Selectors = append(p.Tiers[0].Selectors, "v[ae]r?o/prod*")
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ project, class, tier string }{{"verso", "production", "urgent"}, {"varso", "production", "urgent"}, {"atlas", "production", "shared"}, {"verso", "experiment", "shared"}, {"third", "training", "other"}} {
		if got := q.priorityMatch(Spec{Project: tc.project, Class: tc.class}); got.Tier != tc.tier || got.Revision != 1 {
			t.Fatalf("%+v: %+v", tc, got)
		}
	}
}

func TestPriorityPolicyCASReopenReplay(t *testing.T) {
	q, c := fixture(t)
	s := spec("production")
	s.Project = "verso"
	s.SubmissionKey = "stable"
	j := submit(t, q, s)
	var wg sync.WaitGroup
	successes, conflicts := make(chan bool, 2), make(chan bool, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := q.SetPriorityPolicy(0, examplePriorityPolicy())
			if err == nil {
				successes <- true
			} else if errors.Is(err, ErrPolicyRevision) {
				conflicts <- true
			} else {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(successes) != 1 || len(conflicts) != 1 {
		t.Fatal("CAS did not serialize")
	}
	replay := submit(t, q, s)
	if replay.ID != j.ID || replay.Spec.Class != "production" {
		t.Fatalf("replay: %+v", replay)
	}
	var dbPath, dbName string
	var dbSeq int
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&dbSeq, &dbName, &dbPath); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbPath, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record := reopened.PriorityPolicy()
	if record.Revision != 1 || record.Policy.Tiers[0].Name != "urgent" {
		t.Fatalf("reopen: %+v", record)
	}
	if got := submit(t, reopened, s); got.ID != j.ID {
		t.Fatal("key replay changed after reopen")
	}
}

func TestPriorityPolicyPlacement(t *testing.T) {
	q, c := fixture(t)
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	atlas := spec("production")
	atlas.Project = "atlas"
	a := submit(t, q, atlas)
	c.Advance(1)
	experiment := spec("experiment")
	experiment.Project = "verso"
	e := submit(t, q, experiment)
	production := spec("production")
	production.Project = "verso"
	p := submit(t, q, production)
	m1, m2 := machine("one", "fake"), machine("two", "fake")
	first := check(t, q, m1, nil).Job
	if first == nil || first.ID != p.ID {
		t.Fatalf("urgent did not win: %+v", first)
	}
	finish(t, q, first, true)
	left := check(t, q, m1, nil).Job
	right := check(t, q, m2, nil).Job
	if left == nil || right == nil || left.ID == right.ID || (left.ID != a.ID && left.ID != e.ID) || (right.ID != a.ID && right.ID != e.ID) {
		t.Fatalf("shared placement: %+v %+v", left, right)
	}
	if job(t, q, a.ID).State != "running" || job(t, q, e.ID).State != "running" {
		t.Fatal("same tier stopped a neighbor")
	}
	if q.comparePriority(a.Spec, e.Spec) != 0 {
		t.Fatal("shared classes differ")
	}
	status, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range status.Jobs {
		if j.PriorityPolicy.Revision != 1 || j.PriorityPolicy.Selector == "" {
			t.Fatalf("missing policy status: %+v", j)
		}
	}
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

func TestPriorityPolicyReplacesPreemptionRank(t *testing.T) {
	q, _ := fixture(t)
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	s := spec("production")
	s.Project = "atlas"
	a := submit(t, q, s)
	m := machine("one", "fake")
	running := check(t, q, m, nil).Job
	if running == nil || running.ID != a.ID {
		t.Fatal("atlas not assigned")
	}
	s.Project = "verso"
	p := submit(t, q, s)
	if job(t, q, a.ID).State != "stopping" {
		t.Fatal("legacy equal priority overrode tier preemption")
	}
	if job(t, q, p.ID).PriorityPolicy.Tier != "urgent" {
		t.Fatal("wrong tier")
	}
	// A live policy update changes current rank without rewriting the immutable class.
	policy := examplePriorityPolicy()
	policy.Tiers[0].Selectors = []string{"atlas/*"}
	if _, err := q.SetPriorityPolicy(1, policy); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, a.ID); got.Spec.Class != "production" || got.PriorityPolicy.Tier != "urgent" || got.PriorityPolicy.Revision != 2 {
		t.Fatalf("stale projection: %+v", got.PriorityPolicy)
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
	for _, url := range []string{"/api/status", "/"} {
		r := httptest.NewRequest("GET", url, nil)
		r.Header.Set("Authorization", "Bearer owner-test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("status %s: %d", url, w.Code)
		}
		if url == "/api/status" {
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

func TestPriorityPolicyModelResidentPromotion(t *testing.T) {
	q, _, m := modelFixture(t)
	low := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	high := addModelCall(t, q, "production")
	resident := q.s.ModelSlots[slotKey(m.ID, "test")]
	if low.Machine != m.ID || high.Machine != m.ID || resident.Priority != "production" {
		t.Fatalf("resident priority was not promoted: %+v", resident)
	}
	if err := q.finishCall(high.ID); err != nil {
		t.Fatal(err)
	}
	if resident.Priority != "experiment" || q.comparePriority(q.slotSpec(resident), callJob(low).Spec) != 0 {
		t.Fatalf("resident priority was not restored: %+v", resident)
	}
}

func TestPriorityPolicyCASIncludesExistingProjectSettings(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 2}}); err != nil {
		t.Fatal(err)
	}
	record := q.PriorityPolicy()
	if record.Revision != 1 || record.Policy.Projects["atlas"].Weight != 2 {
		t.Fatalf("settings did not advance policy revision: %+v", record)
	}
	if _, err := q.SetPriorityPolicy(0, examplePriorityPolicy()); !errors.Is(err, ErrPolicyRevision) {
		t.Fatalf("stale settings snapshot accepted: %v", err)
	}
	if _, err := q.SetPriorityPolicy(1, examplePriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	if q.PriorityPolicy().Policy.Projects["atlas"].Weight != 2 {
		t.Fatal("omitted projects lost settings")
	}
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"atlas": {Weight: 3}}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.SetPriorityPolicy(2, examplePriorityPolicy()); !errors.Is(err, ErrPolicyRevision) {
		t.Fatalf("legacy settings write bypassed CAS: %v", err)
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

func TestPriorityPolicyChangePreservesAssignedAttempt(t *testing.T) {
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
	if err := q.Started(result(assigned, true)); err != nil {
		t.Fatalf("policy change rejected assigned attempt: %v", err)
	}
	started := q.s.Jobs[assigned.ID]
	if started.State != "running" || started.StartedAt == nil || started.Attempt != assigned.Attempt || started.Session != assigned.Session || started.Machine != assigned.Machine {
		t.Fatalf("assigned attempt did not start: %+v", started)
	}
	if got := q.s.Jobs[waiting.ID]; got.State != "queued" {
		t.Fatalf("higher-tier job should still wait: %+v", got)
	}
	// Future placement uses the new policy.
	next := check(t, q, machine("two", "fake"), nil).Job
	if next == nil || next.ID != waiting.ID || next.PriorityPolicy.Revision != 2 {
		t.Fatalf("future placement did not use new policy: %+v", next)
	}
}
