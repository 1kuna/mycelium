package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestAPIAuthenticationAndFlow(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "secret"}.Handler())
	defer server.Close()
	ctx := context.Background()
	c := Client{URL: server.URL, Token: "wrong"}
	if err := c.Call(ctx, "POST", "/api/workers/checkin", Checkin{Machine: machine("one", "fake")}, nil); err == nil {
		t.Fatal("unauthorized worker accepted")
	}
	c.Token = "secret"
	var j Job
	s := spec("experiment")
	s.Name = "<script>alert(1)</script>"
	if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	var r Reply
	if err := c.Call(ctx, "POST", "/api/workers/checkin", Checkin{Machine: machine("one", "fake")}, &r); err != nil {
		t.Fatal(err)
	}
	if r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("pull failed")
	}
	if err := c.Call(ctx, "POST", "/api/workers/log", LogChunk{Result: result(r.Job, true), Text: "line\n"}, nil); err != nil {
		t.Fatal(err)
	}
	var logs []Log
	if err := c.Call(ctx, "GET", "/api/jobs/"+j.ID+"/logs", nil, &logs); err != nil || len(logs) != 1 {
		t.Fatalf("logs %v %v", logs, err)
	}
	if err := c.Call(ctx, "POST", "/api/jobs/"+j.ID+"/cancel", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Call(ctx, "POST", "/api/workers/complete", result(r.Job, false), nil); err != nil {
		t.Fatal(err)
	}
	var snap Snapshot
	if err := c.Call(ctx, "GET", "/api/status", nil, &snap); err != nil || snap.Jobs[0].State != "cancelled" {
		t.Fatalf("snapshot %v %v", snap, err)
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	API{Queue: q, Token: "secret"}.Handler().ServeHTTP(rr, req)
	if strings.Contains(rr.Body.String(), "<script>") || !strings.Contains(rr.Body.String(), "&lt;script&gt;") {
		t.Fatal("status HTML must escape job content")
	}
}
func TestAPIRejectsMalformedRequests(t *testing.T) {
	q, _ := fixture(t)
	h := API{Queue: q, Token: "secret"}.Handler()
	for _, body := range []string{`{"unknown":1}`, `{} {}`, strings.Repeat("x", (1<<20)+1)} {
		r := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status %d", w.Code)
		}
	}
	w := httptest.NewRecorder()
	API{Queue: q}.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/api/status", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatal("empty token must fail closed")
	}
}

func TestAPIJobReplyRedactsExecutionPayload(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "projection-test"
	s.Logs = "keep"
	s.Run.Command = []string{"echo", "private-command"}
	s.Run.Env = map[string]string{"PRIVATE": "private-env", "MYCE_TOKEN": "private-token"}
	_, ref := retentionUpload(t, retentionClient(t, q), "submitted input")
	s.Files = FileManifest{"private-input.txt": ref}
	j := submit(t, q, s)
	if err := q.update(func() error {
		stored := q.s.Jobs[j.ID]
		stored.Spec.Args = []string{"private-args"}
		stored.Spec.Environment = Environment{LockFile: "/private/recipe.lock"}
		stored.EnvironmentRecipes = map[string]EnvironmentRecipe{"fake": {LockFile: "/private/recipe.lock"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	// Metadata does not bypass the existing fleet-token read authorization.
	for _, token := range []string{"", "wrong"} {
		r := httptest.NewRequest("GET", "/api/jobs/"+j.ID, nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized || strings.Contains(w.Body.String(), "spec_projection") {
			t.Errorf("unauthorized GET: status=%d body=%s", w.Code, w.Body.String())
		}
	}
	var reply map[string]json.RawMessage
	c := Client{URL: server.URL, Token: "test"}
	if err := c.Call(context.Background(), "GET", "/api/jobs/"+j.ID, nil, &reply); err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, compileWireSchema(t, "job-v1.schema.json"), reply)
	var projection SpecProjection
	if err := json.Unmarshal(reply["spec_projection"], &projection); err != nil {
		t.Fatal(err)
	}
	if !projection.Projected {
		t.Error("GET must explicitly mark spec as projected")
	}
	for _, field := range []string{"files", "run", "args", "environment"} {
		if !slices.Contains(projection.Omitted, field) {
			t.Errorf("GET did not declare omitted %s", field)
		}
	}
	var specView map[string]json.RawMessage
	if err := json.Unmarshal(reply["spec"], &specView); err != nil {
		t.Fatal(err)
	}
	if string(specView["submission_key"]) != `"projection-test"` || slices.Contains(projection.Omitted, "submission_key") {
		t.Error("GET must retain submission_key without declaring it omitted")
	}
	if string(specView["logs"]) != `"keep"` || slices.Contains(projection.Omitted, "logs") {
		t.Error("GET must retain logs policy without declaring it omitted")
	}
	var run map[string]json.RawMessage
	if raw, present := specView["run"]; present {
		if err := json.Unmarshal(raw, &run); err != nil {
			t.Fatal(err)
		}
	}
	for _, field := range []string{"command", "env"} {
		if _, exposed := run[field]; exposed {
			t.Errorf("GET exposed run.%s", field)
		}
	}
	if _, exposed := specView["files"]; exposed {
		t.Error("GET exposed submitted file references")
	}
	var status Snapshot
	if err := c.Call(context.Background(), "GET", "/api/status", nil, &status); err != nil {
		t.Fatal(err)
	}
	if len(status.Jobs) != 1 || status.Jobs[0].SpecProjection == nil {
		t.Fatal("status must declare its spec projection")
	}
	if !slices.Contains(status.Jobs[0].SpecProjection.Omitted, "submission_key") || status.Jobs[0].Spec.SubmissionKey != "" {
		t.Error("status must declare submission_key omitted")
	}
	if status.Jobs[0].Spec.Logs != "keep" || slices.Contains(status.Jobs[0].SpecProjection.Omitted, "logs") {
		t.Error("status must retain logs policy without declaring it omitted")
	}
	q.mu.Lock()
	stored := q.s.Jobs[j.ID]
	unchanged := stored.Spec.Files["private-input.txt"] == ref && stored.Spec.Run.Env["MYCE_TOKEN"] == "private-token"
	q.mu.Unlock()
	if !unchanged {
		t.Fatal("projection changed submitted payload")
	}
	if _, exposed := specView["args"]; exposed {
		t.Error("GET exposed args")
	}
	if _, exposed := reply["environment_recipes"]; exposed {
		t.Error("GET exposed environment recipe")
	}
	body, _ := json.Marshal(reply)
	statusBody, _ := json.Marshal(status)
	for _, private := range []string{"private-command", "private-env", "private-token", "private-args", "/private/recipe.lock", "private-input.txt", ref.SHA256} {
		if strings.Contains(string(body), private) {
			t.Errorf("GET exposed %q", private)
		}
		if strings.Contains(string(statusBody), private) {
			t.Errorf("status exposed %q", private)
		}
	}
}
