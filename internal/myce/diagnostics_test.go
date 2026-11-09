package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIRejectionDiagnostics(t *testing.T) {
	q, _ := fixture(t)
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	for _, tc := range []struct{ body, token, code string }{
		{`{"private-secret":123}`, "test", "request_invalid"},
		{`{"name":`, "test", "request_invalid"},
		{`{} {}`, "test", "request_invalid"},
		{`{"name":"private-secret","project":"private-secret","priority":"private-secret","run":{"command":["private-secret"],"env":{"KEY":"private-secret"}}}`, "test", "job_priority_invalid"},
		{`{"needs":{"platform":["private-secret"]}}`, "test", "job_platform_invalid"},
		{`{"submission_key":[]}`, "test", "submission_key_invalid"},
		{`{}`, "private-secret", "authentication_required"},
	} {
		r := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
		var body struct{ Code, Error string }
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code < 400 || body.Code != tc.code || body.Error == "" {
			t.Fatalf("rejection: %d %s", w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "private-secret") || strings.Contains(logs.String(), "private-secret") {
			t.Fatal("private input leaked")
		}
		if !strings.Contains(logs.String(), "code="+tc.code) {
			t.Fatalf("missing audit for %s", tc.code)
		}
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	s := spec("experiment")
	s.Needs.MemoryGB = -1 // An omitted figure is learned (#585); a negative one is refused.
	err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/jobs", s, nil)
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Code != "job_memory_invalid" || typed.Message != "declare positive memory_gb (host RAM for CPU work, GPU memory otherwise)" {
		t.Fatalf("lost client diagnostic: %v", err)
	}
}

func TestNativeExecutionCauseSurvivesResultAndAPI(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.state.Session = w.Machine.Session
	j := recoveryJob(t, []string{"sh", "-c", "echo private-command-text >&2; exit 7"})
	w = workerWithPreparedCheckpoint(t, w, s, j)
	p, err := startAfterFixtureInputs(t, w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	var r Result
	select {
	case f := <-p.done:
		if f.err != nil {
			t.Fatal(f.err)
		}
		r = f.result
	case <-time.After(testWaitTimeout):
		p.stop(w.Clock.Now(), 0, true)
		t.Fatal("command did not finish")
	}
	if r.ExecutionCause == nil || r.ExecutionCause.Operation != "command" || r.ExecutionCause.Class != "command_exit" || r.ExecutionCause.ExitCode == nil || *r.ExecutionCause.ExitCode != 7 {
		t.Fatalf("lost native exit cause: %+v", r)
	}
	q, _ := fixture(t)
	q.s.Jobs[j.ID] = &j
	j.Machine = w.Machine.ID
	j.Session = s.state.Session
	j.State = "running"
	check(t, q, w.Machine, &j)
	if err = q.Complete(r); err != nil {
		t.Fatal(err)
	}
	snapshot, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(snapshot)
	if !strings.Contains(string(data), `"exit_code":7`) || strings.Contains(string(data), "private-command-text") {
		t.Fatal("API discarded native cause or exposed command text")
	}
}
