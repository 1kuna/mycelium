package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJobSpecInvalidHTTPAndQueueLog(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 1
	s.Run.Docker, s.Run.DockerNetwork, s.Run.DockerMemoryGB = true, "private", 2
	s.Run.Env = map[string]string{"PRIVATE": "env-secret"}
	const reason = "main Docker memory must fit needs.host_memory_gb"
	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(old)
	// A direct queue rejection must log too, without depending on HTTP.
	_, err := q.Submit(s)
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Code != "job_spec_invalid" || typed.Message != reason {
		t.Errorf("queue error: %v", err)
	}
	if !strings.Contains(logs.String(), reason) || strings.Contains(logs.String(), "env-secret") {
		t.Errorf("queue log missing safe reason: %q", logs.String())
	}
	b, _ := json.Marshal(s)
	r := httptest.NewRequest("POST", "/api/jobs", bytes.NewReader(b))
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
	var body struct{ Code, Error string }
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusBadRequest || body.Code != "job_spec_invalid" || body.Error != reason {
		t.Fatalf("response: %d %s", w.Code, w.Body.String())
	}
	validateWireValue(t, compileWireSchema(t, "error-v1.schema.json"), map[string]string{"code": body.Code, "error": body.Error})
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	err = (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/jobs", s, nil)
	if !errors.As(err, &typed) || typed.Code != "job_spec_invalid" || typed.Message != reason {
		t.Fatalf("client lost reason: %v", err)
	}
}

func TestJobSpecInternalErrorStaysGeneric(t *testing.T) {
	for _, err := range []error{errors.New("private IO failure"), &HTTPError{Status: 500, Code: "internal", Message: "private state failure"}} {
		w := httptest.NewRecorder()
		respond(w, nil, err)
		var body struct{ Code, Error string }
		if e := json.Unmarshal(w.Body.Bytes(), &body); e != nil {
			t.Fatal(e)
		}
		if body.Error != "request failed" || body.Code != "request_failed" {
			t.Fatal(w.Body.String())
		}
	}
}
