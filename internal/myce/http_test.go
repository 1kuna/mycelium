package myce

import (
	"context"
	"net/http"
	"net/http/httptest"
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
