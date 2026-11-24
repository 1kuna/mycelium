package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func keyedSpec() Spec {
	s := spec("production")
	s.SubmissionKey = "stable-key"
	s.Run.Env = map[string]string{"a": "one", "b": "two"}
	s.AllowedMachines = []string{"one", "two"}
	return s
}

func submissionError(t *testing.T, err error, status int, code string) {
	t.Helper()
	var e *HTTPError
	if !errors.As(err, &e) || e.Status != status || e.Code != code {
		t.Fatalf("error = %v, want %d %s", err, status, code)
	}
}

// S03 will create reservations through cancel-by-key, under this same update lock.
func reserveSubmission(t *testing.T, q *Queue, project, key string) {
	t.Helper()
	if err := q.update(func() error {
		if q.s.Submissions == nil {
			q.s.Submissions = map[string]*submissionRecord{}
		}
		created := q.clock.Now()
		q.putSubmission(submissionDigest(project, key), &submissionRecord{Kind: submissionReservation, Project: project, Key: key, Created: &created})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSubmissionPersistenceAndRollback(t *testing.T) {
	q, c := fixture(t)
	s := keyedSpec()
	first := submit(t, q, s)
	var path string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := submit(t, reopened, s); got.ID != first.ID {
		t.Fatal("restart created duplicate")
	}
	// A SQLite failure must roll back both the job and its key.
	if _, err := reopened.db.Exec(`CREATE TRIGGER reject_queue BEFORE INSERT ON queue_state BEGIN SELECT RAISE(ABORT,'test write failure'); END`); err != nil {
		t.Fatal(err)
	}
	s.SubmissionKey = "write-failure"
	if _, err := reopened.Submit(s); err == nil {
		t.Fatal("failed write accepted")
	}
	if len(reopened.s.Jobs) != 1 || reopened.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)] != nil {
		t.Fatal("failed write retained identity")
	}
	if _, err := reopened.db.Exec(`DROP TRIGGER reject_queue`); err != nil {
		t.Fatal(err)
	}
	submit(t, reopened, s)
}

func TestSubmissionHTTP(t *testing.T) {
	q, c := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test-only"}
	ctx := context.Background()
	s := keyedSpec()
	var first, again Job
	if err := client.Call(ctx, "POST", "/api/jobs", s, &first); err != nil {
		t.Fatal(err)
	}
	if err := client.Call(ctx, "POST", "/api/jobs", s, &again); err != nil {
		t.Fatal(err)
	}
	if first.ID != again.ID {
		t.Fatal("HTTP repeat duplicated")
	}
	// Reordering HTTP object members (including nested env members) is identical.
	raw := `{"submission_key":"http-order","name":"ordered","project":"test","priority":"experiment","run":{"command":["echo"],"env":{"a":"1","b":"2"}},"needs":{"memory_gb":1,"platform":{"any":true}}}`
	reordered := `{"needs":{"platform":{"any":true},"memory_gb":1},"run":{"env":{"b":"2","a":"1"},"command":["echo"]},"priority":"experiment","project":"test","name":"ordered","submission_key":"http-order"}`
	h := API{Queue: q, Token: "test-only"}.Handler()
	id := ""
	for _, body := range []string{raw, reordered} {
		r := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer test-only")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("ordered POST: %d %s", w.Code, w.Body.String())
		}
		var j Job
		if err := json.Unmarshal(w.Body.Bytes(), &j); err != nil {
			t.Fatal(err)
		}
		if id != "" && id != j.ID {
			t.Fatal("object order changed identity")
		}
		id = j.ID
	}
	s.Name = "different"
	submissionError(t, client.Call(ctx, "POST", "/api/jobs", s, nil), http.StatusConflict, "submission_key_conflict")
	s.SubmissionKey = "reserved"
	reserveSubmission(t, q, s.Project, s.SubmissionKey)
	c.Advance(submissionRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	var reserved Job
	if err := client.Call(ctx, "POST", "/api/jobs", s, &reserved); err != nil || reserved.State != "cancelled" {
		t.Fatal("age lost cancelled intent", err)
	}
}

func TestSubmissionLegacyState(t *testing.T) {
	data := []byte("unmarked queue data")
	path := filepath.Join(t.TempDir(), "queue.db")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	c := &fakeClock{now: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)}
	q, err := Open(path, t.TempDir(), c)
	if q != nil {
		q.Close()
	}
	if !errors.Is(err, ErrLegacyState) {
		t.Fatal("old submission state accepted", err)
	}
	after, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(data, after) {
		t.Fatal("refusal mutated old submission state", e)
	}
}

func TestSubmissionPastStartByPreservesReplayAndCancellation(t *testing.T) {
	q, clock := fixture(t)
	s := spec("production")
	s.SubmissionKey = "deadline-replay"
	deadline := clock.Now().Add(time.Second)
	s.StartBy = &deadline
	original := submit(t, q, s)
	clock.Advance(107 * time.Second)
	if replay := submit(t, q, s); replay.ID != original.ID {
		t.Fatal("expired explicit deadline changed keyed replay")
	}
	s.SubmissionKey = "new-deadline"
	_, err := q.Submit(s)
	submissionError(t, err, 400, "job_deadline_invalid")
	reserveSubmission(t, q, s.Project, s.SubmissionKey)
	if cancelled := submit(t, q, s); cancelled.State != "cancelled" {
		t.Fatalf("past deadline overrode cancellation: %+v", cancelled)
	}
}
