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
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
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

func TestSubmissionIdentity(t *testing.T) {
	q, c := fixture(t)
	s := keyedSpec()
	first := submit(t, q, s)
	if first.Spec.SubmissionKey != s.SubmissionKey {
		t.Fatal("key lost")
	}
	c.Advance(time.Second)
	// Map insertion order is not part of identity; the generated start_by is not submitted.
	s.Run.Env = map[string]string{"b": "two", "a": "one"}
	again := submit(t, q, s)
	if again.ID != first.ID || !again.Queued.Equal(first.Queued) || !again.Spec.StartBy.Equal(*first.Spec.StartBy) {
		t.Fatal("repeat changed identity or timestamps")
	}
	for _, mutate := range []func(*Spec){
		func(s *Spec) { s.Run.Env = map[string]string{"a": "changed"} },
		func(s *Spec) { s.Run.Command = []string{"done", "echo"} },
		func(s *Spec) { s.AllowedMachines = []string{"two", "one"} },
		func(s *Spec) { v := c.Now().Add(time.Hour); s.StartBy = &v },
	} {
		changed := s
		mutate(&changed)
		_, err := q.Submit(changed)
		submissionError(t, err, 409, "submission_key_conflict")
	}
	for _, state := range []string{"succeeded", "failed", "cancelled"} {
		if err := q.update(func() error { q.s.Jobs[first.ID].State = state; return nil }); err != nil {
			t.Fatal(err)
		}
		if got := submit(t, q, s); got.ID != first.ID || got.State != state {
			t.Fatalf("terminal repeat = %+v", got)
		}
	}
	s.Project = "other"
	if got := submit(t, q, s); got.ID == first.ID {
		t.Fatal("projects share key namespace")
	}
	s.SubmissionKey = ""
	if a, b := submit(t, q, s), submit(t, q, s); a.ID == b.ID {
		t.Fatal("unkeyed deduplicated")
	}
}

func TestSubmissionReservationPrecedence(t *testing.T) {
	q, c := fixture(t)
	s := keyedSpec()
	reserveSubmission(t, q, s.Project, s.SubmissionKey)
	created := c.Now()
	c.Advance(time.Hour)
	// No placement, environment, extension or executable validation may turn this into runnable work.
	s.Environment = Environment{Name: "missing"}
	s.Run.Command = nil
	got := submit(t, q, s)
	if got.ID == "" || got.State != "cancelled" || got.Attempt != 0 || got.Machine != "" || got.FinishedAt == nil || !got.FinishedAt.Equal(created) {
		t.Fatalf("reservation = %+v", got)
	}
	s = keyedSpec()
	s.Name = "different"
	if repeat := submit(t, q, s); repeat.ID != got.ID || repeat.State != "cancelled" {
		t.Fatal("reservation precedence lost after binding")
	}
	if r := check(t, q, machine("one", "fake"), nil); r.Job != nil {
		t.Fatal("reserved job assigned")
	}
}

func TestSubmissionRetention(t *testing.T) {
	for _, reservation := range []bool{false, true} {
		t.Run(map[bool]string{false: "binding", true: "reservation"}[reservation], func(t *testing.T) {
			q, c := fixture(t)
			s := keyedSpec()
			digest := submissionDigest(s.Project, s.SubmissionKey)
			var original *Job
			if reservation {
				reserveSubmission(t, q, s.Project, s.SubmissionKey)
			} else {
				original = submit(t, q, s)
				if err := q.Cancel(original.ID); err != nil {
					t.Fatal(err)
				}
				c.Advance(2 * submissionRetention)
				if err := q.Tick(); err != nil {
					t.Fatal(err)
				}
				if got := submit(t, q, s); got.ID != original.ID {
					t.Fatal("live job binding expired")
				}
				// There is no product job-removal path. Model future removal under the update lock.
				if err := q.update(func() error { q.deleteJob(original.ID); return nil }); err != nil {
					t.Fatal(err)
				}
			}
			c.Advance(submissionRetention - time.Nanosecond)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if q.s.Submissions[digest].Kind == submissionExpired {
				t.Fatal("expired early")
			}
			if !reservation {
				if got := submit(t, q, s); got.ID != original.ID || got.State != "cancelled" {
					t.Fatal("removed job answer lost")
				}
			}
			c.Advance(time.Nanosecond)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(q.s.Submissions[digest])
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != `{"kind":"expired"}` {
				t.Fatalf("marker retains full identity: %s", data)
			}
			_, err = q.Submit(s)
			submissionError(t, err, 410, "submission_key_expired")
			c.Advance(100 * submissionRetention)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			_, err = q.Submit(s)
			submissionError(t, err, 410, "submission_key_expired")
			if len(q.s.Jobs) != 0 {
				t.Fatal("expired key created job")
			}
		})
	}
}

func TestSubmissionConcurrent(t *testing.T) {
	q, _ := fixture(t)
	s := keyedSpec()
	const n = 16
	ids := make(chan string, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := q.Submit(s)
			if err != nil {
				errs <- err
				return
			}
			ids <- j.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	id := ""
	for got := range ids {
		if id != "" && got != id {
			t.Fatal("duplicate jobs")
		}
		id = got
	}
	if len(q.s.Jobs) != 1 || len(q.s.Submissions) != 1 {
		t.Fatal("non-atomic identity")
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

func TestSubmissionCanonicalAndMutableState(t *testing.T) {
	q, c := fixture(t)
	s := keyedSpec()
	data, err := yaml.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Spec
	if err := yaml.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	first := submit(t, q, decoded)
	if first.Spec.SubmissionKey != s.SubmissionKey {
		t.Fatal("YAML key lost")
	}
	// Previously accepted specs must replay before current placement/catalog checks.
	if err := q.update(func() error {
		q.s.Policies = map[string]ProjectPolicy{s.Project: {AllowedMachines: []string{"different"}}}
		q.s.Jobs[first.ID].Spec.Needs.MemoryGB = 64 // Resolved/learned value, not submitted identity.
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := submit(t, q, s); got.ID != first.ID || got.Spec.Needs.MemoryGB != 64 {
		t.Fatal("mutated job used for fingerprint")
	}
	// Explicit deadlines and array order remain part of the submitted identity.
	s.SubmissionKey = "explicit"
	s.AllowedMachines = nil
	deadline := c.Now().Add(time.Hour)
	s.StartBy = &deadline
	explicit := submit(t, q, s)
	if repeat := submit(t, q, s); repeat.ID != explicit.ID {
		t.Fatal("explicit deadline repeat")
	}
	changed := s
	changed.StartBy = nil
	_, err = q.Submit(changed)
	submissionError(t, err, 409, "submission_key_conflict")
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
	submissionError(t, client.Call(ctx, "POST", "/api/jobs", s, nil), http.StatusGone, "submission_key_expired")
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

func TestSubmissionReservationAndMarkerRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	root := t.TempDir()
	c := &fakeClock{now: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	s := keyedSpec()
	reserveSubmission(t, q, s.Project, s.SubmissionKey)
	reserveSubmission(t, q, s.Project, "expired-key")
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	bound := submit(t, q, s)
	if bound.State != "cancelled" {
		t.Fatal("reservation lost on restart")
	}
	c.Advance(submissionRetention)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	s.Name = "changed"
	if repeat := submit(t, q, s); repeat.ID != bound.ID || repeat.State != "cancelled" {
		t.Fatal("bound reservation precedence lost on restart")
	}
	s.SubmissionKey = "expired-key"
	_, err = q.Submit(s)
	submissionError(t, err, 410, "submission_key_expired")
	if len(q.s.Jobs) != 1 {
		t.Fatal("marker created a job after restart")
	}
}
