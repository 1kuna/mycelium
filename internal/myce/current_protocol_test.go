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
)

func TestCurrentProtocolRejectsMissingIdentity(t *testing.T) {
	q, _ := fixture(t)
	for _, path := range []string{"/api/workers/checkin", "/api/workers/launch", "/api/workers/started", "/api/workers/complete", "/api/workers/log", "/api/workers/environment-log", "/api/workers/one/leave", "/api/files/checkpoint", "/api/workers/one/events"} {
		for _, protocol := range []string{"", "durable-start-v1", "host-attempt-v3"} {
			method := "POST"
			if strings.HasSuffix(path, "/events") {
				method = "GET"
			}
			r := httptest.NewRequest(method, path, strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer test")
			r.Header.Set("X-Myce-Start-Gate", protocol)
			w := httptest.NewRecorder()
			API{q, "test"}.Handler().ServeHTTP(w, r)
			if w.Code != http.StatusUpgradeRequired {
				t.Fatalf("%s: %d", path, w.Code)
			}
		}
	}
	for _, edit := range []func(*Checkin){func(c *Checkin) { c.Machine.Session = "" }, func(c *Checkin) { c.Machine.CapabilityProfile = nil }, func(c *Checkin) { c.Machine.ApprovedModels = map[string]string{"source": "hash"} }, func(c *Checkin) { c.ModelReports = []ModelReport{{Name: "model", State: "unloaded"}} }} {
		c := Checkin{Machine: machine("current", "fake")}
		edit(&c)
		if _, err := q.Checkin(c); err == nil {
			t.Fatal("accepted absent identity/profile/approval")
		}
	}
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	badCheckin := Checkin{Machine: machine("current", "fake")}
	badCheckin.Machine.CapabilityProfile = nil
	var reply Reply
	err := client.Call(context.Background(), "POST", "/api/workers/checkin", badCheckin, &reply)
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Status != 426 || refusal.Code != "worker_protocol_invalid" || !strings.Contains(refusal.Message, "capability profile") {
		t.Fatalf("unhelpful protocol refusal: %v", err)
	}
	for _, gpu := range []string{"cuda", "xpu", "metal", "fake"} {
		for _, layout := range []string{"separate", "unified"} {
			m := machine("current-"+gpu+layout, gpu)
			m.CapabilityProfile.MemoryLayout = layout
			if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Protocol validation is independent of which equal-value peer pulls first.
	wanted := spec("experiment")
	wanted.AllowedMachines = []string{"worker"}
	s := submit(t, q, wanted)
	a := check(t, q, machine("worker", "fake"), nil).Job
	if a == nil || a.ID != s.ID {
		t.Fatal("assignment")
	}
	before := cloneJob(q.s.Jobs[a.ID])
	bad := result(a, true)
	bad.Session = ""
	if err := q.Complete(bad); err == nil {
		t.Fatal("empty session discharged custody")
	}
	if q.s.Jobs[a.ID].ReleaseConfirmed != before.ReleaseConfirmed {
		t.Fatal("custody changed")
	}
}

func TestCurrentProtocolUniversalPrelaunch(t *testing.T) {
	for _, priority := range []string{"experiment", "deadline", "production"} {
		t.Run(priority, func(t *testing.T) {
			q, _ := fixture(t)
			submit(t, q, spec(priority))
			a := check(t, q, machine("worker", "fake"), nil).Job
			if a == nil {
				t.Fatal("assignment")
			}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; API{q, "test"}.Handler().ServeHTTP(w, r) }))
			defer server.Close()
			worker := Worker{Client: Client{URL: server.URL, Token: "test"}, Machine: machine("worker", "fake")}
			if err := worker.authorizeStart(*a, a.Session); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || job(t, q, a.ID).StartedAt == nil {
				t.Fatal("missing durable start")
			}
			if err := q.Cancel(a.ID); err != nil {
				t.Fatal(err)
			}
			if err := worker.authorizeStart(*a, a.Session); err == nil {
				t.Fatal("cancelled launch authorized")
			}
		})
	}
}

func TestCurrentProtocolServerCompatibility(t *testing.T) {
	q, _ := fixture(t)
	m := machine("server", "fake")
	m.ApprovedModels = map[string]string{"source": "hash"}
	model := Model{Name: "entry", LocalSource: "source", SHA256: "hash", AllowedMachines: []string{"server"}}
	if modelFenceAllows(model, &m) {
		t.Fatal("implicit local-source approval")
	}
	m.ModelApprovalStates = map[string]ModelApprovalState{"source": approvalVerified}
	if !modelFenceAllows(model, &m) {
		t.Fatal("explicit local-source approval refused")
	}
	if (&ModelPlacement{AllowedMachines: []string{"server"}, ProjectMachines: []string{"caller"}}).allows("server") {
		t.Fatal("owner/caller intersection")
	}
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	var out json.RawMessage
	if err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/workers/start-permit", Result{}, &out); err == nil {
		t.Fatal("retired permit route")
	}
}

func TestCurrentProtocolBatchRefusalPreventsExecution(t *testing.T) {
	worker := recoveryWorker(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUpgradeRequired) }))
	defer server.Close()
	worker.Client = Client{URL: server.URL}
	store, err := worker.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = worker.Machine.Session
	j := recoveryJob(t, []string{"sh", "-c", "printf unsafe > {work}/payload"})
	marker := filepath.Join(store.dir, "job-files", j.ID, "1", "payload")
	j.Spec.Resumable = false
	if _, err = worker.start(store, j); err == nil {
		t.Fatal("refused batch executed")
	}
	if _, err = os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("payload ran before authorization")
	}
}

func TestCurrentProtocolCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service-secret")
	if err := os.WriteFile(path, []byte(`{"token":"synthetic-secret"}`), 0600); err != nil {
		t.Fatal(err)
	}
	c := ServiceConfig{Credentials: path}
	if _, err := ReadServiceToken(c); err == nil {
		t.Fatal("old JSON credential store was used at runtime")
	}
	if err := os.WriteFile(path, []byte("MYCE_TOKEN=synthetic-secret\nMYCE_QUEUE=http://127.0.0.1:1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if token, err := ReadServiceToken(c); err != nil || token != "synthetic-secret" {
		t.Fatal("literal credential mechanism refused")
	}
}

func TestCurrentProtocolCheckpointCannotPersistWithoutHeader(t *testing.T) {
	q, _ := fixture(t)
	if e := q.ConfigureFileStore(t.TempDir(), 1<<20, true); e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	j := submit(t, q, spec("experiment"))
	m := machine("checkpoint", "fake")
	a := check(t, q, m, nil).Job
	if a == nil {
		t.Fatal("assignment")
	}
	src := filepath.Join(t.TempDir(), "save.json")
	if e := os.WriteFile(src, []byte(`{"myce_checkpoint":{"generation":7},"value":5}`), 0600); e != nil {
		t.Fatal(e)
	}
	ref, e := IdentifyFile(src)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.UploadFile(context.Background(), src, ref); e != nil {
		t.Fatal(e)
	}
	save := checkpointPublication{Result: Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: a.Attempt}}, Checkpoint: FileCheckpoint{7, FileManifest{"checkpoint.json": ref}}}
	b, _ := json.Marshal(save)
	for _, header := range []string{"", "durable-start-v1", "host-attempt-v3"} {
		r := httptest.NewRequest("POST", "/api/files/checkpoint", bytes.NewReader(b))
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("X-Myce-Start-Gate", header)
		w := httptest.NewRecorder()
		API{q, "test"}.Handler().ServeHTTP(w, r)
		if w.Code != 426 {
			t.Fatal(header, w.Code)
		}
		if len(job(t, q, j.ID).FileCheckpoints) != 0 {
			t.Fatal("old protocol persisted checkpoint")
		}
	}
	if e = c.Call(context.Background(), "POST", "/api/files/checkpoint", save, nil); e != nil {
		t.Fatal(e)
	}
	if len(job(t, q, j.ID).FileCheckpoints) != 1 {
		t.Fatal("current protocol did not persist checkpoint")
	}
}
