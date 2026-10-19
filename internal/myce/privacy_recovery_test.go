package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrivacySpoolStartupPermissionsAndErrors(t *testing.T) {
	q, _ := fixture(t)
	dir := filepath.Join(t.TempDir(), "uploads")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := PrepareStateRoots(dir); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(dir, "myce-model-request-orphan")
	if err := os.WriteFile(orphan, []byte("synthetic"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureUploadSpool(dir, 32); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("spool directory is not private")
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("startup retained orphan")
	}
	f, _, err := q.uploadSpool.request(context.Background(), strings.NewReader("synthetic"), 16, -1)
	if err != nil {
		t.Fatal(err)
	}
	info, err = f.Stat()
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("spool file is not private")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.uploadSpool.request(context.Background(), strings.NewReader("too long"), 2, -1); err == nil {
		t.Fatal("expected size error")
	}
	files, err := filepath.Glob(filepath.Join(dir, "myce-model-request-*"))
	if err != nil || len(files) != 0 || q.uploadSpool.used != 0 {
		t.Fatal("error retained upload or quota")
	}
	other, _ := fixture(t)
	if err := other.ConfigureUploadSpool(dir, 32); err == nil {
		t.Fatal("live spool can be claimed by another queue")
	}
}

func TestPrivacyStatusAuthentication(t *testing.T) {
	q, _ := fixture(t)
	h := API{Queue: q, Token: "test"}.Handler()
	for _, path := range []string{"/", "/api/status"} {
		for _, token := range []string{"", "wrong", "test"} {
			r := httptest.NewRequest("GET", path, nil)
			if token != "" {
				r.Header.Set("Authorization", "Bearer "+token)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			want := 401
			if token == "test" {
				want = 200
			}
			if w.Code != want {
				t.Fatalf("%s auth: got %d want %d", path, w.Code, want)
			}
		}
	}
}

func TestPrivacyControlErrorDoesNotEchoBody(t *testing.T) {
	upstream := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte(strings.Repeat("synthetic-secret", 1000)))
	}))
	defer upstream.Close()
	err := (Client{URL: upstream.URL}).Call(context.Background(), "GET", "/", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "503") || strings.Contains(err.Error(), "synthetic-secret") || len(err.Error()) > 128 {
		t.Fatal("unsafe control error")
	}
}

func privacyRequest(path string) *http.Request {
	r := httptest.NewRequest("POST", path, strings.NewReader(`{"model":"test","input":"synthetic"}`))
	r.Header.Set("Authorization", "Bearer test")
	r.Header.Set("X-Myce-Project", "test")
	r.Header.Set("X-Myce-Priority", "production")
	return r
}

func TestPrivacyUpstreamErrorsAreFixed(t *testing.T) {
	q, _, m := modelFixture(t)
	upstream := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Private", "synthetic-secret")
		w.WriteHeader(422)
		w.Write([]byte("synthetic-secret"))
	}))
	defer upstream.Close()
	m.ModelURL = upstream.URL
	check(t, q, m, nil)
	seed := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	q.finishCall(seed.ID)
	for _, path := range []string{"/models/test/ocr", "/v1/chat/completions"} {
		w := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, privacyRequest(path))
		if w.Code != 422 || strings.Contains(w.Body.String(), "synthetic-secret") || w.Header().Get("X-Private") != "" {
			t.Fatal("upstream error exposed private body or headers")
		}
	}
}

func TestPrivacyFrozenCallTimesOutAndReleasesSlot(t *testing.T) {
	for _, path := range []string{"/models/test/ocr", "/v1/chat/completions"} {
		t.Run(path, func(t *testing.T) {
			q, clock, m := modelFixture(t)
			// JSON assignment lets this regression run against the old catalog schema.
			if err := json.Unmarshal([]byte(`{"request_timeout":40000000}`), &q.models.Models[0]); err != nil {
				t.Fatal(err)
			}
			upstream := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				q.mu.Lock()
				clock.Advance(40 * time.Millisecond)
				q.mu.Unlock()
				<-r.Context().Done()
			}))
			defer upstream.Close()
			m.ModelConfigHash = modelConfigHash(q.models)
			m.ModelURL = upstream.URL
			check(t, q, m, nil)
			seed := addModelCall(t, q, "production")
			reportModel(t, q, m, "ready")
			q.finishCall(seed.ID)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			w := httptest.NewRecorder()
			API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, privacyRequest(path).WithContext(ctx))
			if w.Code != 504 || !strings.Contains(w.Body.String(), "request timeout") {
				t.Fatal("frozen response did not fail clearly within model limit")
			}
			if len(q.calls) != 0 || q.s.ModelSlots[slotKey(m.ID, "test")].InFlight != 0 {
				t.Fatal("timeout retained slot")
			}
		})
	}
}

func TestPrivacyGenericRetryRequiresCatalogOptIn(t *testing.T) {
	for _, safe := range []bool{false, true} {
		t.Run(fmt.Sprint(safe), func(t *testing.T) {
			q, _, m := modelFixture(t)
			if err := json.Unmarshal([]byte(fmt.Sprintf(`{"retry_safe":%v}`, safe)), &q.models.Models[0]); err != nil {
				t.Fatal(err)
			}
			first := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); panic(http.ErrAbortHandler) }))
			defer first.Close()
			var retries atomic.Int32
			second := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { retries.Add(1); w.Write([]byte("ok")) }))
			defer second.Close()
			m.ModelURL = first.URL
			m.ModelConfigHash = modelConfigHash(q.models)
			check(t, q, m, nil)
			other := machine("two", "fake")
			other.ModelURL = second.URL
			other.ModelConfigHash = m.ModelConfigHash
			check(t, q, other, nil)
			for _, worker := range []Machine{m, other} {
				q.s.ModelSlots[slotKey(worker.ID, "test")] = &ModelSlot{Model: "test", Machine: worker.ID, Session: worker.Session, State: "ready", LastUsed: q.clock.Now()}
			}
			w := httptest.NewRecorder()
			API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, privacyRequest("/models/test/ocr"))
			if safe && (w.Code != 200 || retries.Load() != 1) {
				t.Fatal("safe request not retried")
			}
			if !safe && (w.Code != 503 || retries.Load() != 0) {
				t.Fatal("unsafe request retried")
			}
		})
	}
}

func TestPrivacyPinnedFailureBackoffAndStatus(t *testing.T) {
	q, clock, m := modelFixture(t)
	if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	fail := func() Reply {
		t.Helper()
		r, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: currentTestReportAttempt(q, m, "test"), State: "failed", Error: "synthetic-secret"}}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	fail()
	get := func() ModelSlot {
		t.Helper()
		s, err := q.Snapshot()
		if err != nil || len(s.LoadedModels) != 1 {
			t.Fatal("missing pin status")
		}
		return s.LoadedModels[0]
	}
	s := get()
	if s.State != "failed" || s.Error == "" || strings.Contains(s.Error, "synthetic-secret") {
		t.Fatal("pin failure missing or unsafe")
	}
	clock.Advance(time.Second - time.Nanosecond)
	fail()
	if get().State != "failed" {
		t.Fatal("retried too soon")
	}
	clock.Advance(time.Nanosecond)
	r := reportModel(t, q, m, "failed")
	if len(r.ModelPlan) != 1 || r.ModelPlan[0].State != "loading" {
		t.Fatal("failed pin stuck instead of retrying")
	}
	fail()
	clock.Advance(time.Second)
	fail()
	if get().State != "failed" {
		t.Fatal("second failure did not back off")
	}
	clock.Advance(time.Second)
	reportModel(t, q, m, "failed")
	if get().State != "loading" {
		t.Fatal("second retry missing")
	}
	reportModel(t, q, m, "ready")
	if get().State != "ready" || get().Error != "" {
		t.Fatal("successful retry retained failure")
	}
	// Cleanup failures must never be retried as though memory were free.
	_, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: currentTestReportAttempt(q, m, "test"), State: "unload_failed", Error: "synthetic-secret"}}})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	reportModel(t, q, m, "unload_failed")
	if get().State != "unload_failed" {
		t.Fatal("unsafe cleanup retried")
	}
}

func TestPrivacyServerLogsNone(t *testing.T) {
	m := serverTestModel(t, "ready")
	if err := json.Unmarshal([]byte(`{"logs":"none"}`), &m); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	engine := NewModelEngine(m, dir, RealClock{})
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	if err := engine.Load(ctx, m); err != nil {
		t.Fatal(err)
	}
	defer engine.Unload()
	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(logs) != 0 {
		t.Fatal("logs:none captured server output")
	}
}

func TestPrivacyWorkerRetriesCleanedFailedModel(t *testing.T) {
	old := &lifecycleEngine{loadErr: errors.New("warmup failed")}
	rt, wm, ctx := runtimeLoadFixture(t, old)
	rt.load(ctx, wm)
	rt.config = ModelConfig{Wait: time.Minute, Models: []Model{{Name: "test", Engine: "fake", MemoryGB: 8}}}
	rt.engineFactory = func(Model) Engine { return &lifecycleEngine{} }
	rt.sync([]ModelSlot{{Attempt: "test-engine", Model: "test", Session: "session", State: "loading"}})
	rt.wg.Wait()
	reports, _ := rt.report()
	if len(reports) != 1 || reports[0].State != "ready" || old.unloads.Load() != 1 {
		t.Fatal("worker ignored retry plan after successful cleanup")
	}
}

func TestPrivacyCompletedBatchLogRetention(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, spec("production"))
	m := machine("one", "fake")
	assigned := check(t, q, m, nil).Job
	r := result(assigned, true)
	if err := q.AppendLog(LogChunk{Result: r, Text: "synthetic log"}); err != nil {
		t.Fatal(err)
	}
	finish(t, q, assigned, true)
	clock.Advance(7 * 24 * time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil || len(logs) != 0 {
		t.Fatal("completed logs outlived default retention")
	}
}

func TestPrivacyWorkerLogRetentionProtectsLiveAndReplay(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)}
	old := clock.Now().Add(-8 * 24 * time.Hour)
	expired, pending, live, fresh := ID()+".log", ID()+".log", "engine-"+ID()+".log", "engine-"+ID()+".log"
	for _, name := range []string{expired, pending, live, fresh} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("synthetic"), 0600); err != nil {
			t.Fatal(err)
		}
		stamp := old
		if name == fresh {
			stamp = clock.Now()
		}
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveWorkerJSON(dir, strings.TrimSuffix(live, ".log")+".json", workerState{PID: 123}); err != nil {
		t.Fatal(err)
	}
	if err := pruneWorkerLogs(dir, pending, clock.Now(), DefaultLogRetention); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, expired)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expired batch log retained")
	}
	for _, name := range []string{pending, live, fresh} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatal("live, pending or recent output removed")
		}
	}
	if err := saveWorkerJSON(dir, strings.TrimSuffix(live, ".log")+".json", workerState{}); err != nil {
		t.Fatal(err)
	}
	if err := pruneWorkerLogs(dir, "", clock.Now(), DefaultLogRetention); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, live)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("inactive server log retained")
	}
}

func TestPrivacyConfiguredRetentionProtectsActiveBatch(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureLogRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureLogRetention(0); err == nil {
		t.Fatal("unbounded retention accepted")
	}
	j := submit(t, q, spec("production"))
	m := machine("one", "fake")
	assigned := check(t, q, m, nil).Job
	if err := q.AppendLog(LogChunk{Result: result(assigned, true), Text: "synthetic"}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Hour)
	check(t, q, m, assigned)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if logs, err := q.Logs(j.ID, 0); err != nil || len(logs) != 1 {
		t.Fatal("active batch logs pruned")
	}
	finish(t, q, assigned, true)
	clock.Advance(time.Hour)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if logs, err := q.Logs(j.ID, 0); err != nil || len(logs) != 0 {
		t.Fatal("custom retention not enforced")
	}
}

func TestPrivacyQueueOpenRemovesDefaultSpoolOrphans(t *testing.T) {
	base := t.TempDir()
	db := filepath.Join(base, "queue.db")
	root := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)}
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	dir := q.uploadSpool.dir
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(dir, "myce-model-request-crashed")
	if err := os.WriteFile(orphan, []byte("synthetic"), 0600); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("queue startup retained crashed upload")
	}
}

func TestPrivacyStatusScrubsWorkerErrors(t *testing.T) {
	q, _, m := modelFixture(t)
	if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	m.CacheError = strings.Repeat("synthetic-secret", 1000)
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: currentTestReportAttempt(q, m, "test"), State: "failed", Error: m.CacheError}}}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/api/status"} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, req)
		if w.Code != 200 || strings.Contains(w.Body.String(), "synthetic-secret") || !strings.Contains(w.Body.String(), "model load or unload failed") {
			t.Fatal("unsafe or hidden status failure")
		}
	}
}

func TestPrivacyStatusBrowserAuthUsesFleetToken(t *testing.T) {
	q, _ := fixture(t)
	h := API{Queue: q, Token: "test"}.Handler()
	for _, path := range []string{"/", "/api/status"} {
		req := httptest.NewRequest("GET", path, nil)
		req.SetBasicAuth("myce", "test")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatal("browser fleet-token authentication failed")
		}
		if strings.Contains(w.Body.String(), "Bearer test") {
			t.Fatal("token embedded in status")
		}
	}
}

func TestPrivacyWorkerFrozenHeadersTimesOut(t *testing.T) {
	upstream := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer upstream.Close()
	wm := &workerModel{state: "ready", model: Model{RequestTimeout: 20 * time.Millisecond}, engine: &LlamaEngine{url: upstream.URL}}
	rt := &modelRuntime{models: map[string]*workerModel{"test": wm}, session: "session", token: "test"}
	req := privacyRequest("/v1/chat/completions")
	req.Header.Set("X-Myce-Session", "session")
	w := httptest.NewRecorder()
	rt.serve(w, req)
	if w.Code != 504 || !strings.Contains(w.Body.String(), "request timeout") || wm.inflight != 0 {
		t.Fatal("worker timeout is unclear or retained slot")
	}
}

func TestPrivacyCatalogFieldsValidate(t *testing.T) {
	base := "models:\n  - name: synthetic\n    engine: fake\n    path: synthetic\n    memory_gb: 1\n    platforms: [fake]\n"
	path := filepath.Join(t.TempDir(), "models.yaml")
	if err := os.WriteFile(path, []byte(base+"    request_timeout: 25ms\n    logs: none\n    retry_safe: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := ReadModels(path)
	if err != nil || c.Models[0].RequestTimeout != 25*time.Millisecond || c.Models[0].Logs != "none" || !c.Models[0].RetrySafe {
		t.Fatal("catalog privacy fields not parsed")
	}
	for _, invalid := range []string{"    request_timeout: -1s\n", "    logs: invalid\n"} {
		if err := os.WriteFile(path, []byte(base+invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadModels(path); err == nil {
			t.Fatal("invalid catalog privacy field accepted")
		}
	}
}
