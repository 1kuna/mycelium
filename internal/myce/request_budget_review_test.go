package myce

import (
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

func TestRequestBudgetCustomDecoderExpansion(t *testing.T) {
	for _, shape := range []string{
		`{"needs":{"platform":[BODY]}}`,
		`{"needs":{"gpu":{"devices":[BODY]}}}`,
	} {
		q, _ := fixture(t)
		budgetHost(q, 128<<10, 1)
		data := strings.Replace(shape, "BODY", strings.TrimSuffix(strings.Repeat("[],", 600), ","), 1)
		r := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(data))
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		API{q, "test"}.Handler().ServeHTTP(w, r)
		if w.Code != 413 || !strings.Contains(w.Body.String(), "request_too_large") {
			t.Fatal("custom decoder bypassed preflight", w.Code, w.Body.String())
		}
	}
}

type blockedBudgetWriter struct {
	*httptest.ResponseRecorder
	entered, release chan struct{}
}

func (w *blockedBudgetWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return w.ResponseRecorder.Write(p)
}

func TestRequestBudgetProjectionLivesThroughResponse(t *testing.T) {
	q, _ := fixture(t)
	budgetHost(q, 128<<10, 1)
	job := &Job{ID: "projection-ledger", State: "succeeded", ReleaseConfirmed: true}
	for i := range 100 {
		job.LedgerEntries = append(job.LedgerEntries, JobLedgerEntry{Sequence: i + 1, Reason: strings.Repeat("r", 200)})
	}
	q.s.Jobs[job.ID] = job
	handler := API{q, "test"}.Handler()
	request := func() *http.Request {
		r := httptest.NewRequest("GET", "/api/jobs/"+job.ID+"/ledger?limit=100", nil)
		r.Header.Set("Authorization", "Bearer test")
		return r
	}
	first := &blockedBudgetWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
	done := make(chan struct{})
	go func() { handler.ServeHTTP(first, request()); close(done) }()
	<-first.entered
	q.requestBudgets.mu.Lock()
	held := q.requestBudgets.held
	q.requestBudgets.mu.Unlock()
	if held == 0 {
		close(first.release)
		<-done
		t.Fatal("projection/encoding was not charged")
	}
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, request())
	q.requestBudgets.mu.Lock()
	peak := q.requestBudgets.held
	q.requestBudgets.mu.Unlock()
	close(first.release)
	<-done
	if peak > 128<<10 {
		t.Fatal("concurrent projections exceeded headroom", peak)
	}
	var page JobLedgerPage
	if first.Code != 200 || json.Unmarshal(first.Body.Bytes(), &page) != nil || len(page.Entries) >= 100 || !page.HasMore {
		t.Fatal("variable-sized rows were not paged", first.Code, len(page.Entries))
	}
	if second.Code != 200 && second.Code != 503 {
		t.Fatal("wrong pressure response", second.Code, second.Body.String())
	}
	if q.requestBudgets.held != 0 {
		t.Fatal("projection charge survived response")
	}
}

func TestRequestBudgetIntervalVariablePayload(t *testing.T) {
	q, _ := fixture(t)
	budgetHost(q, 128<<10, 1)
	for i, project := range []string{"small", strings.Repeat("x", 16000)} {
		data, _ := json.Marshal(ModelIntervalEvent{Sequence: i + 1, ModelInterval: ModelInterval{Project: project}})
		if _, err := q.db.Exec("INSERT INTO model_intervals(seq,data) VALUES(?,?)", i+1, data); err != nil {
			t.Fatal(err)
		}
	}
	q.s.ModelIntervalHighWater = 2
	first, err := q.ModelIntervals(t.Context(), 0, -1, 100)
	if err != nil || len(first.Entries) != 1 || !first.HasMore || first.Next != 1 {
		t.Fatal(first, err)
	}
	_, err = q.ModelIntervals(t.Context(), first.Next, first.HighWater, 100)
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Status != 503 || refusal.Code != "projection_capacity_exceeded" {
		t.Fatal("large row bypassed memory allowance", err)
	}
	if q.requestBudgets.held != 0 {
		t.Fatal("interval capacity leaked")
	}
}

func TestRequestBudgetCachedPeerAndUnavailableRefusals(t *testing.T) {
	q, _ := fixture(t)
	budgetHost(q, 64<<20, 2)
	handler := API{q, "test"}.Handler()
	request := func(negotiated bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/status", nil)
		r.Header.Set("Authorization", "Bearer test")
		if negotiated {
			r.Header.Set("X-Myce-Request-Budgets", requestBudgetProtocol)
			r.Header.Set("X-Myce-Max-Response-Bytes", "1")
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := request(false); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request(true); w.Code != 503 || !strings.Contains(w.Body.String(), "projection_capacity_exceeded") || !strings.Contains(w.Body.String(), "fitting bytes=1") {
		t.Fatal("cached peer capacity bypass", w.Code, w.Body.String())
	}
	q.requestBudgets.memory = func(context.Context) (int64, error) { return 0, errors.New("private measurement failure") }
	w := request(false)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "request_capacity_unavailable") || !strings.Contains(w.Body.String(), "fitting bytes=0") || strings.Contains(w.Body.String(), "private") {
		t.Fatal("capacity refusal lost or unsanitized", w.Code, w.Body.String())
	}
}

func TestRequestBudgetGrowingFolderUsesActualBytes(t *testing.T) {
	folder := t.TempDir()
	for _, name := range []string{"a", "b"} {
		if err := os.WriteFile(filepath.Join(folder, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	reads := 0
	_, err := readExtensionFolder(folder, 100, func(_ string, remaining int64) ([]byte, error) { reads++; return make([]byte, remaining), nil })
	if err == nil || reads != 1 {
		t.Fatal("second growing file exceeded aggregate allowance", reads, err)
	}
}

func TestRequestBudgetSpoolMeasurementWriteFence(t *testing.T) {
	dir := t.TempDir()
	control := filepath.Join(dir, "control")
	if err := os.WriteFile(control, make([]byte, 64), 0600); err != nil {
		t.Fatal(err)
	}
	s := uploadSpool{dir: dir, defaultDir: control + ".uploads", limit: -1, free: func(string) (int64, error) { return 1024, nil }}
	if err := s.reserve(960); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(dir, "payload")
	if err != nil {
		t.Fatal(err)
	}
	f := &spooledModelRequest{File: file, spool: &s, held: 960, maxBytes: 960}
	defer f.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	s.free = func(string) (int64, error) { close(entered); <-release; return 1024, nil }
	admitted := make(chan error, 1)
	go func() { admitted <- s.reserve(960) }()
	<-entered
	written := make(chan error, 1)
	go func() { _, err := f.Write(make([]byte, 960)); written <- err }()
	// Sampling must own the accounting fence until admission is decided.
	if s.mu.TryLock() {
		s.mu.Unlock()
		close(release)
		<-admitted
		<-written
		t.Fatal("free-space sample was not fenced against write credit")
	}
	close(release)
	if err := <-admitted; !errors.Is(err, errUploadSpoolFull) {
		t.Fatal("stale free space admitted a second upload", err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestRequestBudgetCachedReceiptProjection(t *testing.T) {
	q, _ := fixture(t)
	budgetHost(q, 64<<20, 1)
	job := &Job{ID: "cached-receipt", State: "succeeded", ReleaseConfirmed: true, Spec: Spec{Name: strings.Repeat("x", 10000)}}
	q.s.Jobs[job.ID] = job
	handler := API{q, "test"}.Handler()
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/jobs/"+job.ID, nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := request(); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	budgetHost(q, 64<<10, 1)
	if w := request(); w.Code != 503 || !strings.Contains(w.Body.String(), "projection_capacity_exceeded") {
		t.Fatal("cached receipt escaped shared capacity", w.Code, w.Body.String())
	}
	if q.requestBudgets.held != 0 {
		t.Fatal("receipt capacity leaked")
	}
}
