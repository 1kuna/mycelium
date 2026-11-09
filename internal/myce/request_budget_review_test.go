package myce

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

type slowBudgetBody struct {
	io.Reader
	delayed bool
}

func (r *slowBudgetBody) Read(p []byte) (int, error) {
	if !r.delayed {
		r.delayed = true
		time.Sleep(31 * time.Second)
	}
	return r.Reader.Read(p)
}

type slowBudgetResponse struct{ *httptest.ResponseRecorder }

func (w slowBudgetResponse) Write(p []byte) (int, error) {
	time.Sleep(10 * time.Second)
	return w.ResponseRecorder.Write(p)
}
