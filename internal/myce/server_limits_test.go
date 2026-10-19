package myce

import (
	"context"
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

	"gopkg.in/yaml.v3"
)

func limitedModelFixture(t *testing.T, limit int) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock, m := modelFixture(t)
	data, err := yaml.Marshal(q.models)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "models.yaml")
	data = []byte(strings.Replace(string(data), "name: test", fmt.Sprintf("name: test\n      max_in_flight: %d", limit), 1))
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	config, err := ReadModels(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(config)
	check(t, q, m, nil)
	return q, clock, m
}

func TestModelLimitOrdersWaitingCallsAndReleasesCapacity(t *testing.T) {
	q, clock, m := limitedModelFixture(t, 1)
	active := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	experiment := addModelCall(t, q, "experiment")
	later := addModelCall(t, q, "deadline")
	early := &ModelCall{Model: "test", Project: "test", Priority: "deadline"}
	deadline := clock.Now().Add(time.Minute)
	early.Deadline = &deadline
	if err := q.addCall(early); err != nil {
		t.Fatal(err)
	}
	production := addModelCall(t, q, "production")
	for _, c := range []*ModelCall{experiment, later, early, production} {
		if c.Machine != "" {
			t.Fatal("call dispatched while loaded copy was full")
		}
	}
	for _, pair := range [][2]*ModelCall{{active, production}, {production, early}, {early, later}, {later, experiment}} {
		if err := q.finishCall(pair[0].ID); err != nil {
			t.Fatal(err)
		}
		if pair[1].Machine != m.ID {
			t.Fatalf("next %s call did not receive released capacity", pair[1].Priority)
		}
	}
}

func TestModelLimitUsesFreeCopyAndWorkerLossClearsCapacity(t *testing.T) {
	q, clock, m := limitedModelFixture(t, 2)
	active := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	sibling := addModelCall(t, q, "production")
	other := machine("two", "fake")
	other.ModelURL = "http://127.0.0.1:1235"
	other.ModelConfigHash = m.ModelConfigHash
	check(t, q, other, nil)
	second := addModelCall(t, q, "production")
	reportModel(t, q, other, "ready")
	if second.Machine != other.ID {
		t.Fatal("free loaded copy was not used")
	}
	addModelCall(t, q, "production") // Fill the second copy too.
	waiting := addModelCall(t, q, "experiment")
	if waiting.Machine != "" {
		t.Fatal("both loaded copies are full")
	}
	clock.Advance(ProductionLossAfter)
	reportModel(t, q, other, "ready")
	if q.s.ModelSlots[slotKey(m.ID, "test")] != nil {
		t.Fatal("lost worker retained slots")
	}
	// Rejoin with a fresh session: the old call must not spend its capacity.
	m.Session = ID()
	check(t, q, m, nil)
	reportModel(t, q, m, "ready")
	if waiting.Machine != m.ID {
		t.Fatal("lost copy capacity was not released on rejoin")
	}
	for _, old := range []*ModelCall{active, sibling} {
		if err := q.finishCall(old.ID); err != nil {
			t.Fatal(err)
		}
	}
	if q.s.ModelSlots[slotKey(m.ID, "test")].InFlight != 1 {
		t.Fatal("stale completion released new session's slot")
	}
}

func TestModelLimitHTTPSerializesAndCancellationReleases(t *testing.T) {
	for _, generic := range []bool{true, false} {
		t.Run(fmt.Sprintf("generic=%v", generic), func(t *testing.T) { testModelLimitHTTP(t, generic) })
	}
}

func testModelLimitHTTP(t *testing.T, generic bool) {
	q, _, m := limitedModelFixture(t, 1)
	entered := make(chan string, 8)
	release := make(chan struct{}, 8)
	exited := make(chan struct{}, 8)
	var active, overlap atomic.Int32
	upstream := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if active.Add(1) > 1 {
			overlap.Add(1)
			active.Add(-1)
			w.WriteHeader(409)
			return
		}
		defer func() { active.Add(-1); exited <- struct{}{} }()
		entered <- r.Header.Get("X-Call")
		select {
		case <-release:
			fmt.Fprint(w, "ok")
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	m.ModelURL = upstream.URL
	check(t, q, m, nil)
	seed := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	q.finishCall(seed.ID)
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	handler := API{Queue: q, Token: "test"}.Handler()
	start := func(name, priority string) (context.CancelFunc, <-chan int) {
		child, stop := context.WithCancel(ctx)
		path, payload := "/models/test/infer", "audio"
		if !generic {
			path, payload = "/v1/chat/completions", `{"model":"test"}`
		}
		req := httptest.NewRequest("POST", path, strings.NewReader(payload)).WithContext(child)
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("X-Myce-Project", "test")
		req.Header.Set("X-Myce-Priority", priority)
		req.Header.Set("X-Call", name)
		done := make(chan int, 1)
		go func() { w := httptest.NewRecorder(); handler.ServeHTTP(w, req); done <- w.Code }()
		return stop, done
	}
	receive := func(want string) {
		t.Helper()
		select {
		case got := <-entered:
			if got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
		case <-ctx.Done():
			t.Fatal("request did not reach upstream")
		}
	}
	stop, first := start("active", "experiment")
	receive("active")
	_, experiment := start("experiment", "experiment")
	_, production := start("production", "production")
	// Wait until both calls are actually queued, using queue state under its lock.
	for {
		q.mu.Lock()
		n := len(q.calls)
		q.mu.Unlock()
		if n == 3 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("calls did not queue")
		case <-time.After(time.Millisecond):
		}
	}
	stop()
	select {
	case <-exited:
	case <-ctx.Done():
		t.Fatal("cancel did not reach upstream")
	}
	receive("production")
	release <- struct{}{}
	receive("experiment")
	release <- struct{}{}
	for _, done := range []<-chan int{first, production, experiment} {
		select {
		case code := <-done:
			if code == 409 {
				t.Fatal("overlapping HTTP request")
			}
		case <-ctx.Done():
			t.Fatal("handler did not finish")
		}
	}
	if overlap.Load() != 0 {
		t.Fatal("upstream recorded overlap")
	}
}

func TestUploadSpoolBudgetHTTPAdmissionAndCleanup(t *testing.T) {
	q, _, m := modelFixture(t)
	dir := t.TempDir()
	if err := q.ConfigureUploadSpool(dir, 8); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 4)
	release := make(chan struct{}, 4)
	upstream := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		select {
		case <-release:
			fmt.Fprint(w, "ok")
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	m.ModelURL = upstream.URL
	check(t, q, m, nil)
	seed := addModelCall(t, q, "experiment")
	reportModel(t, q, m, "ready")
	q.finishCall(seed.ID)
	handler := API{Queue: q, Token: "test"}.Handler()
	request := func(body io.Reader, length int64) *http.Request {
		r := httptest.NewRequest("POST", "/models/test/audio", body)
		r.ContentLength = length
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("X-Myce-Project", "test")
		r.Header.Set("X-Myce-Priority", "experiment")
		return r
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	done := make(chan int, 2)
	for i := 0; i < 2; i++ {
		r := request(strings.NewReader("1234"), 4).WithContext(ctx)
		go func() { w := httptest.NewRecorder(); handler.ServeHTTP(w, r); done <- w.Code }()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("upload did not reach server")
		}
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 3 { // Two uploads plus the durable format marker.
		t.Fatalf("two held uploads: %d files, %v", len(files), err)
	}
	for _, length := range []int64{1, -1} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, request(strings.NewReader("x"), length))
		if w.Code != 507 || !strings.Contains(w.Body.String(), "upload spool byte budget") {
			t.Fatalf("full spool: %d %s", w.Code, w.Body.String())
		}
	}
	release <- struct{}{}
	release <- struct{}{}
	for i := 0; i < 2; i++ {
		select {
		case code := <-done:
			if code != 200 {
				t.Fatalf("upload status %d", code)
			}
		case <-ctx.Done():
			t.Fatal("upload did not finish")
		}
	}
	assertEmpty := func() {
		t.Helper()
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 1 || files[0].Name() != FormatMarker {
			t.Fatalf("leftover spool files: %v %v", files, err)
		}
	}
	assertEmpty()
	for _, length := range []int64{9, -1, maxGenericRequestBytes + 1} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, request(strings.NewReader("123456789"), length))
		want := 507
		if length > maxGenericRequestBytes {
			want = 413
		}
		if w.Code != want {
			t.Fatalf("oversize length %d: %d %s", length, w.Code, w.Body.String())
		}
		assertEmpty()
	}
	// Abort after a partial body has reached the spool writer.
	reader, writer := io.Pipe()
	aborted := make(chan int, 1)
	go func() { w := httptest.NewRecorder(); handler.ServeHTTP(w, request(reader, -1)); aborted <- w.Code }()
	if _, err := writer.Write([]byte("1234")); err != nil {
		t.Fatal(err)
	}
	writer.CloseWithError(context.Canceled)
	select {
	case <-aborted:
	case <-ctx.Done():
		t.Fatal("aborted upload did not finish")
	}
	reader.Close()
	assertEmpty()
	// All quota is reusable after successful and failed requests.
	release <- struct{}{}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, request(strings.NewReader("12345678"), 8))
	if w.Code != 200 {
		t.Fatalf("released bytes not reusable: %d %s", w.Code, w.Body.String())
	}
	assertEmpty()
}

func TestModelLimitUnsetAndHigherLimit(t *testing.T) {
	for _, limit := range []int{0, 2} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			q, _, m := limitedModelFixture(t, limit)
			first := addModelCall(t, q, "experiment")
			reportModel(t, q, m, "ready")
			second := addModelCall(t, q, "production")
			third := addModelCall(t, q, "production")
			if first.Machine != m.ID || second.Machine != m.ID {
				t.Fatal("allowed concurrent calls did not dispatch")
			}
			if limit == 0 && third.Machine != m.ID {
				t.Fatal("unset limit changed unlimited behavior")
			}
			if limit == 2 && third.Machine != "" {
				t.Fatal("third call exceeded two-request limit")
			}
		})
	}
}

func TestModelLimitRejectsNegative(t *testing.T) {
	q, _, _ := modelFixture(t)
	c := q.models
	c.Models[0].MaxInFlight = -1
	if err := q.ConfigureModels(c); err == nil || !strings.Contains(err.Error(), "max_in_flight") {
		t.Fatalf("negative limit accepted: %v", err)
	}
}

func TestUploadSpoolRejectsInvalidConfiguration(t *testing.T) {
	q, _, _ := modelFixture(t)
	for _, budget := range []int64{0, -1} {
		if err := q.ConfigureUploadSpool(t.TempDir(), budget); err == nil {
			t.Fatal("nonpositive upload budget accepted")
		}
	}
}

func TestModelLimitWaitingCallDoesNotBlockFittingBatch(t *testing.T) {
	q, _, m := limitedModelFixture(t, 1)
	m.FreeMemoryGB = 16
	check(t, q, m, nil)
	addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	waiting := addModelCall(t, q, "production")
	batch := submit(t, q, spec("deadline"))
	reply := reportModel(t, q, m, "ready")
	if waiting.Machine != "" {
		t.Fatal("full copy accepted another call")
	}
	if reply.Job == nil || reply.Job.ID != batch.ID {
		t.Fatal("call waiting for model capacity blocked a batch that fits alongside it")
	}
}
