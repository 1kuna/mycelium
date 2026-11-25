package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOutputUploadWaitsForMeasuredQueueService(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	c.Clock = clock
	c.publication = &publicationProgress{}
	c.singleAttempt = true
	_, input := retentionUpload(t, c, "input")
	s := spec("experiment")
	s.Resumable = false
	s.Files = FileManifest{"input": input}
	s.Outputs = map[string]string{"first": "results", "output": "results"}
	assigned := submit(t, q, s)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != assigned.ID {
		t.Fatal("no exact attempt")
	}
	ctx, cancel := context.WithTimeout(withFileStoreAttempt(context.Background(), *j), testWaitTimeout)
	defer cancel()
	dir := t.TempDir()
	content := strings.Repeat("0123456789", 10_000)
	path := filepath.Join(dir, "output")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	ref := creationRef(content)
	// A fast first response leaves the client on the TCP recovery floor,
	// while earlier check-ins show that a queue turn can take much longer.
	const service = 3 * time.Second
	firstPath := filepath.Join(dir, "first")
	if err := os.WriteFile(firstPath, []byte("first output"), 0600); err != nil {
		t.Fatal(err)
	}
	first := creationRef("first output")
	if err := c.uploadOutputFile(ctx, firstPath, first); err != nil {
		t.Fatal(err)
	}
	q.checkinService.record(clock, service)
	clock.Advance(checkinServiceBucket)
	q.checkinService.record(clock, time.Millisecond)
	entered := make(chan struct{})
	continueAccounting := make(chan struct{})
	var blockOnce sync.Once
	original := q.files.recordUpload
	q.files.recordUpload = func(hash, purpose string, created bool, contexts ...context.Context) error {
		if hash == ref.SHA256 && !created {
			blockOnce.Do(func() {
				close(entered)
				<-continueAccounting
			})
		}
		return original(hash, purpose, created, contexts...)
	}
	type uploadEnd struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan uploadEnd, 1)
	started := time.Now()
	go func() {
		err := c.uploadOutputFile(ctx, path, ref)
		done <- uploadEnd{err, time.Since(started)}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		close(continueAccounting)
		t.Fatal("upload never reached final accounting", ctx.Err())
	}
	const hold = clientRecoveryPeriod + clientRecoveryPeriod/2
	var end uploadEnd
	var endedDuringHold bool
	func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		close(continueAccounting)
		await(t, func() bool { return q.mu.waiting.Load() > 0 })
		// The real socket deadline uses wall time. Queue decisions use the
		// injected clock even while the wall timer holds the fixture.
		clock.Advance(service)
		timer := time.NewTimer(hold)
		defer timer.Stop()
		select {
		case end = <-done:
			endedDuringHold = true
			t.Errorf("upload ended at %s while accounting was blocked: %v", end.elapsed.Round(time.Millisecond), end.err)
			<-timer.C
		case <-timer.C:
		}
	}()
	if !endedDuringHold {
		end = <-done
		if end.err != nil {
			t.Fatalf("upload failed after accounting released: %v", end.err)
		}
	}
	if end.err != nil {
		// For the failing baseline, measure completion after the worker's
		// service-paced retry while keeping the one-pass assertion above.
		var pace ClientPace
		pace.Observe(end.elapsed, false)
		delay := pace.ServiceDelay(end.err, func() float64 { return 0 })
		if err := c.pause(ctx, time.Until(started.Add(end.elapsed+delay))); err != nil {
			t.Fatal(err)
		}
		if err := c.uploadOutputFile(ctx, path, ref); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.files.verify(ctx, ref); err != nil {
		t.Fatal("upload reported completion before the blob was verified", err)
	}
	t.Logf("verified 100 kB upload in %s through a %s accounting hold", time.Since(started).Round(time.Millisecond), hold)
	finished := result(j, true)
	finished.OutputRefs = FileManifest{"first": first, "output": ref}
	if err := q.Complete(finished); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "succeeded" || got.Attempt != j.Attempt || got.Session != j.Session || got.OutputRefs["output"] != ref {
		t.Fatalf("output lost exact attempt custody: %+v", got)
	}
	clock.Advance(checkinServiceBucket)
	if got := outputUploadServiceHint(q, time.Second); got != "" {
		t.Fatalf("expired queue service was still advertised: %s", got)
	}
}

func outputUploadServiceHint(q *Queue, period time.Duration) string {
	r := httptest.NewRequest(http.MethodPost, "/api/files/uploads/", nil)
	r.Header.Set(uploadQueueServiceHeader, "1")
	r.Header.Set("X-Myce-Upload-Progress", period.String())
	w := httptest.NewRecorder()
	q.reportUploadQueueService(w, r)
	if w.Code == http.StatusProcessing {
		return w.Result().Header.Get(uploadQueueServiceHeader)
	}
	return ""
}

func TestOutputUploadHintExcludesSlowCheckinBody(t *testing.T) {
	q, _ := fixture(t)
	var duration, occupied time.Duration
	q.observeRequest = func(route string, elapsed, wait, hold, _ time.Duration) {
		if route == "POST /api/workers/checkin" {
			duration, occupied = elapsed, wait+hold
		}
	}
	body, err := json.Marshal(keyedFixtureCheckin(Checkin{Machine: machine("worker", "fake")}))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/api/workers/checkin", &slowCheckinBody{Reader: bytes.NewReader(body)})
	r.Header.Set("Authorization", "Bearer fixture")
	r.Header.Set("X-Myce-Start-Gate", startGateProtocol)
	w := httptest.NewRecorder()
	(API{Queue: q, Token: "fixture"}).Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK || occupied <= 0 || duration-occupied < slowCheckinBodyDelay {
		t.Fatalf("expected successful check-in with a slow body: status=%d duration=%s occupied=%s body=%s", w.Code, duration, occupied, w.Body)
	}
	raw := outputUploadServiceHint(q, time.Nanosecond)
	hint, err := time.ParseDuration(raw)
	if err != nil || hint <= 0 || hint > occupied {
		t.Fatalf("hint = %q, want positive queue service within the full request's mutex wait and hold %s", raw, occupied)
	}
	if got := outputUploadServiceHint(q, occupied+(duration-occupied)/2); got != "" {
		t.Fatalf("slow check-in body inflated hint to %s", got)
	}
}

const slowCheckinBodyDelay = 75 * time.Millisecond

type slowCheckinBody struct {
	io.Reader
	once sync.Once
}

func (b *slowCheckinBody) Read(p []byte) (int, error) {
	b.once.Do(func() { time.Sleep(slowCheckinBodyDelay) })
	return b.Reader.Read(p)
}

func TestOutputUploadTimesOutAfterQueueServiceHint(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(uploadQueueServiceHeader) != "1" {
			t.Error("upload did not opt into queue service hints")
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set(uploadQueueServiceHeader, "2m")
		w.WriteHeader(http.StatusProcessing)
		<-release
	}))
	defer server.Close()
	defer close(release)
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return fastHintDeadlineConn{conn}, nil
	}}
	c := Client{URL: server.URL, HTTP: &http.Client{Transport: transport}}
	const content = "one output"
	path := filepath.Join(t.TempDir(), "output")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.uploadOutputFile(ctx, path, creationRef(content))
	if !errors.Is(err, errFileTransferStalled) || !strings.Contains(err.Error(), "no network progress for 30s") {
		t.Fatalf("silent peer should hit the 30s socket window before the test context: %v", err)
	}
}

type fastHintDeadlineConn struct{ net.Conn }

func (c fastHintDeadlineConn) SetReadDeadline(end time.Time) error {
	// Compress long deadlines 100:1 on the real test socket. The 30s window
	// expires in 300ms, while the uncapped 8m window exceeds the test context.
	if now := time.Now(); end.Sub(now) > 2*clientRecoveryPeriod {
		end = now.Add(end.Sub(now) / 100)
	}
	return c.Conn.SetReadDeadline(end)
}

// ResponseRecorder treats the first 1xx as final; a real HTTP transport consumes
// informational responses and returns the later final response to its caller.
type outputUploadRecorder struct {
	*httptest.ResponseRecorder
	progress int
}

func (w *outputUploadRecorder) WriteHeader(status int) {
	if status >= 100 && status < 200 {
		if status == http.StatusProcessing {
			w.progress++
		}
		return
	}
	w.ResponseRecorder.WriteHeader(status)
}

// Exercise the actual queue/tus finish callback, including its durability
// barrier and old wrapper responses, without sockets or wall-clock sleeps.
func TestOutputUploadCreationAndReconciliation(t *testing.T) {
	for _, mode := range []string{"new", "duplicate", "partial", "finish-refusal", "stale", "empty", "changed"} {
		t.Run(mode, func(t *testing.T) {
			q, store := creationStore(t, 1<<20)
			content := "one chunk of output"
			if mode == "empty" {
				content = ""
			}
			ref := creationRef(content)
			path := filepath.Join(t.TempDir(), "output")
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "duplicate" {
				if err := os.WriteFile(store.blob(ref), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "partial" {
				w := httptest.NewRecorder()
				store.serveUpload(w, creationRequest(context.Background(), ref))
				if w.Code != 201 {
					t.Fatal(w.Code, w.Body.String())
				}
				r := httptest.NewRequest("PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(content[:4]))
				r.Header.Set("Tus-Resumable", "1.0.0")
				r.Header.Set("Upload-Offset", "0")
				r.Header.Set("Content-Type", "application/offset+octet-stream")
				w = httptest.NewRecorder()
				store.serveUpload(w, r)
				if w.Code != 204 {
					t.Fatal(w.Code, w.Body.String())
				}
			}
			if mode == "finish-refusal" {
				store.syncDir = func(string) error { return errors.New("fixture sync refusal") }
			}
			if mode == "stale" {
				store.checkAttempt = func(context.Context) error { return ErrStale }
			}
			if mode == "changed" {
				if err := os.WriteFile(path, []byte("different output"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			requests, progress := 0, 0
			c := Client{URL: "http://queue.invalid", Clock: q.clock, singleAttempt: true, HTTP: &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				requests++
				w := &outputUploadRecorder{ResponseRecorder: httptest.NewRecorder()}
				if strings.HasPrefix(r.URL.Path, "/api/files/blobs/") {
					// serveUpload handles only tus routes. Match the queue's blob
					// identity contract for the legacy reconciliation probe.
					if store.verify(r.Context(), ref) != nil {
						w.WriteHeader(404)
					} else {
						w.Header().Set("Content-Length", strconv.FormatInt(ref.SizeBytes, 10))
						w.Header().Set("ETag", `"`+ref.SHA256+`"`)
						w.WriteHeader(200)
					}
				} else {
					store.serveUpload(w, r)
				}
				progress += w.progress
				return w.Result(), nil
			})}}
			err := c.uploadOutputFile(context.Background(), path, ref)
			if mode == "finish-refusal" || mode == "stale" || mode == "changed" {
				if err == nil {
					t.Fatal("refused or changed output acknowledged")
				}
				return
			}
			if err != nil || store.verify(context.Background(), ref) != nil {
				t.Fatal("output not published", err)
			}
			if (mode == "new" || mode == "empty") && requests != 1 {
				t.Fatal("fresh file request count", requests)
			}
			if mode == "new" && progress == 0 {
				t.Fatal("creation-with-upload did not report verification progress")
			}
		})
	}
}
