package myce

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// Real client/dispatcher, fake tus/link/store. synctest advances transport timers
// only while all goroutines are blocked: there are no wall-clock sleeps. Body
// transmission is serialized at linkBytesPerSecond; independent request latency
// overlaps. Count every request, including retry/reconciliation and preflight.
type outputTestTransport struct {
	mu                 sync.Mutex
	latency            time.Duration
	linkBytesPerSecond int64
	linkFree           time.Time
	blobs              map[string]FileRef
	uploads            map[string]FileRef
	offsets            map[string]int64
	requests           map[string]int
	preflights         []FileManifest
	newFiles, refuseAt int
	active, peak       int
	lostReply          bool
	legacy             bool
	newFileRequests    int
	cancelAt           int
	cancel             context.CancelFunc
}

func newOutputTestTransport() *outputTestTransport {
	return &outputTestTransport{latency: 50 * time.Millisecond, linkBytesPerSecond: 10_000_000, blobs: map[string]FileRef{}, uploads: map[string]FileRef{}, offsets: map[string]int64{}, requests: map[string]int{}}
}

func outputResponse(r *http.Request, status int, headers http.Header, body string) *http.Response {
	if headers == nil {
		headers = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: r, ContentLength: int64(len(body))}
}

func (s *outputTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			return nil, err
		}
	}
	hash := filepath.Base(r.URL.Path)
	if r.Method == "POST" && r.URL.Path == "/api/files/uploads/" {
		raw, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(r.Header.Get("Upload-Metadata"), "sha256 "))
		hash = string(raw)
	}
	s.mu.Lock()
	s.requests[r.Method+" "+r.URL.Path]++
	if r.Method == "POST" && r.URL.Path == "/api/files/uploads/" && s.cancelAt > 0 && s.requests[r.Method+" "+r.URL.Path] == s.cancelAt {
		s.cancel()
		s.cancelAt = 0
	}
	s.active++
	s.peak = max(s.peak, s.active)
	end := maxTime(time.Now(), s.linkFree).Add(time.Duration(int64(len(body)) * int64(time.Second) / s.linkBytesPerSecond))
	s.linkFree = end
	end = end.Add(s.latency)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
	// This timer is virtual inside synctest, never a real sleep.
	timer := time.NewTimer(time.Until(end))
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return nil, r.Context().Err()
	case <-timer.C:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.URL.Path == "/api/files/preflight" {
		var files FileManifest
		if err := json.Unmarshal(body, &files); err != nil {
			return nil, err
		}
		s.preflights = append(s.preflights, files)
		return outputResponse(r, 200, nil, `{"ok":true}`), nil
	}
	if strings.HasPrefix(r.URL.Path, "/api/files/blobs/") {
		if ref, ok := s.blobs[hash]; ok {
			res := outputResponse(r, 200, http.Header{"Etag": {`"` + hash + `"`}}, "")
			res.ContentLength = ref.SizeBytes
			return res, nil
		}
		return outputResponse(r, 404, nil, ""), nil
	}
	if r.Method == "POST" {
		s.newFileRequests++
		size, _ := strconv.ParseInt(r.Header.Get("Upload-Length"), 10, 64)
		ref := FileRef{hash, size}
		if _, exists := s.blobs[hash]; exists {
			return outputResponse(r, 201, nil, ""), nil // Previous queue's dedup response.
		}
		if _, exists := s.uploads[hash]; exists {
			return outputResponse(r, 201, nil, ""), nil // Previous queue's resume response.
		}
		s.newFiles++
		if s.refuseAt > 0 && s.newFiles == s.refuseAt {
			return outputResponse(r, 507, nil, `{"error":"file_store_full","message":"fixture capacity refusal"}`), nil
		}
		s.uploads[hash] = ref
		if r.Header.Get("Content-Type") != "application/offset+octet-stream" || s.legacy {
			return outputResponse(r, 201, nil, ""), nil
		}
		got := sha256.Sum256(body)
		if int64(len(body)) != size || hex.EncodeToString(got[:]) != hash {
			return outputResponse(r, 400, nil, "invalid content"), nil
		}
		s.blobs[hash], s.offsets[hash] = ref, size
		if s.lostReply {
			s.lostReply = false
			return nil, io.ErrUnexpectedEOF
		}
		return outputResponse(r, 201, http.Header{"Upload-Offset": {strconv.FormatInt(size, 10)}}, ""), nil
	}
	ref := s.uploads[hash]
	if r.Method == "HEAD" {
		return outputResponse(r, 200, http.Header{"Upload-Offset": {strconv.FormatInt(s.offsets[hash], 10)}, "Upload-Length": {strconv.FormatInt(ref.SizeBytes, 10)}}, ""), nil
	}
	if r.Method == "PATCH" {
		offset, _ := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
		if offset != s.offsets[hash] {
			return outputResponse(r, 409, nil, "offset conflict"), nil
		}
		s.offsets[hash] += int64(len(body))
		if s.offsets[hash] == ref.SizeBytes {
			s.blobs[hash] = ref
		}
		return outputResponse(r, 204, http.Header{"Upload-Offset": {strconv.FormatInt(s.offsets[hash], 10)}}, ""), nil
	}
	return nil, fmt.Errorf("unexpected fixture request: %s %s", r.Method, r.URL.Path)
}

func outputFixture(t *testing.T, count int, bytes int64) (Job, FileManifest) {
	t.Helper()
	j := Job{ID: ID(), Attempt: 2, Machine: "fixture", Session: "session", Spec: Spec{Run: RunSpec{Workdir: t.TempDir()}, Outputs: map[string]string{"out/": "results"}}}
	if err := os.Mkdir(filepath.Join(j.Spec.Run.Workdir, "out"), 0700); err != nil {
		t.Fatal(err)
	}
	files := FileManifest{}
	for i := range count {
		size := bytes / int64(count)
		if int64(i) < bytes%int64(count) {
			size++
		}
		data := make([]byte, size)
		copy(data, fmt.Sprintf("unique-output-%08d", i))
		name := fmt.Sprintf("out/%04d.bin", i)
		path := filepath.Join(j.Spec.Run.Workdir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		files[name] = FileRef{hex.EncodeToString(sum[:]), size}
	}
	return j, files
}

func outputWorker(s *outputTestTransport) Worker {
	return Worker{Client: Client{URL: "https://queue.invalid", Clock: RealClock{}, HTTP: &http.Client{Transport: s}, singleAttempt: true, publication: &publicationProgress{}}}
}

func requestTotal(s *outputTestTransport) int {
	n := 0
	for _, count := range s.requests {
		n += count
	}
	return n
}

func TestSmallOutputPublicationRequestsAndResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		j, want := outputFixture(t, 1000, 1000*4096)
		parent := t.TempDir()
		s := newOutputTestTransport()
		s.refuseAt = 350
		w := outputWorker(s)
		step, name := "", ""
		if _, terminal, err := w.publishOutputFiles(context.Background(), j, parent, &step, &name); err == nil || terminal {
			t.Fatalf("capacity refusal lost retry: terminal=%v err=%v", terminal, err)
		}
		root := filepath.Join(attemptPublicationRoot(j, parent), "outputs")
		journal, confirmed, err := openOutputConfirmations(root, want)
		if err != nil {
			t.Fatal(err)
		}
		journal.Close()
		if len(confirmed) < 300 {
			t.Fatal("lost confirmed prefix", len(confirmed))
		}
		before := maps.Clone(s.requests)
		s.refuseAt = 0
		w = outputWorker(s) // New worker/client, no in-memory progress retained.
		got, terminal, err := w.publishOutputFiles(context.Background(), j, parent, &step, &name)
		if err != nil || terminal || !maps.Equal(got, want) || len(s.blobs) != 1000 {
			t.Fatalf("restart: files=%d blobs=%d terminal=%v err=%v", len(got), len(s.blobs), terminal, err)
		}
		for hash := range confirmed {
			key := "HEAD /api/files/blobs/" + hash
			if s.requests[key] != before[key] {
				t.Fatal("re-HEAD confirmed blob", hash)
			}
		}
		if len(s.preflights) != 2 || len(s.preflights[1]) != 1000-len(confirmed) {
			t.Fatal("preflight re-walked confirmed manifest")
		}
		if n := s.requests["POST /api/files/uploads/"]; n > 1000+s.peak {
			t.Fatalf("request amplification: POSTs=%d peak=%d", n, s.peak)
		}
		for key, count := range s.requests {
			if strings.HasPrefix(key, "HEAD ") && count != 0 {
				t.Fatal("fresh/resumed confirmed output needed HEAD", key, count)
			}
		}
		t.Logf("1000 files: requests=%d preflights=%d peak_inflight=%d durable_prefix=%d", requestTotal(s), len(s.preflights), s.peak, len(confirmed))
	})
}

func TestSmallOutputPublicationThroughput(t *testing.T) {
	// Same files, link and per-request service for the serial predecessor and
	// the real optimized dispatcher. The issue's exact representative byte total.
	for _, fixture := range []struct {
		count int
		bytes int64
	}{{1000, 4_096_000}, {1701, 666_584_721}} {
		t.Run(fmt.Sprint(fixture.count), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				j, files := outputFixture(t, fixture.count, fixture.bytes)
				baseline := newOutputTestTransport()
				old := outputWorker(baseline)
				start := time.Now()
				if err := old.Client.PreflightFileStore(context.Background(), files); err != nil {
					t.Fatal(err)
				}
				for _, name := range slices.Sorted(maps.Keys(files)) {
					if err := old.Client.UploadFile(context.Background(), filepath.Join(j.Spec.Run.Workdir, name), files[name]); err != nil {
						t.Fatal(err)
					}
				}
				oldTime := time.Since(start)
				if n := requestTotal(baseline); n != 6*fixture.count+1 {
					t.Fatalf("baseline requests=%d want=%d", n, 6*fixture.count+1)
				}
				s := newOutputTestTransport()
				w := outputWorker(s)
				start = time.Now()
				step, name := "", ""
				got, terminal, err := w.publishOutputFiles(context.Background(), j, t.TempDir(), &step, &name)
				elapsed := time.Since(start)
				if err != nil || terminal || !maps.Equal(got, files) {
					t.Fatalf("publication: terminal=%v err=%v", terminal, err)
				}
				if n := requestTotal(s); n != fixture.count+1 {
					t.Fatalf("optimized requests=%d want=%d", n, fixture.count+1)
				}
				if elapsed >= oldTime/4 {
					t.Fatalf("no material speedup: old=%s new=%s", oldTime, elapsed)
				}
				utilization := float64(fixture.bytes) / elapsed.Seconds() / float64(s.linkBytesPerSecond)
				if fixture.count == 1701 && utilization < .9 {
					t.Fatalf("link utilization %.3f: elapsed=%s peak=%d", utilization, elapsed, s.peak)
				}
				t.Logf("files=%d bytes=%d baseline_requests=%d optimized_requests=%d baseline_time=%s optimized_time=%s baseline_Bps=%.0f optimized_Bps=%.0f link_utilization=%.3f peak_inflight=%d", fixture.count, fixture.bytes, requestTotal(baseline), requestTotal(s), oldTime, elapsed, float64(fixture.bytes)/oldTime.Seconds(), float64(fixture.bytes)/elapsed.Seconds(), utilization, s.peak)
			})
		})
	}
}

func TestSmallOutputPublicationLostReplyAndLegacy(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				j, want := outputFixture(t, 1, 4096)
				s := newOutputTestTransport()
				s.legacy, s.lostReply = legacy, !legacy
				parent := t.TempDir()
				step, name := "", ""
				w := outputWorker(s)
				_, _, err := w.publishOutputFiles(context.Background(), j, parent, &step, &name)
				if !legacy && err == nil {
					t.Fatal("lost reply was treated as confirmed")
				}
				got, terminal, err := outputWorker(s).publishOutputFiles(context.Background(), j, parent, &step, &name)
				if err != nil || terminal || !maps.Equal(got, want) {
					t.Fatal("reconciliation failed", terminal, err)
				}
				if s.requests["PATCH /api/files/uploads/"+want["out/0000.bin"].SHA256] != map[bool]int{false: 0, true: 1}[legacy] {
					t.Fatal("lost ACK replayed bytes or legacy queue did not reconcile")
				}
			})
		})
	}
}

func TestSmallOutputPublicationChangedSourceAndFence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		j, _ := outputFixture(t, 2, 8192)
		s := newOutputTestTransport()
		w := outputWorker(s)
		parent := t.TempDir()
		step, name := "", ""
		if _, _, err := w.publishOutputFiles(context.Background(), j, parent, &step, &name); err != nil {
			t.Fatal(err)
		}
		requests := requestTotal(s)
		changed := j
		changed.Session = "replacement"
		if _, _, err := w.publishOutputFiles(context.Background(), changed, parent, &step, &name); err == nil {
			t.Fatal("reused another session's progress")
		}
		path := filepath.Join(j.Spec.Run.Workdir, "out/0000.bin")
		st, _ := os.Stat(path)
		if err := os.WriteFile(path, make([]byte, 4096), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, st.ModTime(), st.ModTime()); err != nil {
			t.Fatal(err)
		}
		if _, terminal, err := w.publishOutputFiles(context.Background(), j, parent, &step, &name); err == nil || !terminal {
			t.Fatal("changed source reused confirmed digest", terminal, err)
		}
		if requestTotal(s) != requests {
			t.Fatal("invalid snapshot made remote requests")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := w.Client.uploadOutputFile(ctx, path, creationRef("different")); !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation ignored", err)
		}
	})
}

func TestSmallOutputPublicationDirectorySnapshot(t *testing.T) {
	for _, glob := range []bool{false, true} {
		t.Run(fmt.Sprint(glob), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				j, _ := outputFixture(t, 1, 4096)
				if err := os.Mkdir(filepath.Join(j.Spec.Run.Workdir, "out/empty"), 0700); err != nil {
					t.Fatal(err)
				}
				if glob {
					j.Spec.Outputs = map[string]string{"out/*/*.bin": "results", "out/*.bin": "results"}
				}
				// Give each declaration an initial match while leaving another
				// candidate directory empty; creation there must invalidate reuse.
				if glob {
					if err := os.Mkdir(filepath.Join(j.Spec.Run.Workdir, "out/used"), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "out/used/match.bin"), []byte("matched"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				s := newOutputTestTransport()
				w, parent := outputWorker(s), t.TempDir()
				step, name := "", ""
				if _, _, err := w.publishOutputFiles(context.Background(), j, parent, &step, &name); err != nil {
					t.Fatal(err)
				}
				requests := requestTotal(s)
				if err := os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "out/empty/new.bin"), []byte("new output"), 0600); err != nil {
					t.Fatal(err)
				}
				if _, terminal, err := w.publishOutputFiles(context.Background(), j, parent, &step, &name); err == nil || !terminal {
					t.Fatal("new output silently omitted", terminal, err)
				}
				if requestTotal(s) != requests {
					t.Fatal("changed tree reached remote publication")
				}
			})
		})
	}
}

func TestSmallOutputPublicationCancellationDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		j, want := outputFixture(t, 100, 100*4096)
		s := newOutputTestTransport()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s.cancel, s.cancelAt = cancel, 20
		parent, w := t.TempDir(), outputWorker(s)
		step, name := "", ""
		if _, terminal, err := w.publishOutputFiles(ctx, j, parent, &step, &name); terminal || !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation lost", terminal, err)
		}
		if s.active != 0 || s.requests["POST /api/files/uploads/"] > 20 {
			t.Fatal("publication dispatched or leaked work after cancellation", s.active, s.requests)
		}
		got, terminal, err := outputWorker(s).publishOutputFiles(context.Background(), j, parent, &step, &name)
		if err != nil || terminal || !maps.Equal(got, want) {
			t.Fatal("cancelled publication could not resume", terminal, err)
		}
	})
}
