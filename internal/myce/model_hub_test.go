package myce

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHub serves the two model hub routes the worker uses: revision metadata
// with blob identities, and file downloads. Files ending in .json are small
// (Git blob SHA-1), the rest large (LFS SHA-256). It counts every request.
type fakeHub struct {
	*httptest.Server
	mu       sync.Mutex
	requests map[string]int
	repos    map[string]map[string]string // repo@revision -> file -> content
	serve    map[string]string            // tampered bodies by file name
	status   int                          // non-zero: every request fails with it
	token    string                       // non-empty: required bearer token
	gate     chan struct{}                // non-nil: downloads wait for it
}

func newFakeHub(t *testing.T) *fakeHub {
	h := &fakeHub{requests: map[string]int{}, repos: map[string]map[string]string{}, serve: map[string]string{}}
	h.Server = httptest.NewServer(http.HandlerFunc(h.handle))
	t.Cleanup(h.Close)
	return h
}

func (h *fakeHub) add(repo, revision string, files map[string]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.repos[repo+"@"+revision] = files
}

func (h *fakeHub) count(path string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.requests[path]
}

func (h *fakeHub) total() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, c := range h.requests {
		n += c
	}
	return n
}

func (h *fakeHub) handle(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.requests[r.URL.Path]++
	status, token, gate := h.status, h.token, h.gate
	h.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	if token != "" && r.Header.Get("Authorization") != "Bearer "+token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if rest, ok := strings.CutPrefix(path, "api/models/"); ok {
		repo, revision, ok := strings.Cut(rest, "/revision/")
		h.mu.Lock()
		files, found := h.repos[repo+"@"+revision]
		h.mu.Unlock()
		if !ok || !found || r.URL.Query().Get("blobs") != "true" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var siblings []map[string]any
		for name, content := range files {
			entry := map[string]any{"rfilename": name, "size": len(content)}
			if strings.HasSuffix(name, ".json") {
				sum := sha1.Sum([]byte(fmt.Sprintf("blob %d\x00%s", len(content), content)))
				entry["blobId"] = hex.EncodeToString(sum[:])
			} else {
				sum := sha256.Sum256([]byte(content))
				entry["blobId"] = strings.Repeat("a", 40)
				entry["lfs"] = map[string]any{"sha256": hex.EncodeToString(sum[:]), "size": len(content)}
			}
			siblings = append(siblings, entry)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sha": revision, "siblings": siblings})
		return
	}
	parts := strings.SplitN(path, "/", 5) // owner/name/resolve/revision/file
	if len(parts) != 5 || parts[2] != "resolve" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	h.mu.Lock()
	content, found := h.repos[parts[0]+"/"+parts[1]+"@"+parts[3]][parts[4]]
	if tampered, ok := h.serve[parts[4]]; ok {
		content = tampered
	}
	h.mu.Unlock()
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if gate != nil && !strings.HasSuffix(parts[4], ".json") {
		<-gate
	}
	_, _ = w.Write([]byte(content))
}

func hubWorker(t *testing.T, cache, hub string) *Worker {
	t.Helper()
	w := &Worker{ModelHubURL: hub, FileCacheRoot: cache, FileCacheLimit: 16, Clock: &fakeClock{now: time.Unix(1000, 0)}}
	if err := w.initFileTransfers(context.Background(), &workerStore{dir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeSharedEntry(w.modelSnapshots.cache.Root) })
	return w
}

func acquireOnce(t *testing.T, w *Worker, j Job) (string, error) {
	t.Helper()
	paths, releases, err := w.acquireModelSnapshots(context.Background(), j)
	for _, release := range releases {
		release()
	}
	return paths["MYCE_MODEL_BASE"], err
}

const tinyWeightsPath = "/org/tiny/resolve/" + tinyRevision + "/model.safetensors"

// A host without the snapshot fetches it once into its own cache, two jobs on
// that host share the download, and once held (also after a worker restart)
// nothing is downloaded or even listed again.
func TestModelHubFetchesUncachedSnapshotOnce(t *testing.T) {
	hub := newFakeHub(t)
	hub.add("org/tiny", tinyRevision, map[string]string{"model.safetensors": tinyWeights, "config.json": tinyConfig})
	cache := filepath.Join(t.TempDir(), "file-cache")
	w := hubWorker(t, cache, hub.URL)

	hub.gate = make(chan struct{})
	var wg sync.WaitGroup
	paths := make([]string, 2)
	errs := make([]error, 2)
	var started atomic.Int32
	for i := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started.Add(1)
			paths[i], errs[i] = acquireOnce(t, w, snapshotJob("org/tiny"))
		}()
	}
	for started.Load() < 2 || hub.count(tinyWeightsPath) == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // Let the second job reach the shared lock.
	close(hub.gate)
	wg.Wait()
	for i := range paths {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if data, _ := os.ReadFile(filepath.Join(paths[i], "model.safetensors")); string(data) != tinyWeights {
			t.Fatalf("job %d sees %q", i, data)
		}
		if data, _ := os.ReadFile(filepath.Join(paths[i], "config.json")); string(data) != tinyConfig {
			t.Fatalf("job %d config %q", i, data)
		}
	}
	if paths[0] != paths[1] || hub.count(tinyWeightsPath) != 1 {
		t.Fatalf("two jobs did not share one download: paths %v downloads %d", paths, hub.count(tinyWeightsPath))
	}
	id := snapshotIdentity(snapshotJob("org/tiny").Spec.ModelSnapshots["base"])
	held, sizes := w.modelIndex.report(w.modelSnapshots.readyKeys())
	if len(held) != 1 || held[0] != id || sizes[id] != uint64(len(tinyWeights)+len(tinyConfig)) {
		t.Fatalf("held snapshot not reported: %v %v", held, sizes)
	}
	if entries, _ := filepath.Glob(filepath.Join(w.modelSnapshots.cache.Root, "hub-fetch-*")); len(entries) != 0 {
		t.Fatalf("fetch scratch left behind: %v", entries)
	}

	before := hub.total()
	if _, err := acquireOnce(t, w, snapshotJob("org/tiny")); err != nil {
		t.Fatal(err)
	}
	restarted := hubWorker(t, cache, hub.URL)
	if held, _ := restarted.modelIndex.report(restarted.modelSnapshots.readyKeys()); len(held) != 1 {
		t.Fatalf("restarted worker forgot its snapshot: %v", held)
	}
	if path, err := acquireOnce(t, restarted, snapshotJob("org/tiny")); err != nil || path != paths[0] {
		t.Fatal(path, err)
	}
	if hub.total() != before {
		t.Fatalf("held snapshot contacted the hub: %d requests", hub.total()-before)
	}
}

// A revision the hub lacks, refused access or bytes that do not match the
// hub's identity fail the job clearly; server and network faults are this
// host's and requeue the attempt.
func TestModelHubFailures(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		setup      func(*fakeHub)
		retryable  bool
	}{
		{"unknown revision", "model_snapshot_revision", func(*fakeHub) {}, false},
		{"gated without token", "model_snapshot_access", func(h *fakeHub) {
			h.add("org/tiny", tinyRevision, map[string]string{"model.safetensors": tinyWeights})
			h.token = "secret"
		}, false},
		{"tampered bytes", "model_snapshot_mismatch", func(h *fakeHub) {
			h.add("org/tiny", tinyRevision, map[string]string{"model.safetensors": tinyWeights})
			h.serve["model.safetensors"] = strings.Repeat("x", len(tinyWeights))
		}, false},
		{"server fault", "", func(h *fakeHub) { h.status = http.StatusBadGateway }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub := newFakeHub(t)
			tc.setup(hub)
			w := hubWorker(t, filepath.Join(t.TempDir(), "file-cache"), hub.URL)
			_, err := acquireOnce(t, w, snapshotJob("org/tiny"))
			if err == nil {
				t.Fatal("refusal ignored")
			}
			if cause := runtimeDiagnostic(err); cause == nil || cause.Predicate != tc.want {
				t.Fatalf("cause %+v, want predicate %q: %v", cause, tc.want, err)
			}
			if stagingRetryable(err) != tc.retryable {
				t.Fatalf("retryable=%v, want %v", stagingRetryable(err), tc.retryable)
			}
		})
	}
	t.Run("gated with local secret", func(t *testing.T) {
		hub := newFakeHub(t)
		hub.add("org/tiny", tinyRevision, map[string]string{"model.safetensors": tinyWeights})
		hub.token = "hub-secret-value"
		secrets := filepath.Join(t.TempDir(), "secrets")
		if err := os.Mkdir(secrets, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(secrets, modelHubSecret), []byte(hub.token+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		w := hubWorker(t, filepath.Join(t.TempDir(), "file-cache"), hub.URL)
		w.SecretRoot = secrets
		if _, err := acquireOnce(t, w, snapshotJob("org/tiny")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("plain HTTP off loopback", func(t *testing.T) {
		if _, err := (Worker{ModelHubURL: "http://hub.example"}).modelHub(); err == nil {
			t.Fatal("credential could travel in clear text")
		}
	})
}

// Admission charges an uncached snapshot's learned size against the host's
// file-cache volume, so a host without room is not chosen and the job waits
// with a disk reason; a host that holds it is not charged.
func TestModelSnapshotDiskAdmission(t *testing.T) {
	q, c := fixture(t)
	snapshot := ModelSnapshot{Repo: "org/tiny", Revision: tinyRevision}
	id := snapshotIdentity(snapshot)
	snapshotHost := func(id, host string, free uint64) Machine {
		m := reportDisk(t, machine(id, "fake", host), c, id+"-volume", []string{"file-cache"}, 100<<30, free)
		m.Capabilities = append(m.Capabilities, modelSnapshotsCapability)
		return m
	}
	cold := snapshotHost("cold", "cold-host", 30<<30)
	cold.ModelSnapshotBytes = map[string]uint64{id: 40 << 30} // Learned when it refused.
	check(t, q, cold, nil)
	s := spec("experiment")
	s.ModelSnapshots = map[string]ModelSnapshot{"base": snapshot}
	j := submit(t, q, s)
	if got := check(t, q, cold, nil).Job; got != nil {
		t.Fatalf("host short of disk was chosen: %+v", got)
	}
	held := job(t, q, j.ID)
	if held.State != "queued" || held.DiagnosticCode != "disk_headroom" || !strings.Contains(held.Reason, "file-cache disk: 40.0 GiB needed") {
		t.Fatalf("waiting reason does not name disk: code=%s reason=%s", held.DiagnosticCode, held.Reason)
	}
	holder := snapshotHost("holder", "holder-host", 30<<30)
	holder.ModelSnapshots = []string{id}
	if got := check(t, q, holder, nil).Job; got == nil || got.ID != j.ID {
		t.Fatalf("host holding the snapshot was charged: %+v", job(t, q, j.ID))
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if !(affinity(q.s.Jobs[j.ID], q.s.Machines["holder"]) > affinity(q.s.Jobs[j.ID], q.s.Machines["cold"])) {
		t.Fatal("placement does not prefer the host holding the snapshot")
	}
}
