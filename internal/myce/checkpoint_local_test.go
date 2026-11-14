package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func localCheckpointAssignment(t *testing.T, q *Queue) Job {
	t.Helper()
	s := spec("experiment")
	s.Advanced = &Advanced{CheckpointPolicy: "local_first"}
	m := machine("writer", "fake")
	m.Capabilities = append(m.Capabilities, localFirstProtocol)
	check(t, q, m, nil)
	queued := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != queued.ID || !j.FileTransport || !j.Spec.localFirst() {
		t.Fatal("local checkpoint assignment missing", j)
	}
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
	if err := os.MkdirAll(attemptCheckpoint(*j), 0700); err != nil {
		t.Fatal(err)
	}
	return *j
}
func localGeneration(t *testing.T, j Job, generation int64) FileCheckpoint {
	t.Helper()
	dir := attemptCheckpoint(j)
	data, _ := json.Marshal(map[string]any{"myce_checkpoint": map[string]any{"generation": generation}, "value": generation})
	name := "generation-" + strconv.FormatInt(generation, 10) + ".json"
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	marker, _ := json.Marshal(checkpointGeneration{Protocol: localFirstProtocol, Checkpoint: FileCheckpoint{Generation: generation, Files: FileManifest{name: ref}}})
	if err = os.WriteFile(filepath.Join(dir, generationMarker), marker, 0600); err != nil {
		t.Fatal(err)
	}
	stage, save, err := captureLocalFirst(dir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(stage)
	return save
}
func localWorker(t *testing.T, q *Queue, j Job) Worker {
	return Worker{Client: deliveryClient(t, q), Machine: machine(j.Machine, "fake")}
}
func TestLocalFirstTornAndUnsupportedGenerations(t *testing.T) {
	q, _ := fixture(t)
	j := localCheckpointAssignment(t, q)
	w := localWorker(t, q, j)
	save := localGeneration(t, j, 1)
	if err := os.WriteFile(filepath.Join(attemptCheckpoint(j), "generation-1.json"), []byte("torn"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err == nil {
		t.Fatal("torn bytes committed")
	}
	if bestFileCheckpoint(job(t, q, j.ID)) != nil {
		t.Fatal("partial generation became portable")
	}
	localGeneration(t, j, 1)
	// Arbitrary in-progress files are outside the manifest and cannot enter it.
	os.WriteFile(filepath.Join(attemptCheckpoint(j), "mutable.bin"), []byte("half"), 0600)
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if got := bestFileCheckpoint(job(t, q, j.ID)); len(got.Files) != 2 || got.Generation != save.Generation {
		t.Fatal(got)
	}
	os.Remove(filepath.Join(attemptCheckpoint(j), generationMarker))
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err == nil {
		t.Fatal("legacy/missing marker silently substituted")
	}
}
func TestLocalFirstRealWriterBackpressureAndMigration(t *testing.T) {
	q, _ := fixture(t)
	j := localCheckpointAssignment(t, q)
	w := localWorker(t, q, j)
	// Python performs real write/flush/close/rename. Save must stay blocked until
	// the durable current-execution commit is acknowledged by the worker.
	var output bytes.Buffer
	cmd := exec.Command("python3", "-c", "from mycelium import Checkpoint; c=Checkpoint(); c.save(7); print('accepted', flush=True)")
	pythonPath, _ := filepath.Abs("../../clients/python/src")
	cmd.Env = append(os.Environ(), "PYTHONPATH="+pythonPath, "MYCE_CHECKPOINT_DIR="+attemptCheckpoint(j), "MYCE_CHECKPOINT_PROTOCOL="+localFirstProtocol)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	finished := false
	t.Cleanup(func() {
		if !finished {
			cmd.Process.Kill()
			<-done
		}
	})
	await(t, func() bool {
		_, err := os.Stat(filepath.Join(attemptCheckpoint(j), generationMarker))
		return err == nil
	})
	select {
	case err := <-done:
		finished = true
		t.Fatal("save returned before acceptance", err, output.String())
	default:
	}
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatal(err, output.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("save failed to consume queue ack")
	}
	accepted := job(t, q, j.ID)
	if accepted.CheckpointMirrors == nil || len(accepted.CheckpointMirrors) != 1 {
		t.Fatal("no mirror custody")
	}
	// A second compatible worker stages only verified queue bytes, with no NAS.
	next := j
	next.Attempt++
	next.Checkpoint = filepath.Join(t.TempDir(), "other-host")
	next.ResumeFiles = bestFileCheckpoint(accepted)
	other := Worker{fileCache: &FileCache{Root: t.TempDir(), Limit: 1 << 20, Client: w.Client, Clock: q.clock}}
	dest, err := other.prepareFileCheckpoint(next, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	read := exec.Command("python3", "-c", "from mycelium import Checkpoint; assert Checkpoint().load()==7")
	read.Env = append(os.Environ(), "PYTHONPATH="+pythonPath, "MYCE_CHECKPOINT_DIR="+dest, "MYCE_CHECKPOINT_PROTOCOL="+localFirstProtocol)
	if b, err := read.CombinedOutput(); err != nil {
		t.Fatal(err, string(b))
	}
}
func TestLocalFirstMirrorStallRestartReplayAndExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	j := localCheckpointAssignment(t, q)
	w := localWorker(t, q, j)
	localGeneration(t, j, 1)
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	blocked, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	q.deliveryPublish = func(j Job, s *FileStore, root string, progress func()) error {
		if calls.Add(1) == 1 {
			close(blocked)
			<-release
		}
		return publishOutputDelivery(j, s, root, progress)
	}
	advanceDelivery(t, q)
	<-blocked
	for i := 0; i < 3; i++ {
		clock.Advance(time.Hour)
		advanceDelivery(t, q)
	}
	if calls.Load() != 1 {
		t.Fatal("overlapping stalled NAS retries")
	}
	// A newer generation commits and is downloadable while NAS is blocked.
	localGeneration(t, j, 2)
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if bestFileCheckpoint(job(t, q, j.ID)).Generation != 2 {
		t.Fatal("NAS stalled queue acceptance")
	}
	close(release)
	await(t, func() bool { advanceDelivery(t, q); return q.deliveryActive == nil })
	q.deliveryPublish = nil
	if err = q.update(func() error {
		for _, m := range q.touchJob(j.ID).CheckpointMirrors {
			m.Delivery.State = "pending"
			m.Delivery.VerifiedAt = nil
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate restart after NAS rename but before its queue ack. Persisted
	// mirrors are replayed through the existing identity/content receipt checks.
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	await(t, func() bool { advanceDelivery(t, q); return !checkpointMirrorsPending(job(t, q, j.ID)) })
	if err = q.Complete(result(&j, true)); err != nil {
		t.Fatal(err)
	}
	q.fileRetention = DefaultCheckpointExpiryGrace
	clock.Advance(DefaultCheckpointExpiryGrace - time.Second)
	if err = q.update(func() error { q.maintainFileExpiry(); return nil }); err != nil {
		t.Fatal(err)
	}
	if job(t, q, j.ID).FilesExpired {
		t.Fatal("checkpoint expired early")
	}
	clock.Advance(2 * time.Second)
	if err = q.update(func() error { q.maintainFileExpiry(); return nil }); err != nil {
		t.Fatal(err)
	}
	if !job(t, q, j.ID).FilesExpired {
		t.Fatal("checkpoint retained beyond settled grace")
	}
}
func TestLocalFirstLostCommitAckAndStaleAttempt(t *testing.T) {
	q, _ := fixture(t)
	j := localCheckpointAssignment(t, q)
	save := localGeneration(t, j, 1)
	var lost atomic.Bool
	handler := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/files/checkpoint" && !lost.Swap(true) {
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, r)
			if rr.Code != http.StatusOK {
				t.Errorf("commit failed: %s", rr.Body.String())
			}
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(rw, r)
	}))
	defer server.Close()
	w := Worker{Client: Client{URL: server.URL, Token: "test"}, Machine: machine(j.Machine, "fake")}
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err == nil {
		t.Fatal("lost ack not injected")
	}
	if bestFileCheckpoint(job(t, q, j.ID)) == nil {
		t.Fatal("commit not durable")
	}
	// A recreated worker reuses the immutable source after interrupted/lost ack.
	w2 := w
	if _, err := w2.publishFileCheckpoint(j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if len(job(t, q, j.ID).CheckpointMirrors) != 1 {
		t.Fatal("replay duplicated custody")
	}
	if err := q.update(func() error {
		current := q.touchJob(j.ID)
		current.Attempt++
		current.Session = "new-session"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := q.publishCheckpoint(context.Background(), checkpointPublication{Result: result(&j, false), Checkpoint: save})
	if !errors.Is(err, ErrStale) {
		t.Fatal("stale attempt accepted", err)
	}
}

func TestLocalFirstPolicyDefaultAndCapability(t *testing.T) {
	q, _ := fixture(t)
	m := machine("old-worker", "fake")
	j := &Job{Spec: Spec{Resumable: true, CheckpointPolicy: "local_first"}}
	if q.checkpointReady(j, &m) {
		t.Fatal("old worker silently received local policy")
	}
	m.Capabilities = append(m.Capabilities, localFirstProtocol)
	m.CheckpointUnavailable = true
	fakeCheckpointAvailability(t, q, false)
	if !q.checkpointReady(j, &m) {
		t.Fatal("NAS stall gated local policy")
	}
	j.Spec.CheckpointPolicy = ""
	if q.checkpointReady(j, &m) {
		t.Fatal("default NAS behavior changed")
	}
	for _, s := range []Spec{{CheckpointPolicy: "unknown"}, {CheckpointPolicy: "local_first"}} {
		if err := validateLocalFirst(s); err == nil {
			t.Fatal("unsupported policy admitted")
		}
	}
}
func TestLocalFirstQueueCapacityAndInterruptedUpload(t *testing.T) {
	q, _ := fixture(t)
	j := localCheckpointAssignment(t, q)
	localGeneration(t, j, 1)
	q.files.limit = 1
	w := localWorker(t, q, j)
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err == nil {
		t.Fatal("queue pressure ignored")
	}
	if _, err := os.Stat(filepath.Join(attemptCheckpoint(j), generationMarker)); err != nil {
		t.Fatal("unique local generation lost", err)
	}
	if _, err := os.Stat(filepath.Join(attemptCheckpoint(j), checkpointAck)); !os.IsNotExist(err) {
		t.Fatal("uncommitted generation acknowledged")
	}
	q.files.limit = 1 << 20
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	localGeneration(t, j, 2)
	var fail atomic.Bool
	handler := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method == "PATCH" && !fail.Swap(true) {
			rw.WriteHeader(400)
			rw.Write([]byte(`{"code":"injected_interruption","error":"interrupted upload"}`))
			return
		}
		handler.ServeHTTP(rw, r)
	}))
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err == nil {
		t.Fatal("upload fault not exercised")
	}
	if bestFileCheckpoint(job(t, q, j.ID)).Generation != 1 {
		t.Fatal("interrupted bytes accepted")
	}
	// Worker restart reconstructs from the durable published marker, not a
	// half-uploaded immutable staging directory or an unsynced acknowledgement.
	restarted := Worker{Client: deliveryClient(t, q), Machine: w.Machine}
	if _, err := restarted.publishFileCheckpoint(j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if bestFileCheckpoint(job(t, q, j.ID)).Generation != 2 {
		t.Fatal("restart failed to commit newest generation")
	}
}

func TestLocalFirstPlannedStopAndLossResumeAccepted(t *testing.T) {
	for _, loss := range []bool{false, true} {
		t.Run(strconv.FormatBool(loss), func(t *testing.T) {
			q, clock := fixture(t)
			j := localCheckpointAssignment(t, q)
			w := localWorker(t, q, j)
			localGeneration(t, j, 1)
			if _, err := w.publishFileCheckpoint(j, t.TempDir()); err != nil {
				t.Fatal(err)
			}
			localGeneration(t, j, 2) // Local only: never rank this ahead of accepted bytes.
			clock.Advance(time.Minute)
			if err := q.update(func() error { q.requeue(q.touchJob(j.ID), "planned stop", loss); return nil }); err != nil {
				t.Fatal(err)
			}
			m := machine("next-worker", "fake")
			m.Capabilities = append(m.Capabilities, localFirstProtocol)
			next := check(t, q, m, nil).Job
			if next == nil || next.ResumeFiles == nil || next.ResumeFiles.Generation != 1 {
				t.Fatal("resume guessed unpublished progress", next)
			}
			if _, err := w.publishFileCheckpoint(j, t.TempDir()); !stale(err) {
				t.Fatal("old attempt committed after replacement", err)
			}
		})
	}
}
func TestLocalFirstPendingMirrorPinsAndCheckpointCollection(t *testing.T) {
	q, clock := fixture(t)
	q.fileRetention = time.Hour
	j := localCheckpointAssignment(t, q)
	w := localWorker(t, q, j)
	save := localGeneration(t, j, 1)
	if _, err := w.publishFileCheckpoint(j, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := q.Complete(result(&j, true)); err != nil {
		t.Fatal(err)
	}
	request := AttemptFilesRequest{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Checkpoint: true}
	clock.Advance(DefaultCheckpointExpiryGrace + time.Hour)
	if err := q.update(func() error { q.maintainFileExpiry(); return nil }); err != nil {
		t.Fatal(err)
	}
	if job(t, q, j.ID).FilesExpired || q.attemptFilesDisposition(request).Reclaim {
		t.Fatal("pending mirror custody expired")
	}
	for _, ref := range save.Files {
		if err := q.files.verify(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool { advanceDelivery(t, q); return !checkpointMirrorsPending(job(t, q, j.ID)) })
	if err := q.update(func() error { q.maintainFileExpiry(); return nil }); err != nil {
		t.Fatal(err)
	}
	if !job(t, q, j.ID).FilesExpired || !q.attemptFilesDisposition(request).Reclaim {
		t.Fatal("settled checkpoint grace not collected")
	}
}

func TestLocalFirstUnknownStorageRefuses(t *testing.T) {
	j := Job{Spec: Spec{CheckpointPolicy: "local_first"}, Checkpoint: t.TempDir(), Attempt: 1}
	_, err := (Worker{}).prepareFileCheckpoint(j, filepath.Join(t.TempDir(), "missing-storage"))
	var refused *HTTPError
	if !errors.As(err, &refused) || refused.Code != "checkpoint_storage_unsupported" {
		t.Fatal("unknown storage substituted", err)
	}
}
