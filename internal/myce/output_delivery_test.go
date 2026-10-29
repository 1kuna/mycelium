package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func deliveryClient(t *testing.T, q *Queue) Client {
	t.Helper()
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(api.Close)
	return Client{URL: api.URL, Token: "test"}
}

func asyncDeliverySpec() Spec {
	s := spec("experiment")
	s.Resumable = false
	s.OutputDelivery = asyncNAS
	s.Disk = &DiskNeeds{OutputBytes: 1024}
	s.Outputs = map[string]string{"out.txt": "results"}
	return s
}

func deliveryAssignment(t *testing.T, q *Queue) *Job {
	q.deliveryRoot = q.root
	t.Helper()
	m := machine("publisher", "fake")
	check(t, q, m, nil)
	j := submit(t, q, asyncDeliverySpec())
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("missing assignment")
	}
	return assigned
}

func deliveryResult(t *testing.T, q *Queue, j *Job, content string) Result {
	t.Helper()
	c := deliveryClient(t, q)
	_, ref := retentionUpload(t, c, content)
	r := result(j, true)
	r.OutputRefs = FileManifest{"out.txt": ref}
	if err := c.Call(context.Background(), "POST", "/api/files/result", r, nil); err != nil {
		t.Fatal(err)
	}
	return r
}

func advanceDelivery(t *testing.T, q *Queue) {
	t.Helper()
	if err := q.advanceOutputDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func awaitDeliveryState(t *testing.T, q *Queue, id, state string) *Job {
	t.Helper()
	await(t, func() bool { advanceDelivery(t, q); return job(t, q, id).Delivery.State == state })
	return job(t, q, id)
}

func TestOutputDeliveryCompletesBeforeNASAndRetriesStall(t *testing.T) {
	q, clock := fixture(t)
	entered, release := make(chan func(), 2), make(chan struct{})
	var calls atomic.Int32
	q.deliveryPublish = func(j Job, s *FileStore, root string, progress func()) error {
		n := calls.Add(1)
		entered <- progress
		if n == 1 {
			<-release
			return errors.New("NAS offline")
		}
		return publishOutputDelivery(j, s, root)
	}
	j := deliveryAssignment(t, q)
	r := deliveryResult(t, q, j, "durable result")
	// A local-only or unverified result is never success.
	missing := r
	missing.OutputRefs = FileManifest{"out.txt": {SHA256: strings.Repeat("f", 64), SizeBytes: 10}}
	if err := q.Complete(missing); err == nil {
		t.Fatal("unreserved result accepted")
	}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	accepted := job(t, q, j.ID)
	if accepted.State != "succeeded" || accepted.Delivery.State != "pending" || calls.Load() != 0 {
		t.Fatalf("completion waited for NAS: %+v", accepted)
	}
	if err := q.Complete(r); err != nil {
		t.Fatal("lost acceptance ack", err)
	}
	c := deliveryClient(t, q)
	dst := filepath.Join(t.TempDir(), "download")
	if err := c.DownloadFile(context.Background(), accepted.OutputRefs["out.txt"], dst); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(dst)
	if string(data) != "durable result" {
		t.Fatal("retrieved wrong bytes")
	}
	advanceDelivery(t, q)
	var progress func()
	select {
	case progress = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("publisher did not start")
	}
	clock.Advance(2 * checkpointAvailabilityStallBudget)
	for range 3 {
		advanceDelivery(t, q)
	}
	if calls.Load() != 1 || !strings.Contains(job(t, q, j.ID).Delivery.Cause, "stalled") {
		t.Fatal("stall lost ownership or status")
	}
	// A long copy that resumes making progress clears the stall and receives
	// a new inactivity window without creating another operation.
	progress()
	advanceDelivery(t, q)
	if d := job(t, q, j.ID).Delivery; d.Cause != "" || d.LastProgressAt == nil || !d.LastProgressAt.Equal(clock.Now()) {
		t.Fatal("progress did not clear stall")
	}
	clock.Advance(checkpointAvailabilityStallBudget - time.Nanosecond)
	advanceDelivery(t, q)
	if job(t, q, j.ID).Delivery.Cause != "" {
		t.Fatal("healthy progress marked stalled")
	}
	// Control and unrelated computation continue while every publication call blocks.
	other := machine("independent", "fake")
	check(t, q, other, nil)
	s := spec("experiment")
	s.Resumable = false
	s.AllowedMachines = []string{other.ID}
	independent := submit(t, q, s)
	run := check(t, q, other, nil).Job
	if run == nil || run.ID != independent.ID {
		t.Fatal("independent job stalled")
	}
	finish(t, q, run, true)
	close(release)
	failed := awaitDeliveryState(t, q, j.ID, "failed")
	if failed.State != "succeeded" || failed.Delivery.VerifiedAt != nil {
		t.Fatal("delivery changed compute outcome")
	}
	clock.Advance(deliveryRetryDelay - time.Nanosecond)
	advanceDelivery(t, q)
	if calls.Load() != 1 {
		t.Fatal("early retry")
	}
	clock.Advance(time.Nanosecond)
	verified := awaitDeliveryState(t, q, j.ID, "verified")
	if calls.Load() != 2 || verified.Delivery.VerifiedAt == nil || !verified.Delivery.VerifiedAt.Equal(clock.Now()) {
		t.Fatal("retry or fake-clock receipt failed")
	}
	if !verified.FinishedAt.Equal(*accepted.FinishedAt) {
		t.Fatal("NAS extended completion timestamp")
	}
}

func TestOutputDeliveryRestartRetentionAndReceiptReplay(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, clock, WithOutputDeliveryRoot(root), WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	j := deliveryAssignment(t, q)
	r := deliveryResult(t, q, j, "restart result")
	if err = q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock, WithOutputDeliveryRoot(root), WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	clock.Advance(DefaultFileRetention + time.Hour)
	// Ordinary expiry/collection cannot expire or unlink pending delivery.
	if err = q.update(func() error { q.maintainFileExpiry(); return nil }); err != nil {
		t.Fatal(err)
	}
	if err = q.reclaimJobFiles(); err != nil {
		t.Fatal(err)
	}
	accepted := job(t, q, j.ID)
	if accepted.FilesExpired || accepted.Delivery.State != "pending" {
		t.Fatal("pending delivery aged out")
	}
	if err = q.files.verify(context.Background(), r.OutputRefs["out.txt"]); err != nil {
		t.Fatal("pending source lost", err)
	}
	// Simulate crash after destination rename but before queue receipt acknowledgement.
	if err = publishOutputDelivery(*accepted, q.files, q.root); err != nil {
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock, WithOutputDeliveryRoot(root), WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	verified := awaitDeliveryState(t, q, j.ID, "verified")
	dest := filepath.Join(root, outputRelative(*j, "out.txt", "results"))
	bytes, err := os.ReadFile(dest)
	if err != nil || string(bytes) != "restart result" {
		t.Fatal("incomplete NAS result", err)
	}
	receiptBytes, err := os.ReadFile(filepath.Join(filepath.Dir(dest), deliveryReceiptName))
	if err != nil {
		t.Fatal(err)
	}
	var receipt deliveryReceipt
	if json.Unmarshal(receiptBytes, &receipt) != nil || receipt.Execution != r.Execution || receipt.ManifestSHA256 != verified.Delivery.ManifestSHA256 {
		t.Fatal("wrong attempt receipt")
	}
	if err = publishOutputDelivery(*verified, q.files, q.root); err != nil {
		t.Fatal("matching replay", err)
	}
	// A correct receipt alone never blesses corrupt bytes, even of identical length.
	if err = os.WriteFile(dest, []byte("corrupt result"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = publishOutputDelivery(*verified, q.files, q.root); err == nil {
		t.Fatal("corrupt destination accepted")
	}
	if err = os.WriteFile(dest, bytes, 0600); err != nil {
		t.Fatal(err)
	}
	receipt.Attempt++
	bad, _ := json.Marshal(receipt)
	if err = os.WriteFile(filepath.Join(filepath.Dir(dest), deliveryReceiptName), bad, 0600); err != nil {
		t.Fatal(err)
	}
	if err = publishOutputDelivery(*verified, q.files, q.root); err == nil {
		t.Fatal("conflicting receipt accepted")
	}
	if err = q.reclaimJobFiles(); err != nil {
		t.Fatal(err)
	}
	if err = q.update(func() error { q.maintainFileExpiry(); return nil }); err != nil {
		t.Fatal(err)
	}
	if !job(t, q, j.ID).FilesExpired {
		t.Fatal("verified result never expires under normal retention")
	}
}

func TestOutputDeliveryRejectsStaleAttempt(t *testing.T) {
	q, _ := fixture(t)
	j := deliveryAssignment(t, q)
	r := deliveryResult(t, q, j, "first attempt")
	interrupted := result(j, false)
	interrupted.Interrupted, interrupted.PoliteStop = true, true
	if err := q.Complete(interrupted); err != nil {
		t.Fatal(err)
	}
	next := check(t, q, machine("publisher", "fake"), nil).Job
	if next == nil || next.Attempt == j.Attempt {
		t.Fatal("missing next attempt")
	}
	if err := q.reserveDeliveryResult(context.Background(), r); !errors.Is(err, ErrStale) {
		t.Fatalf("stale declaration: %v", err)
	}
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatalf("stale completion: %v", err)
	}
	current := job(t, q, j.ID)
	if current.Delivery != nil || current.State == "succeeded" {
		t.Fatal("stale result published")
	}
	valid := deliveryResult(t, q, next, "current attempt")
	if err := q.Complete(valid); err != nil {
		t.Fatal(err)
	}
	accepted := job(t, q, j.ID)
	snapshot := cloneJob(accepted)
	snapshot.Attempt--
	snapshot.Delivery.Attempt--
	if sameDelivery(accepted, snapshot) {
		t.Fatal("stale publication receipt accepted")
	}
	awaitDeliveryState(t, q, j.ID, "verified")
	if _, err := os.Stat(filepath.Join(q.root, outputRelative(*j, "out.txt", "results"))); !os.IsNotExist(err) {
		t.Fatal("old attempt delivered", err)
	}
}

func TestOutputDeliveryDefaultAndValidation(t *testing.T) {
	s := asyncDeliverySpec()
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.OutputDelivery = "typo"
	if err := s.Validate(); err == nil {
		t.Fatal("unknown policy accepted")
	}
	s.OutputDelivery = asyncNAS
	s.Disk = nil
	if err := s.Validate(); err == nil {
		t.Fatal("unbudgeted outputs accepted")
	}
	j := Job{Spec: Spec{Outputs: map[string]string{"out.txt": "results"}}, ID: ID(), Attempt: 1}
	if j.storedFileResults() {
		t.Fatal("default result transport changed")
	}
	j.Spec.Run.Workdir = t.TempDir()
	os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "out.txt"), []byte("default"), 0600)
	root := t.TempDir()
	if err := publishJobOutputs(j, root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, outputRelative(j, "out.txt", "results"))); err != nil {
		t.Fatal("default synchronous publication changed", err)
	}
}
