package myce

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOutputDeliveryCapacityPreservesSpoolAndReservations(t *testing.T) {
	q, clock := fixture(t)
	q.files.limit = 16
	q.deliveryRoot = q.root
	c := deliveryClient(t, q)
	s := asyncDeliverySpec()
	s.Disk.OutputBytes = 8
	m := machine("publisher", "fake")
	check(t, q, m, nil)
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("missing assignment")
	}
	if _, err := q.Submit(s); err != nil {
		t.Fatal("second estimate should fit", err)
	}
	if _, err := q.Submit(s); err == nil {
		t.Fatal("unbounded estimated admission")
	}
	// Queued estimates protect space against unrelated new uploads.
	src := filepath.Join(t.TempDir(), "input")
	os.WriteFile(src, []byte("x"), 0600)
	ref, _ := IdentifyFile(src)
	err := c.UploadFile(context.Background(), src, ref)
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != http.StatusInsufficientStorage {
		t.Fatalf("reservation not protected: %v", err)
	}
	assigned.Spec.Run.Workdir = t.TempDir()
	os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("123456789"), 0600)
	w := Worker{Client: c}
	_, err = w.finishFileResult(*assigned, result(assigned, true), t.TempDir())
	if !errors.As(err, &he) || he.Status != http.StatusInsufficientStorage {
		t.Fatalf("underestimated result should wait: %v", err)
	}
	if job(t, q, j.ID).State != "running" {
		t.Fatal("capacity refusal reported success")
	}
	spool := deliverySpoolPath(*assigned)
	if _, err = os.Stat(filepath.Join(spool, "manifest.json")); err != nil {
		t.Fatal("unaccepted spool lost", err)
	}
	// Simulate later operational relief; replay uses immutable spool, not work files.
	q.files.limit = 32
	clock.Advance(time.Minute)
	os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("changed"), 0600)
	r, err := w.finishFileResult(*assigned, result(assigned, true), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Complete(r); err != nil {
		t.Fatal(err)
	}
	accepted := job(t, q, j.ID)
	if accepted.OutputRefs["out.txt"].SizeBytes != 9 || accepted.Delivery.State != "pending" {
		t.Fatal("retry changed immutable result")
	}
	status := q.fileStoreStatus(context.Background())
	if status.ReservedOutputBytes != 8 {
		t.Fatalf("reservation not exchanged/released: %+v", status)
	}
	// Actual pending output bytes remain charged; they cannot be called reclaimable.
	if status.UsedBytes != 9 || status.ReclaimableBytes != 0 {
		t.Fatalf("pending capacity missing: %+v", status)
	}
}

func TestOutputDeliveryBundleHonorsReservation(t *testing.T) {
	q, _ := fixture(t)
	q.deliveryRoot = q.root
	q.files.limit = 16
	s := asyncDeliverySpec()
	s.Disk.OutputBytes = 16
	j := submit(t, q, s)
	files := map[string][]byte{"f": []byte("x")}
	var ref *FileRef
	err := q.files.freezeFiles(&files, &ref, "", nil)
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Status != http.StatusInsufficientStorage {
		t.Fatalf("bundle consumed output reservation: %v", err)
	}
	status := q.fileStoreStatus(context.Background())
	if status.UsedBytes != 0 || status.ReservedOutputBytes != 16 || ref != nil || len(files) != 1 {
		t.Fatalf("refused bundle changed storage: %+v", status)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.files.freezeFiles(&files, &ref, "", nil); err != nil {
		t.Fatal("bundle did not resume after capacity relief", err)
	}
	if status := q.fileStoreStatus(context.Background()); status.UsedBytes != 12 || status.ReservedOutputBytes != 0 {
		t.Fatalf("wrong bundle usage: %+v", status)
	}
}

// Independent review reproduced unreferenced uploaded results without any GC
// owner. Exercise the real worker path, then prove both reclamation and the
// unmarked-import boundary after a lost attempt.
func TestOutputDeliveryAbandonedUploadReclaimed(t *testing.T) {
	q, clock := fixture(t)
	q.deliveryRoot = q.root
	c := deliveryClient(t, q)
	j := deliveryAssignment(t, q)
	j.Spec.Run.Workdir = t.TempDir()
	if err := os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "out.txt"), []byte("12345678"), 0600); err != nil {
		t.Fatal(err)
	}
	w := Worker{Client: c}
	uploaded, err := w.finishFileResult(*j, result(j, true), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, imported := retentionUpload(t, c, "unmarked import")
	interrupted := result(j, false)
	interrupted.Interrupted, interrupted.PoliteStop = true, true
	if err = q.Complete(interrupted); err != nil {
		t.Fatal(err)
	}
	if err = q.reserveDeliveryResult(context.Background(), uploaded); !errors.Is(err, ErrStale) {
		t.Fatalf("abandoned attempt resurrected: %v", err)
	}
	clock.Advance(DefaultFileRetention + filePublicationGrace + time.Hour)
	if err = q.reclaimJobFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(q.files.blob(uploaded.OutputRefs["out.txt"])); !os.IsNotExist(err) {
		t.Fatal("abandoned output permanently retained", err)
	}
	if err = q.files.verify(context.Background(), imported); err != nil {
		t.Fatal("unmarked import reclaimed", err)
	}
}

func TestOutputDeliveryRetryReacquiresCapacity(t *testing.T) {
	q, _ := fixture(t)
	q.deliveryRoot = q.root
	q.files.limit = 16
	c := deliveryClient(t, q)
	m, other := machine("publisher", "fake"), machine("other", "fake")
	check(t, q, m, nil)
	check(t, q, other, nil)
	s := asyncDeliverySpec()
	s.Disk.OutputBytes = 16
	s.AllowedMachines = []string{m.ID}
	first := submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil || j.ID != first.ID {
		t.Fatal("missing initial assignment")
	}
	j.Spec.Run.Workdir = t.TempDir()
	if err := os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "out.txt"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := (Worker{Client: c}).finishFileResult(*j, result(j, true), t.TempDir()); err != nil {
		t.Fatal(err)
	}
	s.Disk.OutputBytes = 15
	s.AllowedMachines = []string{other.ID}
	second := submit(t, q, s)
	interrupted := result(j, false)
	interrupted.Interrupted, interrupted.PoliteStop = true, true
	if err := q.Complete(interrupted); err != nil {
		t.Fatal(err)
	}
	if err := q.admitOutputRetries(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := q.fileStoreStatus(context.Background())
	if status.UsedBytes != 1 || status.ReservedOutputBytes != 15 || status.FreeBytes != 0 {
		t.Fatalf("retry conjured capacity: %+v", status)
	}
	if run := check(t, q, m, nil).Job; run != nil {
		t.Fatal("retry dispatched without reacquiring budget")
	}
	if !strings.Contains(job(t, q, j.ID).Reason, "file_store_capacity") {
		t.Fatal("capacity hold not explained")
	}
	// Clearing the competing reservation still leaves one stored byte; the 16-
	// byte estimate cannot fit until that output expires or capacity increases.
	if err := q.Cancel(second.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.admitOutputRetries(context.Background()); err != nil {
		t.Fatal(err)
	}
	if run := check(t, q, m, nil).Job; run != nil {
		t.Fatal("stored byte omitted from retry admission")
	}
	q.files.limit = 17
	if err := q.admitOutputRetries(context.Background()); err != nil {
		t.Fatal(err)
	}
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != first.ID || run.Attempt != j.Attempt+1 {
		t.Fatal("retry did not resume after capacity relief")
	}
}
