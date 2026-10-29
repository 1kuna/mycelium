package myce

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
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
