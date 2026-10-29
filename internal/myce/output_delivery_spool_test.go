package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOutputDeliverySpoolSurvivesRestartAndDiskRefusal(t *testing.T) {
	j := Job{ID: ID(), Attempt: 2, Machine: "worker", Session: "session", Spec: asyncDeliverySpec()}
	j.Spec.Run.Workdir = t.TempDir()
	path := filepath.Join(j.Spec.Run.Workdir, "out.txt")
	if err := os.WriteFile(path, []byte("immutable output"), 0600); err != nil {
		t.Fatal(err)
	}
	oldProbe := workerDiskProbe
	t.Cleanup(func() { workerDiskProbe = oldProbe })
	workerDiskProbe = func(string) (DiskReading, error) {
		return DiskReading{TotalBytes: 1 << 30, FreeBytes: diskFreeFloor(1<<30) + 1}, nil
	}
	if _, _, err := captureDeliverySpool(j); err == nil {
		t.Fatal("full disk accepted local spool")
	}
	if bytes, err := os.ReadFile(path); err != nil || string(bytes) != "immutable output" {
		t.Fatal("disk refusal lost original output", err)
	}
	workerDiskProbe = oldProbe
	root, files, err := captureDeliverySpool(j)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte("changed after capture"), 0600); err != nil {
		t.Fatal(err)
	}
	// A new capture after restart reuses verified snapshot bytes and manifest.
	replay, again, err := captureDeliverySpool(j)
	if err != nil {
		t.Fatal(err)
	}
	if replay != root || again["out.txt"] != files["out.txt"] {
		t.Fatal("restart changed snapshot")
	}
	if bytes, err := os.ReadFile(filepath.Join(replay, "out.txt")); err != nil || string(bytes) != "immutable output" {
		t.Fatal("spool changed", err)
	}
	if err = os.WriteFile(filepath.Join(root, "out.txt"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = captureDeliverySpool(j); err == nil {
		t.Fatal("corrupt spool reused")
	}
	if err = os.RemoveAll(deliverySpoolPath(j)); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err = captureDeliverySpool(j); !errors.Is(err, errRequiredDeliveryOutput) {
		t.Fatalf("missing required output did not fail: %v", err)
	}
}

func TestOutputDeliverySpoolCleanupUsesPreparedAttempt(t *testing.T) {
	q, clock := fixture(t)
	original := deliveryAssignment(t, q) // Default assignment has no workdir.
	prepared := cloneJob(original)
	prepared.Spec.Run.Workdir = t.TempDir()
	if err := os.WriteFile(filepath.Join(prepared.Spec.Run.Workdir, "out.txt"), []byte("accepted output"), 0600); err != nil {
		t.Fatal(err)
	}
	// A relative cleanup path previously deleted an unrelated directory here.
	t.Chdir(t.TempDir())
	unrelated := filepath.Join(".myce-result-spool", "unrelated")
	if err := os.MkdirAll(filepath.Dir(unrelated), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unrelated, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	w := recoveryWorker(t)
	w.Client, w.Clock, w.Machine = deliveryClient(t, q), clock, machine(prepared.Machine, "fake", prepared.Session)
	w.control = &processControl{}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	r, err := w.finishFileResult(prepared, result(&prepared, true), store.dir)
	if err != nil {
		t.Fatal(err)
	}
	wrong := prepared
	wrong.Attempt++
	wrongResult := r
	wrongResult.Attempt++
	if err := removeAcceptedDeliverySpool(wrong, wrongResult); err == nil {
		t.Fatal("different attempt's spool removed")
	}
	store.state.Job, store.state.Result, store.state.Session = &prepared, &r, prepared.Session
	actor := &attemptSupervisor{store: store, job: *original, worker: w, connected: true, done: make(chan struct{}), wake: make(chan struct{}, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actor.run(ctx)
	_, _, finished, err := actor.snapshot()
	if err != nil || !finished || store.state.Job != nil || job(t, q, prepared.ID).State != "succeeded" {
		t.Fatalf("accepted actor did not finish: finished=%t err=%v state=%+v", finished, err, store.state)
	}
	if _, err := os.Stat(deliverySpoolPath(prepared)); !os.IsNotExist(err) {
		t.Fatal("owned accepted spool retained", err)
	}
	if data, err := os.ReadFile(unrelated); err != nil || string(data) != "keep" {
		t.Fatal("unrelated spool deleted", err)
	}
}
