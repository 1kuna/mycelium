package myce

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
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
