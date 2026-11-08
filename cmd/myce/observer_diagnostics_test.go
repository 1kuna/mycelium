package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestObserverDiagnosticsAtomicWithoutSync(t *testing.T) {
	original := profileJSONSync
	t.Cleanup(func() { profileJSONSync = original })
	var syncedDirectories []bool
	profileJSONSync = func(f *os.File) error {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		syncedDirectories = append(syncedDirectories, info.IsDir())
		return original(f)
	}
	root := t.TempDir()
	diagnostics := newObserverDiagnostics(root, &waitClock{now: time.Now()})
	if err := diagnostics.record("started", "", nil, true); err != nil {
		t.Fatal(err)
	}
	read := diagnostics.beginRead(1)
	read.phase("batch_read", "fixture-job", nil)
	var status observerDiagnostic
	path := filepath.Join(root, "observer.status")
	if err := readProfileJSON(path, &status); err != nil || status.Readers[read.id].Phase != "batch_read" {
		t.Fatalf("missing complete diagnostic snapshot: %+v %v", status, err)
	}
	read.done()
	if err := diagnostics.record("waiting", "", nil, false); err != nil {
		t.Fatal(err)
	}
	if err := readProfileJSON(path, &status); err != nil || status.Phase != "waiting" || len(status.Readers) != 0 {
		t.Fatalf("missing replacement diagnostic snapshot: %+v %v", status, err)
	}
	if len(syncedDirectories) != 0 {
		t.Fatalf("reconstructible diagnostics performed durable syncs: %v", syncedDirectories)
	}
	// The same storage layer must still sync both receipt bytes and the renamed
	// directory entry before acknowledging an arm or completed delivery.
	if err := saveProfileJSON(filepath.Join(root, "receipt.json"), profileReceipt{Observation: "armed"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(syncedDirectories, []bool{false, true}) {
		t.Fatalf("receipt lost file/directory durability: %v", syncedDirectories)
	}
}
