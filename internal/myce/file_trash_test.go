package myce

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

// A collector that stops between a move and its verification leaves trash
// entries. The next pass restores the referenced one and purges the other.
func TestFileTrashSweepAfterInterruptedPass(t *testing.T) {
	q, clock, c := retentionFixture(t)
	_, orphan := retentionJobInput(t, c, "moved before the collector stopped")
	_, input := retentionJobInput(t, c, "referenced input moved before the collector stopped")
	s := spec("experiment")
	s.Files = FileManifest{"input": input}
	submit(t, q, s)
	clock.Advance(testFileRetention + time.Nanosecond)
	for _, ref := range []FileRef{orphan, input} {
		if _, err := q.files.trashBlob(ref.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(input)); err != nil {
		t.Fatal("referenced bytes were not restored", err)
	}
	if _, err := os.Stat(q.files.blob(orphan)); !os.IsNotExist(err) {
		t.Fatal("unreferenced bytes were restored", err)
	}
	if entries, err := q.files.trashEntries(); err != nil || len(entries) != 0 {
		t.Fatal("trash entries remain", entries, err)
	}
}

// #2117: a collector stalled in a store syscall holds no lock a plain submit needs.
func TestFileTrashBlockedMoveDoesNotBlockSubmit(t *testing.T) {
	q, clock, c := retentionFixture(t)
	_, orphan := retentionJobInput(t, c, "collected while the store hangs")
	clock.Advance(testFileRetention + time.Nanosecond)
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	q.files.beforeTrash = func(string) {
		once.Do(func() { close(blocked) })
		<-release
	}
	done := make(chan error, 1)
	go func() { done <- q.reclaimJobFilesContext(context.Background()) }()
	<-blocked
	submitted := make(chan error, 1)
	go func() {
		_, err := q.Submit(spec("experiment"))
		submitted <- err
	}()
	select {
	case err := <-submitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("a plain submit waited for the collector's store syscall")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(orphan)); !os.IsNotExist(err) {
		t.Fatal("unreferenced bytes were not purged", err)
	}
}
