package myce

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExtensionFilesFailedNASInputsDoNotPublishSnapshot(t *testing.T) {
	root, state, outside := t.TempDir(), t.TempDir(), t.TempDir()
	for _, dir := range []string{root, outside} {
		if err := os.WriteFile(filepath.Join(dir, "input"), []byte("input"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	j := Job{ID: "failed-inputs", Attempt: 1, Spec: Spec{
		Kind: "sample", Inputs: map[string]string{"valid": "input", "invalid": "escape/input"},
	}}
	if err := (Worker{JobFileRoot: root}).prepareExtensionFiles(&j, state); err == nil {
		t.Fatal("input escaping the NAS root was accepted")
	}
	if j.Spec.Run.Workdir != "" {
		t.Fatal("failed inputs exposed a work directory")
	}
	entries, err := os.ReadDir(filepath.Join(state, "job-files", j.ID))
	if err != nil || len(entries) != 0 {
		t.Fatalf("partial snapshot or staging scratch remains: %v, %v", entries, err)
	}

	t.Run("failed-copy-joins-peer-before-cleanup", func(t *testing.T) {
		staged := t.TempDir()
		entered := make(chan string, 4)
		release := make(chan struct{})
		continueCopy := make(chan struct{})
		var releaseOnce, continueOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		finishCopy := func() { continueOnce.Do(func() { close(continueCopy) }) }
		firstReturned := make(chan struct{})
		done := make(chan error, 1)
		finished := make(chan struct{})
		t.Cleanup(func() {
			unblock()
			finishCopy()
			<-finished
		})
		copyErr := errors.New("source read failed")
		var count atomic.Int32
		inputs := map[string]string{"request": "request-source", "upstream": "upstream-source"}

		go func() {
			defer close(finished)
			err := stageJobFiles(inputs, func(name, source string) error {
				ordinal := count.Add(1)
				entered <- name
				if ordinal == 1 {
					<-release
					close(firstReturned)
					return copyErr
				}

				<-firstReturned
				// Keep the other copy active after its peer fails.
				<-continueCopy

				return os.WriteFile(filepath.Join(staged, name), []byte(source), 0600)
			})
			// Models the caller's deferred cleanup of unpublished inputs.
			if err != nil {
				_ = os.RemoveAll(staged)
			}
			done <- err
		}()
		for range 2 {
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("independent NAS input copies did not overlap")
			}
		}
		select {
		case err := <-done:
			t.Fatalf("returned while copies are blocked: %v", err)
		default:
		}
		unblock()

		<-firstReturned
		select {
		case err := <-done:
			t.Fatalf("returned before the remaining copy finished: %v", err)
		case <-time.After(25 * time.Millisecond):
		}
		if _, err := os.Stat(staged); err != nil {
			t.Fatalf("scratch cleaned while a copy still uses it: %v", err)
		}

		finishCopy()
		var err error
		select {
		case err = <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("staging did not finish after all reads were released")
		}

		if !errors.Is(err, copyErr) {
			t.Fatalf("lost source error: %v", err)
		}
		if _, err := os.Stat(staged); !os.IsNotExist(err) {
			t.Fatalf("failed staging scratch remains: %v", err)
		}

	})
}
