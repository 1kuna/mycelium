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
}

func TestExtensionFilesOverlapNASInputsAndJoinBeforeReturn(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "copy failure"
		}
		t.Run(name, func(t *testing.T) {
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
			var active, peak, count atomic.Int32
			inputs := map[string]string{"request": "request-source", "upstream": "upstream-source"}
			if !fail {
				inputs["third"] = "third-source"
				inputs["fourth"] = "fourth-source"
			}
			go func() {
				defer close(finished)
				err := stageJobFiles(inputs, func(name, source string) error {
					n := active.Add(1)
					defer active.Add(-1)
					for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
					}
					ordinal := count.Add(1)
					entered <- name
					if fail && ordinal == 1 {
						<-release
						close(firstReturned)
						return copyErr
					}
					if fail {
						<-firstReturned
						// Keep the other copy active after its peer fails.
						<-continueCopy
					} else {
						<-release
					}
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
			if fail {
				<-firstReturned
				select {
				case err := <-done:
					t.Fatalf("returned before the remaining copy finished: %v", err)
				case <-time.After(25 * time.Millisecond):
				}
				if _, err := os.Stat(staged); err != nil {
					t.Fatalf("scratch cleaned while a copy still uses it: %v", err)
				}
			}
			finishCopy()
			var err error
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("staging did not finish after all reads were released")
			}
			if peak.Load() != 2 || active.Load() != 0 || count.Load() != int32(len(inputs)) {
				t.Fatalf("copies: peak=%d active=%d completed=%d", peak.Load(), active.Load(), count.Load())
			}
			if fail {
				if !errors.Is(err, copyErr) {
					t.Fatalf("lost source error: %v", err)
				}
				if _, err := os.Stat(staged); !os.IsNotExist(err) {
					t.Fatalf("failed staging scratch remains: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for name, source := range inputs {
					b, err := os.ReadFile(filepath.Join(staged, name))
					if err != nil || string(b) != source {
						t.Fatalf("input %s: %q, %v", name, b, err)
					}
				}
			}
		})
	}
}
