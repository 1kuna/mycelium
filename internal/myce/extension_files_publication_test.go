package myce

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// #286: a slow NAS copy/fsync must not serialize independent outputs. All
// active copies must join before the attempt is exposed or failed scratch removed.
func TestExtensionFilesPublicationOverlapsCopiesAndJoins(t *testing.T) {
	for _, fail := range []bool{false, true} {
		label := "success"
		if fail {
			label = "copy failure"
		}
		t.Run(label, func(t *testing.T) {
			root, work := t.TempDir(), t.TempDir()
			j := Job{ID: "job", Attempt: 1, Spec: Spec{Run: RunSpec{Workdir: work}, Outputs: map[string]string{"one": "results", "nested/two": "results"}}}
			for name := range j.Spec.Outputs {
				if err := os.MkdirAll(filepath.Dir(filepath.Join(work, name)), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(work, name), []byte(name), 0600); err != nil {
					t.Fatal(err)
				}
			}
			entered := make(chan string, 2)
			begin := make(chan struct{})
			firstDone := make(chan struct{})
			release := make(chan struct{})
			var once, beginOnce sync.Once
			start := func() { beginOnce.Do(func() { close(begin) }) }
			unblock := func() { once.Do(func() { close(release) }) }
			done := make(chan error, 1)
			finished := make(chan struct{})
			t.Cleanup(func() { start(); unblock(); <-finished })
			copyErr := errors.New("NAS copy failed")
			go func() {
				defer close(finished)
				done <- publishDirectoryJobOutputs(j, root, os.Rename, func(src, dst string) error {
					entered <- dst
					<-begin
					if filepath.Base(src) == "one" {
						defer close(firstDone)
						if fail {
							return copyErr
						}
						return copyJobFileContents(src, dst)
					}
					<-release
					return copyJobFileContents(src, dst)
				})
			}()
			var staged string
			for range 2 {
				select {
				case dst := <-entered:
					if filepath.Base(dst) == "one" {
						staged = filepath.Dir(dst)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("independent output copies did not overlap")
				}
			}
			start()
			<-firstDone
			select {
			case err := <-done:
				t.Fatalf("returned while a copy still owns staging: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			if _, err := os.Stat(staged); err != nil {
				t.Fatal("staging removed before all copies joined", err)
			}
			dest := filepath.Join(root, "results", j.ID, "1")
			if _, err := os.Stat(dest); !os.IsNotExist(err) {
				t.Fatal("attempt exposed before all copies joined", err)
			}
			unblock()
			var err error
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("publication did not finish after copies joined")
			}
			if fail {
				if !errors.Is(err, copyErr) {
					t.Fatalf("lost copy failure: %v", err)
				}
				if _, err := os.Stat(dest); !os.IsNotExist(err) {
					t.Fatal("failed attempt published", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for name := range j.Spec.Outputs {
					b, err := os.ReadFile(filepath.Join(dest, name))
					if err != nil || string(b) != name {
						t.Fatalf("incomplete output %s: %q, %v", name, b, err)
					}
				}
			}
			if _, err := os.Stat(staged); !os.IsNotExist(err) {
				t.Fatal("staging remains after copies joined", err)
			}
		})
	}
}

func TestExtensionFilesPublicationPreservesExistingAttempt(t *testing.T) {
	root, work := t.TempDir(), t.TempDir()
	j := Job{ID: "job", Attempt: 1, Spec: Spec{Run: RunSpec{Workdir: work}, Outputs: map[string]string{"one": "results", "two": "results"}}}
	dest := filepath.Join(root, "results", j.ID, "1")
	if err := os.MkdirAll(dest, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, "existing"), []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	for name := range j.Spec.Outputs {
		if err := os.WriteFile(filepath.Join(work, name), []byte("new"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := publishJobOutputs(j, root); err == nil {
		t.Fatal("publication modified an existing attempt directory")
	}
	files, err := os.ReadDir(dest)
	if err != nil || len(files) != 1 || files[0].Name() != "existing" {
		t.Fatalf("partial publication in retained attempt: %v, %v", files, err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "existing"))
	if err != nil || string(data) != "retained" {
		t.Fatal("existing attempt changed", err)
	}
}

func TestExtensionFilesPublicationRejectsEscapingAncestor(t *testing.T) {
	root, outside, work := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "results")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "output"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	j := Job{ID: "job", Attempt: 1, Spec: Spec{Run: RunSpec{Workdir: work}, Outputs: map[string]string{"output": "results"}}}
	if err := publishJobOutputs(j, root); err == nil {
		t.Fatal("publication followed escaping ancestor")
	}
	files, err := os.ReadDir(outside)
	if err != nil || len(files) != 0 {
		t.Fatalf("publication escaped root: %v, %v", files, err)
	}
}
