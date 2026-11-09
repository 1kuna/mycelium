package myce

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"os"

	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Samba/CIFS mfsymlinks encode an otherwise ordinary symlink as a 1067-byte
// regular file on volumes without symlink support. No SMB server is needed.
func emulateModelSymlink(t *testing.T, file string) string {
	t.Helper()
	target, err := os.Readlink(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf("XSym\n%04d\n%x\n%s", len(target), md5.Sum([]byte(target)), target)
	content += strings.Repeat(" ", 1067-len(content))
	if err := os.WriteFile(file, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return target
}

func TestModelSnapshotEmulatedSymlinkRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, predicate string
		change          func(*testing.T, string, string)
	}{
		{"changed blob", "model_snapshot_mismatch", func(t *testing.T, file, blob string) {
			// Equal size makes this a hash failure, not merely a size failure.
			if err := os.WriteFile(blob, []byte(strings.Repeat("x", len(tinyWeights))), 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{"broken format checksum", "model_snapshot_mismatch", func(t *testing.T, file, blob string) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			data[10] = 'z'
			if err := os.WriteFile(file, data, 0644); err != nil {
				t.Fatal(err)
			}
		}},
		{"escape", "model_snapshot_escape", func(t *testing.T, file, blob string) {
			if err := os.Remove(file); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("../../../../private", file); err != nil {
				t.Fatal(err)
			}
			emulateModelSymlink(t, file)
		}},
		{"missing blob", "model_snapshot_mismatch", func(t *testing.T, file, blob string) {
			if err := os.Remove(blob); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			repo := hubRepo(t, root, "org/tiny", tinyRevision, map[string]string{"model.safetensors": tinyWeights})
			file := filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors")
			target := emulateModelSymlink(t, file)
			tc.change(t, file, filepath.Join(filepath.Dir(file), target))
			w := snapshotWorker(t, root, &fakeClock{now: time.Unix(1000, 0)})
			paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
			for _, release := range releases {
				release()
			}
			if err == nil || runtimeDiagnostic(err).Predicate != tc.predicate || len(paths) != 0 {
				t.Fatalf("unverified bytes exposed: paths=%v diagnostic=%+v error=%v", paths, runtimeDiagnostic(err), err)
			}
		})
	}
}

func TestModelSnapshotEmulatedSymlinkNestedStub(t *testing.T) {
	root := t.TempDir()
	repo := tinyModel(t, root)
	file := filepath.Join(repo, "snapshots", tinyRevision, "model.safetensors")
	target := emulateModelSymlink(t, file)
	stub, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	// A second content-addressed file is itself an emulated link. Its hash is
	// valid for the stub, so copy verification alone cannot reject these bytes.
	sum := sha256.Sum256(stub)
	nested := filepath.Join(repo, "blobs", fmt.Sprintf("%x", sum))
	if err := os.WriteFile(nested, stub, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(filepath.Dir(target), filepath.Base(nested)), file); err != nil {
		t.Fatal(err)
	}
	emulateModelSymlink(t, file)
	w := snapshotWorker(t, root, &fakeClock{now: time.Unix(1000, 0)})
	paths, releases, err := w.acquireModelSnapshots(context.Background(), snapshotJob("org/tiny"))
	for _, release := range releases {
		release()
	}
	if err == nil || runtimeDiagnostic(err).Predicate != "model_snapshot_mismatch" || len(paths) != 0 {
		t.Fatalf("nested stub exposed: paths=%v error=%v", paths, err)
	}
}
