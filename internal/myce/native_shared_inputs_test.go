//go:build linux || darwin

package myce

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeIsolationSharedInputs(t *testing.T) {
	for _, group := range []bool{false, true} {
		name := "single"
		if group {
			name = "group"
		}
		t.Run(name, func(t *testing.T) {
			w, root, j := nativeCompatibilityFixture(t, "")
			if err := w.initFileTransfers(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = removeSharedEntry(w.sharedInputs.cache.Root) })
			const script = "#!/bin/sh\nprintf 'shared executable works\\n'\n"
			var archive bytes.Buffer
			tw := tar.NewWriter(&archive)
			if err := tw.WriteHeader(&tar.Header{Name: "bin/run", Mode: 0755, Size: int64(len(script))}); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write([]byte(script)); err != nil {
				t.Fatal(err)
			}
			if err := tw.Close(); err != nil {
				t.Fatal(err)
			}
			ref := sharedFixtureFile(t, w.CheckpointRoot, "runtime.tar", archive.String())
			input := SharedInput{NAS: "runtime.tar", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes, Archive: "tar", UnpackedBytes: int64(len(script))}
			j.Spec.SharedInputs = map[string]SharedInput{"runtime": input}
			_, release, err := w.sharedInputs.Acquire(context.Background(), input, w.CheckpointRoot)
			if err != nil {
				t.Fatal("stage verified shared fixture:", err)
			}
			release()
			// Authored names must not add grants: only acquired input payloads do.
			undeclared := filepath.Join(w.sharedInputs.cache.Root, "sibling", "data", "secret")
			nativeCompatibilityWrite(t, undeclared, "synthetic unrelated shared data")
			j.Spec.Run.Env["UNDECLARED_SHARED_PATH"] = undeclared
			j.Spec.Run.Command = []string{"/bin/sh", "-ec", `
"$MYCE_INPUT_RUNTIME/bin/run" > shared-result.txt
if cat "$MYCE_INPUT_RUNTIME/../ready.json" 2>/dev/null; then exit 21; fi
if cat "$UNDECLARED_SHARED_PATH" 2>/dev/null; then exit 22; fi
# Linux permits chmod metadata; neither platform may permit content mutation.
chmod u+w "$MYCE_INPUT_RUNTIME/bin/run" 2>/dev/null || true
if (printf changed > "$MYCE_INPUT_RUNTIME/bin/run") 2>/dev/null; then exit 23; fi
`}
			if group {
				j.Spec.Run.MemoryGB = .0625 // Uses the native group policy path.
			}
			p := nativeCompatibilityRun(t, w, root, j, 30*time.Second)
			nativeCompatibilityRead(t, filepath.Join(p.job.Spec.Run.Workdir, "shared-result.txt"), "shared executable works\n")
			nativeCompatibilityRead(t, filepath.Join(w.sharedInputs.cache.Root, input.key(), "data", "bin", "run"), script)
			nativeCompatibilityRead(t, undeclared, "synthetic unrelated shared data")
			var record sharedEntry
			data, err := os.ReadFile(filepath.Join(w.sharedInputs.cache.Root, input.key(), "ready.json"))
			if err != nil || json.Unmarshal(data, &record) != nil || record.Files["bin/run"].SHA256 == "" {
				t.Errorf("cache bookkeeping changed: %v", err)
			}
		})
	}
}
