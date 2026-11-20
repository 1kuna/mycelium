//go:build linux || darwin

package myce

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func init() {
	if len(os.Args) > 1 && os.Args[1] == "_native-bindings-test" {
		if err := nativeBindingsCheck(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

func nativeBindingsCheck() error {
	data, err := os.ReadFile(os.Getenv(inputBindingsVariable))
	if err != nil {
		return errors.New("binding manifest is not readable")
	}
	var bindings inputBindings
	if err := json.Unmarshal(data, &bindings); err != nil {
		return err
	}
	if len(bindings.Inputs) != 771 {
		return errors.New("binding manifest is incomplete")
	}
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "MYCE_INPUT_") || strings.HasPrefix(entry, "MYCE_TOKEN=") {
			return errors.New("large legacy binding environment or token inherited")
		}
	}
	for _, path := range bindings.Inputs {
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "synthetic binding data" {
			return errors.New("verified input is not readable through the binding manifest")
		}
	}
	return os.WriteFile("bindings-ok", []byte("771 inputs verified\n"), 0600)
}

func TestNativeManagedInputBindings(t *testing.T) {
	for _, isolation := range []string{"unsandboxed", "sandboxed"} {
		for _, group := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/group=%t", isolation, group), func(t *testing.T) {
				w, root, j := nativeCompatibilityFixture(t, "")
				if err := w.initFileTransfers(context.Background(), root); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = removeSharedEntry(w.sharedInputs.cache.Root) })
				ref := sharedFixtureFile(t, w.CheckpointRoot, "binding.dat", "synthetic binding data")
				bindings, _ := syntheticInputBindings(771)
				j.Spec.SharedInputs = bindings.Spec.SharedInputs
				for name := range j.Spec.SharedInputs {
					j.Spec.SharedInputs[name] = SharedInput{NAS: "binding.dat", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
				}
				j.Spec.Run.Command = []string{j.Spec.Run.Command[0], "_native-bindings-test"}
				j.Spec.Isolation, j.Spec.Reason = isolation, "synthetic launch regression"
				if group {
					j.Spec.Run.MemoryGB = .0625
				}
				// 771 real cache acquisitions consume about 25s on a loaded
				// laptop before launch; allow room for the grouped handshake too.
				p := nativeCompatibilityRun(t, w, root, j, 60*time.Second)
				nativeCompatibilityRead(t, filepath.Join(p.job.Spec.Run.Workdir, "bindings-ok"), "771 inputs verified\n")
			})
		}
	}
}

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
