package myce

import (
	"archive/tar"
	"bytes"
	"context"
	"debug/macho"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Tiny ABI fixtures, not native toolchain outputs: these tests run on any OS.
func linkageFixture(cpu macho.Cpu, commands ...struct {
	code uint32
	name string
}) []byte {
	var loads bytes.Buffer
	for _, command := range commands {
		n := (24 + len(command.name) + 1 + 7) &^ 7
		b := make([]byte, n)
		binary.LittleEndian.PutUint32(b, command.code)
		binary.LittleEndian.PutUint32(b[4:], uint32(n))
		binary.LittleEndian.PutUint32(b[8:], 24)
		copy(b[24:], command.name)
		loads.Write(b)
	}
	var out bytes.Buffer
	for _, n := range []uint32{macho.Magic64, uint32(cpu), 0, uint32(macho.TypeExec), uint32(len(commands)), uint32(loads.Len()), 0, 0} {
		_ = binary.Write(&out, binary.LittleEndian, n)
	}
	out.Write(loads.Bytes())
	return out.Bytes()
}
func linkedFixture(paths ...string) []byte {
	var commands []struct {
		code uint32
		name string
	}
	for _, p := range paths {
		commands = append(commands, struct {
			code uint32
			name string
		}{uint32(macho.LoadCmdDylib), p})
	}
	return linkageFixture(macho.CpuArm64, commands...)
}

// The parser must not read symbol/string tables or file contents, even when a
// universal binary has a distant second slice.
type linkageReadLedger struct {
	r     io.ReaderAt
	bytes int
}

func (r *linkageReadLedger) ReadAt(p []byte, off int64) (int, error) {
	n, e := r.r.ReadAt(p, off)
	r.bytes += n
	return n, e
}

func TestMachOLinkageWorkerRefusesBeforePayload(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		w := recoveryWorker(t)
		w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
		w.Machine.HostMemoryMeasured = true
		w.Machine.HostAvailableMemoryGB, w.Machine.FreeHostMemoryGB = 64, 64
		w.Machine.CapabilityProfile = &CapabilityProfile{OS: "darwin", Arch: "arm64"}
		store, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer store.close()
		store.state.Session = w.Machine.Session
		marker := filepath.Join(t.TempDir(), "started")
		j := recoveryJob(t, []string{"sh", "-c", "touch \"$MARKER\""})
		j.Spec.Run.Env = map[string]string{"MARKER": marker}
		// Stage the real frozen source through the same cache/extractor used by
		// newly lowered profiles, rather than creating an already-extracted tree.
		missing := filepath.ToSlash(filepath.Join(t.TempDir(), "missing.dylib"))
		data := linkedFixture(missing)
		var archive bytes.Buffer
		tw := tar.NewWriter(&archive)
		if err := tw.WriteHeader(&tar.Header{Name: "helper", Mode: 0700, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		cache := newTestStateRoot(t)
		file := filepath.Join(t.TempDir(), "source.tar")
		if err := os.WriteFile(file, archive.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cache, ref.SHA256), archive.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		w.fileCache = &FileCache{Root: cache, Limit: 1 << 20, Clock: w.Clock}
		j.Spec.Files = FileManifest{".myce-source.tar": ref}
		j.Spec.PrepareSource = true
		p, err := startAfterFixtureInputs(t, w, store, j)
		var typed *runtimeError
		if p != nil || !errors.As(err, &typed) || typed.diagnostic.Class != machoLibraryMissing || !strings.Contains(err.Error(), missing) {
			t.Fatalf("launched or untyped: %v %v", p, err)
		}
		if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("payload started")
		}

	})

	t.Run("MachOLinkageMissingAndPortable", func(t *testing.T) {
		root := t.TempDir()
		missing := filepath.ToSlash(filepath.Join(root, "absent", "libcodec.dylib"))
		present := filepath.Join(root, "present.dylib")
		if err := os.WriteFile(present, []byte("present"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(root, "bin"), 0700); err != nil {
			t.Fatal(err)
		}
		write := func(name string, data []byte, mode os.FileMode) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(root, name), data, mode); err != nil {
				t.Fatal(err)
			}
		}
		write("bin/tool", linkedFixture(missing), 0700)
		write("bin/portable", linkedFixture("/usr/lib/libSystem.B.dylib", "/System/Library/Frameworks/Foundation.framework/Foundation", "@rpath/present.dylib", "@loader_path/../present.dylib", "@executable_path/../present.dylib", filepath.ToSlash(present)), 0700)
		write("data.bin", linkedFixture(missing), 0600) // A data file is never opened.
		write("plugin.so", linkedFixture(missing), 0600)
		write("weak", linkageFixture(macho.CpuArm64, struct {
			code uint32
			name string
		}{0x80000018, missing}), 0700)
		write("identity.dylib", linkageFixture(macho.CpuArm64, struct {
			code uint32
			name string
		}{0xd, missing}), 0600)
		findings, err := InspectMachOLinkage(context.Background(), root, "arm64")
		if err != nil || len(findings) != 2 {
			t.Fatalf("findings: %+v %v", findings, err)
		}
		for _, d := range findings {
			if d.Library != missing || (d.Binary != "bin/tool" && d.Binary != "plugin.so") {
				t.Fatal(d)
			}
		}
		findings, err = InspectMachOLinkage(context.Background(), root, "amd64")
		if err != nil || len(findings) != 0 {
			t.Fatalf("wrong architecture: %+v %v", findings, err)
		}

	})

	t.Run("MachOLinkagePrelaunchDoesNotChargeInfrastructure", func(t *testing.T) {
		for _, priority := range []string{"experiment", "production"} {
			t.Run(priority, func(t *testing.T) {
				root := t.TempDir()
				missing := filepath.ToSlash(filepath.Join(root, "missing.dylib"))
				if err := os.WriteFile(filepath.Join(root, "tool"), linkedFixture(missing), 0700); err != nil {
					t.Fatal(err)
				}
				err := checkPayloadMachOLinkage(context.Background(), ExecutionTarget{OS: "darwin", Arch: "arm64"}, root)
				var typed *runtimeError
				if !errors.As(err, &typed) || typed.diagnostic.Class != machoLibraryMissing || !strings.Contains(err.Error(), missing) {
					t.Fatalf("missing typed path: %v", err)
				}
				if err := checkPayloadMachOLinkage(context.Background(), ExecutionTarget{OS: "linux", Arch: "arm64"}, root); err != nil {
					t.Fatal(err)
				}
				q, _ := fixture(t)
				m := valueMachine("worker", "cpu", 16)
				check(t, q, m, nil)
				spec := cpuSpec(1, 1)
				spec.Priority = priority
				j := submit(t, q, spec)
				assigned := check(t, q, m, nil).Job
				if assigned == nil {
					t.Fatal("not assigned")
				}
				r := startFailureOutcome(result(assigned, false), err)
				if r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Class != machoLibraryMissing {
					t.Fatalf("misclassified: %+v", r)
				}
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				got := job(t, q, j.ID)
				if got.State != "failed" || got.Losses != 0 || got.InfrastructureRetries != 0 {
					t.Fatalf("charged infrastructure: %+v", got)
				}
				public := statusJobCopy(got)
				if !strings.Contains(public.Reason, missing) || !strings.Contains(public.Reason, "tool") {
					t.Fatal("lost actionable path", public.Reason)
				}
			})
		}

	})
}
