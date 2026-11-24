package myce

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Keep the review's native-artifact fixture without requiring a cross compiler
// or executing foreign code. Admission depends on declarations, not these bytes.
func targetPayloadELF() []byte {
	b := make([]byte, 64)
	copy(b, "\x7fELF\x02\x01\x01")
	binary.LittleEndian.PutUint16(b[16:], 2)
	binary.LittleEndian.PutUint16(b[18:], 62)
	binary.LittleEndian.PutUint32(b[20:], 1)
	binary.LittleEndian.PutUint16(b[52:], 64)
	return b
}

// Both independent-review reproducers must fail admission before an amd64
// worker can claim the arm64-only payload. No artifact inspection is trusted.
func TestTargetPayloadReviewBypasses(t *testing.T) {
	for _, kind := range []string{"binary-with-build-hook", "uploaded-tar"} {
		t.Run(kind, func(t *testing.T) {
			q, _ := fixture(t)
			data := targetPayloadELF()
			binary.LittleEndian.PutUint16(data[18:], 183)
			s := cpuSpec(1, 1)
			s.Targets = []ExecutionTarget{{OS: "linux"}}
			s.RequireTarget = true
			m := valueMachine("amd64", "cpu", 16)
			if kind == "binary-with-build-hook" {
				file := filepath.Join(t.TempDir(), "program")
				if err := os.WriteFile(file, data, 0700); err != nil {
					t.Fatal(err)
				}
				r, err := snapshotEnvironment(EnvironmentRecipe{Binary: file, Build: []string{"true"}}, "cpu")
				if err != nil {
					t.Fatal(err)
				}
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
					t.Fatal(err)
				}
				s.Environment = Environment{Name: "tools"}
				s.Run.Command = []string{"program"}
				m.Environments = []string{r.Key}
			} else {
				if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
					t.Fatal(err)
				}
				var b bytes.Buffer
				tw := tar.NewWriter(&b)
				if err := tw.WriteHeader(&tar.Header{Name: "program", Mode: 0755, Size: int64(len(data))}); err != nil {
					t.Fatal(err)
				}
				if _, err := tw.Write(data); err != nil {
					t.Fatal(err)
				}
				if err := tw.Close(); err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(t.TempDir(), "artifacts.tar")
				if err := os.WriteFile(file, b.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
				ref, err := IdentifyFile(file)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(q.files.blob(ref), b.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
				s.Files = FileManifest{"artifacts.tar": ref}
				s.Run.Command = []string{"sh", "-c", "tar -xf artifacts.tar && ./program"}
			}
			check(t, q, m, nil)
			_, err := q.Submit(s)
			var api *HTTPError
			if !errors.As(err, &api) || api.Code != "job_placement_invalid" || !strings.Contains(api.Message, "portable: true") {
				t.Fatalf("missing actionable refusal: %v", err)
			}
			if got := check(t, q, m, nil).Job; got != nil {
				t.Fatal("unproven payload assigned to amd64")
			}
			s.Portable = true
			j := submit(t, q, s)
			if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
				t.Fatal("author's explicit portability assertion not honored", got)
			}
		})
	}
}

func TestTargetPayloadDeclarationRequired(t *testing.T) {
	q, _ := fixture(t)
	s := cpuSpec(1, 1)
	s.Targets = []ExecutionTarget{{OS: "linux"}}
	s.Run.Command = []string{"go", "test", "./..."}
	_, err := q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "job_placement_invalid" || !strings.Contains(api.Message, "declare portable: true if this runs on any arch, or name the arch") {
		t.Fatalf("plain commands also need the author's declaration: %v", err)
	}
	s.Targets[0].Arch = "amd64"
	if _, err := q.Submit(s); err != nil {
		t.Fatal("explicit arch now requires a declaration", err)
	}
}
