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

func TestTargetPayloadOSOnlyPlacement(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run(arch, func(t *testing.T) {
			q, _ := fixture(t)
			small, scarce := valueMachine("z-general", "cpu", 16), valueMachine("a-scarce", "fake", 64)
			small.CapabilityProfile.Arch = arch
			scarce.CapabilityProfile.Arch = "amd64"
			check(t, q, small, nil)
			check(t, q, scarce, nil)
			s := cpuSpec(1, 1)
			s.Targets = []ExecutionTarget{{OS: "linux"}}
			s.RequireTarget = true
			s.Portable = true
			s.Run.Command = []string{"go", "test", "./..."}
			if err := s.Validate(); err != nil {
				t.Fatal(err)
			}
			report, err := q.Portability(PortabilityRequest{Spec: &s})
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range report.Targets {
				if row.Target.Arch == "unknown" || !row.Declared {
					t.Fatalf("OS-only coverage not projected onto inventory: %+v", row)
				}
			}
			j := submit(t, q, s)
			if got := check(t, q, scarce, nil).Job; got != nil {
				t.Fatal("scarce host won by pulling first", got.ID)
			}
			if got := check(t, q, small, nil).Job; got == nil || got.ID != j.ID {
				t.Fatal("OS-only work not placed on least valuable compatible host", got)
			}
			small.CapabilityProfile.OS = "darwin"
			if executionTargetMatches(s, &small) {
				t.Fatal("wrong OS matched")
			}
			s.Targets[0].Arch = "riscv64"
			small.CapabilityProfile.OS = "linux"
			if executionTargetMatches(s, &small) {
				t.Fatal("explicit arch ignored")
			}
		})
	}
}

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

func TestTargetPayloadPrebuiltRejection(t *testing.T) {
	for _, archive := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "source-tar"}[archive], func(t *testing.T) {
			q, _ := fixture(t)
			if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			data := targetPayloadELF()
			name := "program"
			if archive {
				var out bytes.Buffer
				tw := tar.NewWriter(&out)
				if err := tw.WriteHeader(&tar.Header{Name: "program", Mode: 0755, Size: int64(len(data))}); err != nil {
					t.Fatal(err)
				}
				if _, err := tw.Write(data); err != nil {
					t.Fatal(err)
				}
				if err := tw.Close(); err != nil {
					t.Fatal(err)
				}
				data, name = out.Bytes(), ".myce-source.tar"
			}
			file := filepath.Join(t.TempDir(), "private-name")
			if err := os.WriteFile(file, data, 0600); err != nil {
				t.Fatal(err)
			}
			ref, err := IdentifyFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
				t.Fatal(err)
			}
			s := cpuSpec(1, 1)
			s.Targets = []ExecutionTarget{{OS: "linux"}}
			s.Files = FileManifest{name: ref}
			_, err = q.Submit(s)
			var api *HTTPError
			if !errors.As(err, &api) || api.Code != "job_placement_invalid" || !strings.Contains(api.Message, "declare portable: true if this runs on any arch, or name the arch") || strings.Contains(api.Message, "private-name") {
				t.Fatalf("not actionable/sanitized: %v", err)
			}
			s.Targets[0].Arch = "amd64"
			if _, err = q.Submit(s); err != nil {
				t.Fatal("explicit required arch refused", err)
			}
			s.Targets[0].Arch = ""
			s.Portable = true
			if _, err := q.Submit(s); err != nil {
				t.Fatal("author portability declaration refused", err)
			}
		})
	}
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

func TestTargetPayloadNASDeclarationRequired(t *testing.T) {
	q, _ := fixture(t)
	e := compositionExtension(t, false)
	v := e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "fake"}
	v.Execution.Main.Command = []string{"sh", "-c", "chmod +x program && ./program"}
	other := v
	other.Target.Arch = "arm64"
	e.Variants = []ExtensionVariant{v, other}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	data := targetPayloadELF()
	binary.LittleEndian.PutUint16(data[18:], 183)
	if err := os.WriteFile(filepath.Join(q.root, "arm64-program"), data, 0700); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("amd64", "separate", 64, 8, e)
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = "linux", "amd64"
	for i := range m.ExtensionReports {
		r := newExtensionReport(e, &m, m.ExtensionReports[i].Work)
		r.Setup, r.Qualified, r.CheckID = true, true, "test-check"
		m.ExtensionReports[i] = r
	}
	check(t, q, m, nil)
	s := placementSpec(false)
	s.Extension = extensionKey(e)
	s.Targets = []ExecutionTarget{{OS: "linux"}}
	s.Inputs = map[string]string{"program": "arm64-program"}
	_, err := q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "job_placement_invalid" || !strings.Contains(api.Message, "declare portable: true") {
		t.Fatalf("NAS payload admitted without declaration: %v", err)
	}
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("undeclared NAS payload assigned", got)
	}
	s.Targets[0].Arch = "arm64"
	j := submit(t, q, s)
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("explicit arm64 NAS payload assigned to amd64", got)
	}
	if job(t, q, j.ID).State != "queued" {
		t.Fatal("explicit target lost its normal placement behavior")
	}
}
