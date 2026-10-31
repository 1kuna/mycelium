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
			if !errors.As(err, &api) || api.Code != "job_placement_invalid" || !strings.Contains(api.Message, "explicit arch or portable: true") || !strings.Contains(api.Message, "uploaded") || strings.Contains(api.Message, "private-name") {
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

func TestTargetPayloadExtensionAndEnvironmentCoverage(t *testing.T) {
	s := cpuSpec(1, 1)
	if err := normalizeResources(&s); err != nil {
		t.Fatal(err)
	}
	s.Targets = []ExecutionTarget{{OS: "linux"}}
	for _, kind := range []string{"extension", "environment"} {
		t.Run(kind, func(t *testing.T) {
			validate := func(multi bool) error {
				if kind == "extension" {
					variants := []ExtensionVariant{{Target: ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}}}
					if multi {
						variants = append(variants, ExtensionVariant{Target: ExecutionTarget{OS: "linux", Arch: "arm64", Platform: "fake"}})
					}
					// Backend-specific variants can collectively cover multiple arches.
					mixed := s
					mixed.Needs.Platform = Platform{Required: []string{"cpu", "fake"}}
					return validateTargetExtensions(mixed, []Extension{{Variants: variants}})
				}
				lock := "environments:\n  default:\n    packages:\n      linux-64: []\n"
				if multi {
					lock += "      linux-aarch64: []\n"
				}
				return validateTargetRecipes(s, map[string]EnvironmentRecipe{"cpu": {Backend: "pixi", Files: map[string][]byte{"pixi.lock": []byte(lock)}}})
			}
			if err := validate(false); err == nil || !strings.Contains(err.Error(), "portable: true") {
				t.Fatalf("missing architecture/reason: %v", err)
			}
			if err := validate(true); kind == "extension" && err != nil {
				t.Fatal("multi-arch variant coverage refused", err)
			} else if kind == "environment" && err == nil {
				t.Fatal("unproven locked environment accepted")
			}
			declared := s
			declared.Portable = true
			if err := validateTargetRecipes(declared, map[string]EnvironmentRecipe{"cpu": {Binary: "opaque", Build: []string{"true"}}}); err != nil {
				t.Fatal(err)
			}
			if err := validateTargetRecipes(s, map[string]EnvironmentRecipe{"cpu": {Build: []string{"go", "build", "-o", "{cache}/program", "./cmd/program"}}}); err != nil {
				t.Fatal("source-only worker build refused", err)
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
