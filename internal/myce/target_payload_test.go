package myce

import (
	"archive/tar"
	"bytes"
	"context"
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

// A minimal real ELF header is enough for the standard library's binary reader;
// no cross compiler, accelerator, or executable test fixture is needed.
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
			if !errors.As(err, &api) || api.Code != "job_placement_invalid" || !strings.Contains(api.Message, "arch: amd64") || !strings.Contains(api.Message, "prebuilt native binary") || strings.Contains(api.Message, "private-name") {
				t.Fatalf("not actionable/sanitized: %v", err)
			}
			s.Targets[0].Arch = "amd64"
			if _, err = q.Submit(s); err != nil {
				t.Fatal("explicit required arch refused", err)
			}
			if err := ValidateTargetPayload(context.Background(), s, file, archive); err != nil {
				t.Fatal(err)
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
				return validateTargetRecipes(context.Background(), s, map[string]EnvironmentRecipe{"cpu": {Backend: "pixi", Files: map[string][]byte{"pixi.lock": []byte(lock)}}})
			}
			if err := validate(false); err == nil || !strings.Contains(err.Error(), "arch: amd64") || !strings.Contains(err.Error(), kind) {
				t.Fatalf("missing architecture/reason: %v", err)
			}
			if err := validate(true); err != nil {
				t.Fatal("multi-arch payload refused", err)
			}
		})
	}
}
