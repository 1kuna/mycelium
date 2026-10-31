package myce

import (
	"archive/tar"
	"context"
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"gopkg.in/yaml.v3"
)

func hasAnyArchTarget(s Spec) bool {
	return slices.ContainsFunc(s.Targets, func(t ExecutionTarget) bool { return t.Arch == "" })
}

// The payload, not transient inventory, determines an architecture restriction.
// Reasons contain only fixed labels and validated architecture identifiers.
func requirePayloadArch(s Spec, osName string, arches []string, why string) error {
	slices.Sort(arches)
	arches = slices.Compact(arches)
	if len(arches) != 1 {
		return nil
	}
	for _, t := range s.Targets {
		if t.Arch == "" && t.OS == osName {
			return rejectFieldRule("job_placement_invalid", fmt.Errorf("target os: %s requires arch: %s because %s has only that architecture; declare arch: %s or provide portable source/multi-arch variants", osName, arches[0], why, arches[0]))
		}
	}
	return nil
}

// ValidateTargetPayload inspects a frozen file or uncompressed source tar using
// standard binary readers. It executes nothing and never publishes file names.
// Call outside queue locks; source archives may be large.
func ValidateTargetPayload(ctx context.Context, s Spec, filename string, archive bool) error {
	if !hasAnyArchTarget(s) {
		return nil
	}
	f, err := os.Open(filename)
	if err != nil {
		return errors.New("cannot inspect frozen payload; declare its required arch or provide portable source")
	}
	defer f.Close()
	inspect := func(r io.ReaderAt) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		osName, arches := nativePayloadArchitectures(r)
		return requirePayloadArch(s, osName, arches, "a prebuilt native binary in the payload")
	}
	if !archive {
		return inspect(f)
	}
	tr := tar.NewReader(contextReader{ctx, f})
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if err := inspect(io.NewSectionReader(f, offset, h.Size)); err != nil {
			return err
		}
	}
}

func nativePayloadArchitectures(r io.ReaderAt) (string, []string) {
	if f, err := elf.NewFile(r); err == nil {
		arch := map[elf.Machine]string{elf.EM_X86_64: "amd64", elf.EM_AARCH64: "arm64", elf.EM_386: "386", elf.EM_ARM: "arm", elf.EM_RISCV: "riscv64", elf.EM_S390: "s390x"}[f.Machine]
		if arch != "" {
			return "linux", []string{arch}
		}
	}
	machoArch := func(cpu macho.Cpu) string {
		return map[macho.Cpu]string{macho.CpuAmd64: "amd64", macho.CpuArm64: "arm64", macho.Cpu386: "386", macho.CpuArm: "arm"}[cpu]
	}
	if f, err := macho.NewFatFile(r); err == nil {
		var arches []string
		for _, a := range f.Arches {
			if arch := machoArch(a.Cpu); arch != "" {
				arches = append(arches, arch)
			}
		}
		return "darwin", arches
	}
	if f, err := macho.NewFile(r); err == nil {
		if arch := machoArch(f.Cpu); arch != "" {
			return "darwin", []string{arch}
		}
	}
	if f, err := pe.NewFile(r); err == nil {
		arch := map[uint16]string{pe.IMAGE_FILE_MACHINE_AMD64: "amd64", pe.IMAGE_FILE_MACHINE_ARM64: "arm64", pe.IMAGE_FILE_MACHINE_I386: "386"}[f.Machine]
		if arch != "" {
			return "windows", []string{arch}
		}
	}
	return "", nil
}

func validateTargetExtensions(s Spec, extensions []Extension) error {
	for _, t := range s.Targets {
		if t.Arch != "" {
			continue
		}
		var arches []string
		for _, e := range extensions {
			for _, v := range e.Variants {
				if t.matches(v.Target) {
					arches = append(arches, v.Target.Arch)
				}
			}
		}
		if err := requirePayloadArch(Spec{Targets: []ExecutionTarget{t}}, t.OS, arches, "the matching extension variants"); err != nil {
			return err
		}
	}
	return nil
}

// Check before recipes are externalized. Exact recipe targets and Pixi lock
// coverage are evidence; a universal uv lock or worker-side build is not an
// architecture pin. Environment readiness still checks the chosen exact target.
func targetPayloadPlatform(s Spec, platform string) bool {
	if s.Needs.cpuOnly() {
		return platform == "cpu"
	}
	return platform != "cpu" && (len(s.Needs.Platform.Required) == 0 || slices.Contains(s.Needs.Platform.Required, platform))
}

func validateTargetRecipes(ctx context.Context, s Spec, recipes map[string]EnvironmentRecipe) error {
	for _, t := range s.Targets {
		if t.Arch != "" {
			continue
		}
		for platform, r := range recipes {
			if t.Platform != "" && t.Platform != platform || !targetPayloadPlatform(s, platform) {
				continue
			}
			if r.Target != nil && t.matches(*r.Target) {
				if err := requirePayloadArch(s, t.OS, []string{r.Target.Arch}, "the environment recipe"); err != nil {
					return err
				}
			}
			if r.Binary != "" && len(r.Build) == 0 {
				if err := ValidateTargetPayload(ctx, s, r.Binary, false); err != nil {
					return err
				}
			}
			if r.Backend != "pixi" {
				continue
			}
			var lock struct {
				Environments map[string]struct {
					Packages map[string]any `yaml:"packages"`
				} `yaml:"environments"`
			}
			if yaml.Unmarshal(r.Files["pixi.lock"], &lock) != nil {
				continue
			}
			name := r.Environment
			if name == "" {
				name = "default"
			}
			var arches []string
			for _, arch := range []string{"amd64", "arm64"} {
				if _, ok := lock.Environments[name].Packages[lockPlatform(ExecutionTarget{OS: t.OS, Arch: arch})]; ok {
					arches = append(arches, arch)
				}
			}
			if err := requirePayloadArch(Spec{Targets: []ExecutionTarget{t}}, t.OS, arches, "the selected environment lock"); err != nil {
				return err
			}
		}
	}
	return nil
}
