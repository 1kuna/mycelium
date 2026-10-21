package main

import (
	"debug/elf"
	"debug/macho"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"mycelium/internal/myce"
	"os"
	"os/exec"
	"path/filepath"
)

func versionCommand(args []string) error {
	f := flag.NewFlagSet("version", flag.ContinueOnError)
	v := f.String("expect-version", "", "require this release version")
	goos := f.String("expect-os", "", "require this operating system")
	arch := f.String("expect-arch", "", "require this architecture")
	start := f.String("expect-start-gate", "", "require this launch protocol")
	attempt := f.String("expect-attempt-protocol", "", "require this attempt protocol")
	native := f.Bool("require-native-sensing", false, "require native owner signals on macOS")
	f.Bool("json", false, "print version as JSON")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("version accepts flags only")
	}
	info := myce.Version()
	if *v != "" && info.Version != *v || *goos != "" && info.OS != *goos || *arch != "" && info.Arch != *arch || *start != "" && info.StartGate != *start || *attempt != "" && info.AttemptProtocol != *attempt || *native && info.OS == "darwin" && !info.NativeSensing {
		return errors.New("release version, target or native sensing mismatch")
	}
	return json.NewEncoder(os.Stdout).Encode(info)
}
func releaseCommand(args []string) error {
	if len(args) == 1 && args[0] == "targets" {
		return json.NewEncoder(os.Stdout).Encode(myce.ReleaseTargets)
	}
	if len(args) == 1 && args[0] == "build-targets" {
		for _, target := range myce.ReleaseTargets {
			cgo := 0
			if target.NativeSensing {
				cgo = 1
			}
			fmt.Printf("%s %s %d\n", target.OS, target.Arch, cgo)
		}
		return nil
	}
	if len(args) == 0 || args[0] != "manifest" {
		return errors.New("usage: myce release manifest --dir DIR --version VERSION --signing-key PRIVATE_PEM")
	}
	f := flag.NewFlagSet("release manifest", flag.ContinueOnError)
	dir := f.String("dir", "", "release artifacts directory")
	version := f.String("version", "", "release version")
	key := f.String("signing-key", "", "private Ed25519 PKCS8 PEM")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 || *dir == "" || *key == "" {
		return errors.New("release directory and signing key required")
	}
	m := myce.ReleaseManifest{Version: *version, StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol}
	for _, target := range myce.ReleaseTargets {
		a := myce.ReleaseArtifact{OS: target.OS, Arch: target.Arch, File: "myce-" + target.OS + "-" + target.Arch}
		path := filepath.Join(*dir, a.File)
		st, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() {
			return errors.New("release artifact must be a regular executable")
		}
		a.Size = st.Size()
		if a.OS == "linux" {
			bin, err := elf.Open(path)
			if err != nil {
				return errors.New("release artifact is not Linux ELF")
			}
			machine := elf.EM_X86_64
			if a.Arch == "arm64" {
				machine = elf.EM_AARCH64
			}
			valid := bin.Machine == machine
			bin.Close()
			if !valid {
				return errors.New("wrong Linux release architecture")
			}
		} else {
			bin, err := macho.Open(path)
			if err != nil {
				return errors.New("release artifact is not native macOS Mach-O")
			}
			valid := bin.Cpu == macho.CpuArm64
			bin.Close()
			if !valid {
				return errors.New("wrong macOS release architecture")
			}
			out, err := exec.Command(path, "version", "--json").Output()
			if err != nil {
				return errors.New("Mac release must be built and inspected natively on macOS arm64")
			}
			var info myce.VersionInfo
			if json.Unmarshal(out, &info) != nil || info.Version != *version || info.StartGate != m.StartGate || info.AttemptProtocol != m.AttemptProtocol || !info.NativeSensing {
				return errors.New("Mac release lacks the expected protocol or native sensing")
			}
			a.NativeSensing = true
		}
		a.SHA256, err = myce.FileSHA256(path)
		if err != nil {
			return err
		}
		m.Artifacts = append(m.Artifacts, a)
	}
	data, err := myce.SignRelease(m, *key)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(*dir, "release.json"), data, 0644); err != nil {
		return err
	}
	var checksums string
	for _, a := range m.Artifacts {
		checksums += fmt.Sprintf("%s  %s\n", a.SHA256, a.File)
	}
	return os.WriteFile(filepath.Join(*dir, "SHA256SUMS"), []byte(checksums), 0644)
}
