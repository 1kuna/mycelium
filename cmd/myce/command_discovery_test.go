package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestCommandDiscoveryAdapterAndProfileAuthoring(t *testing.T) {
	data := strings.Replace(cpuAdapterYAML("source"), fmt.Sprintf("    targets: [{os: %s, arch: %s, platform: cpu}]", runtime.GOOS, runtime.GOARCH), "    portable: true\n    environment: cpu-tools", 1)
	a, err := decodeAdapter([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "work")
	if err != nil || len(p.Targets) != 0 || !myce.WantsCommandDiscovery(p.Spec) || p.Environment.Name != "cpu-tools" {
		t.Fatal(p, err)
	}
	// #1341: omitted targets without portable: true is an ordinary entry that
	// runs wherever it fits, not managed discovery.
	a, err = decodeAdapter([]byte(strings.Replace(data, "    portable: true\n", "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if p, _, err = adapterProfile(a, "work"); err != nil || len(p.Targets) != 0 || myce.WantsCommandDiscovery(p.Spec) {
		t.Fatal("omitted targets reinterpreted as discovery", p.Spec, err)
	}
	root := t.TempDir()
	file := filepath.Join(root, "profile.yaml")
	profile := "name: discover\nproject: fixture\nclass: test\nsource: {directory: source}\nrun: {command: [sh, run.sh]}\nneeds: {platform: cpu, cpu_cores: 1, memory_gb: 1}\nportable: true\nenvironment: cpu-tools\ncheck: [test, -s, result]\n"
	if err := os.WriteFile(file, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	p, _, _, err = readProfile(file)
	if err != nil || len(p.Targets) != 0 || !p.Portable || p.Environment.Name != "cpu-tools" {
		t.Fatal(p, err)
	}
	if err := os.WriteFile(file, []byte(strings.Replace(profile, "portable: true\n", "", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	if p, _, _, err := readProfile(file); err != nil || len(p.Targets) != 0 || myce.WantsCommandDiscovery(p.Spec) {
		t.Fatal("profile without targets refused or inferred portability", p.Spec, err)
	}
}
