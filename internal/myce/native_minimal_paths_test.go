package myce

import (
	"encoding/json"
	"flag"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func TestNativeMinimalRootLinkLayout(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runtime")
	write := filepath.Join(t.TempDir(), "attempt")
	for _, tc := range []struct {
		target  string
		allowed bool
	}{
		{"lib/tool", true}, {filepath.Join(root, "lib/tool"), true},
		{"../../run/worker.sock", false}, {"/run/worker.sock", false},
		{filepath.Join(write, "control.sock"), false}, {"/", false},
	} {
		if err := nativeMinimalLink(filepath.Join(root, "tool"), tc.target, []string{root}, []string{write}); (err == nil) != tc.allowed {
			t.Errorf("target %q: %v", tc.target, err)
		}
	}
	for _, path := range []string{"/", "relative", "/runtime/../run", "/runtime\ncontrol"} {
		if nativeMinimalDestination(path) == nil {
			t.Errorf("unsafe destination accepted: %q", path)
		}
	}
}

func TestNativeMinimalRootOperatorConfig(t *testing.T) {
	c, err := PlanInstall("worker", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "linux", Platform: "fake", MemoryMode: "fixed", MemoryGB: 1})
	if err != nil {
		t.Fatal(err)
	}
	if c.NativeMinimalRoot || (Worker{}).NativeMinimalRoot {
		t.Fatal("minimal root is enabled by default")
	}
	values := ServiceConfig{}
	f := flag.NewFlagSet("install", flag.ContinueOnError)
	RegisterInstallFlags(f, &values)
	if err := f.Parse([]string{"--native-minimal-root"}); err != nil {
		t.Fatal(err)
	}
	c, err = OverlayInstallFlags(f, c, values)
	if err != nil || !c.NativeMinimalRoot {
		t.Fatal("explicit opt-in lost", err)
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var saved ServiceConfig
	if err := json.Unmarshal(data, &saved); err != nil || !reflect.DeepEqual(saved, c) {
		t.Fatal("persistence lost operator setting", err)
	}
	if !slices.Contains(saved.Args(), "--native-minimal-root") {
		t.Fatal("service argv omitted opt-in")
	}
	reset := flag.NewFlagSet("install", flag.ContinueOnError)
	RegisterInstallFlags(reset, &values)
	if err := reset.Parse([]string{"--native-minimal-root=false"}); err != nil {
		t.Fatal(err)
	}
	cleared, err := OverlayInstallFlags(reset, saved, values)
	if err != nil || cleared.NativeMinimalRoot || slices.Contains(cleared.Args(), "--native-minimal-root") {
		t.Fatal("explicit false did not clear saved opt-in", err)
	}
	for _, bad := range []ServiceConfig{{Role: "queue", Detection: Detection{OS: "linux"}, NativeMinimalRoot: true}, {Role: "worker", Detection: Detection{OS: "darwin"}, NativeMinimalRoot: true}} {
		if bad.ValidateInstall() == nil {
			t.Fatal("unsupported opt-in accepted")
		}
	}
	j := Job{}
	m := Machine{Capabilities: []string{"native_isolation_minimal_root", "native_sidecars"}}
	if !nativeNetworkFits(&j, &m) {
		t.Fatal("minimal-root worker cannot receive offline work")
	}
	j.Spec.Needs.Network = true
	if nativeNetworkFits(&j, &m) {
		t.Fatal("minimal-root worker accepted general egress")
	}
	status := NativeIsolationStatus(Machine{Capabilities: []string{"native_isolation_minimal_root"}})
	if status == NativeIsolationStatus(Machine{Capabilities: []string{"native_isolation_namespace"}}) {
		t.Fatal("minimal root status indistinguishable from host-root namespace")
	}
}
