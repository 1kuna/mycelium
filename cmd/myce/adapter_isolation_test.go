package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func TestAdapterDeclaredIsolation(t *testing.T) {
	const reason = "stage needs host IPC"
	data := strings.Replace(cpuAdapterYAML("source"), "    source:", "    isolation: unsandboxed\n    reason: "+reason+"\n    source:", 1)
	a, err := decodeAdapter([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "work")
	if err != nil || p.Isolation != "unsandboxed" || p.Reason != reason {
		t.Fatal("flat entry lost isolation", p, err)
	}
	// Exercise native entry conversion and the actual extension author schema.
	e := a.Entries["work"]
	e.Version, e.Purpose = "1", "native isolation fixture"
	e.Checks.Qualification = []string{"sh", "check.sh"}
	e.Baselines = map[string]myce.ExtensionBaseline{"extension:work": {Input: "baseline", Output: "baseline", Measure: "exact", Native: "synthetic", Variation: "exact"}}
	a.Entries["work"] = e
	p, _, err = adapterProfile(a, "work")
	if err != nil || p.Native == nil || p.Native.Isolation != "unsandboxed" || p.Native.Reason != reason || p.Isolation != "" || p.Reason != "" {
		t.Fatal("native registration lost ownership of isolation", p, err)
	}
	b, err := yaml.Marshal(p.Native)
	if err != nil || !strings.Contains(string(b), "isolation: unsandboxed") || !strings.Contains(string(b), "reason: "+reason) || strings.Contains(string(b), "sandbox_default") {
		t.Fatal("native authored schema lost isolation or exposed registration metadata", string(b), err)
	}
	for _, bad := range []string{
		strings.Replace(data, "unsandboxed", "invalid", 1),
		strings.Replace(data, reason, "'  '", 1),
	} {
		if _, err := decodeAdapter([]byte(bad)); err == nil {
			t.Error("invalid entry isolation accepted")
		}
	}
}
