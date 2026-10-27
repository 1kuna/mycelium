package myce

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestNativeGroupMetricLeaves(t *testing.T) {
	root := t.TempDir()
	g := nativeGroup{
		worker:  Worker{nativeMetricCgroups: []string{filepath.Join(root, "previous-attempt")}},
		memory:  &memoryAttempt{path: root},
		control: &memoryAttempt{path: filepath.Join(root, "control")},
		memberMemory: map[string]*memoryAttempt{
			"main":   {path: filepath.Join(root, "main")},
			"helper": {path: filepath.Join(root, "sidecar-helper")},
		},
	}
	w := g.metricWorker()
	want := []string{g.memberMemory["main"].path, g.memberMemory["helper"].path}
	if !slices.Equal(w.nativeMetricCgroups, want) {
		t.Fatalf("payload metrics include paths outside this attempt's members: %v", w.nativeMetricCgroups)
	}
	if !slices.Equal(g.worker.nativeMetricCgroups, []string{filepath.Join(root, "previous-attempt")}) {
		t.Fatal("member policy changed the original worker")
	}
	// Without delegated ceilings the helper resolves only its actual inherited
	// leaf. There is no manufactured per-member cgroup distinction.
	g.memberMemory = nil
	if got := g.metricWorker().nativeMetricCgroups; len(got) != 0 {
		t.Fatalf("admission-only group retained cgroup paths: %v", got)
	}
}
