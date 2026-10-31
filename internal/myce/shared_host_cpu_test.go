package myce

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSharedHostCPUAggregateCgroup(t *testing.T) {
	c := fakeCeilings(t)
	shared, err := sharedCPUCeilings(c, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct{ path, want string }{{"shared-execution/cpu.max", "200000 100000"}, {"shared-execution/cpu.weight", "10"}, {"worker/cgroup.procs", "42"}} {
		b, err := os.ReadFile(filepath.Join(c.root, v.path))
		if err != nil || string(b) != v.want {
			t.Errorf("%s=%q want %q: %v", v.path, b, v.want, err)
		}
	}
	if shared.root == c.root || filepath.Dir(shared.root) != c.root {
		t.Fatal("execution/control not separate")
	}
	for _, name := range []string{"main-attempt", "sidecar-attempt"} {
		a, err := shared.create(name, 1<<20, 0)
		if err != nil || filepath.Dir(a.path) != shared.root {
			t.Fatal("attempt outside aggregate cap", a, err)
		}
	}
	if _, err := sharedCPUCeilings(c, 1); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(shared.root, "cpu.max"))
	if string(b) != "100000 100000" {
		t.Fatal("budget reduction not applied", string(b))
	}
	if _, err := sharedCPUCeilings(nil, 2); err == nil {
		t.Fatal("missing delegation claimed enforcement")
	}
}
