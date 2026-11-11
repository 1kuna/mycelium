package myce

import (
	"os"
	"os/exec"
	"slices"
	"testing"
)

// The kernel table read (Darwin) must agree with ps on custody fields, and a
// recorded escaper counts only while its birth matches: a reused PID is never
// treated as the attempt's process (#1322).
func TestProcessInventoryCustody(t *testing.T) {
	rows, err := processInventory()
	if err != nil {
		t.Fatal(err)
	}
	ps, err := psProcessInventory()
	if err != nil {
		t.Fatal(err)
	}
	find := func(rows []processRow) processRow {
		i := slices.IndexFunc(rows, func(r processRow) bool { return r.pid == os.Getpid() })
		if i < 0 {
			t.Fatal("own process missing from inventory")
		}
		return rows[i]
	}
	if got, want := find(rows), find(ps); got != want {
		t.Fatalf("inventory %+v, ps %+v", got, want)
	}

	other := exec.Command("sleep", "60")
	if err := other.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Process.Kill(); _ = other.Wait() }()
	group := 1 << 30 // No live process group or session has this ID.
	defer forgetGroupCustody(group)
	for _, recorded := range []string{"0:0", ""} {
		if recorded == "" {
			if recorded, err = processBirth(other.Process.Pid); err != nil {
				t.Fatal(err)
			}
		}
		adoptGroupCustody(group, map[int]string{other.Process.Pid: recorded})
		members, err := workerGroupMembers(group)
		if err != nil {
			t.Fatal(err)
		}
		if owned := slices.Contains(members, other.Process.Pid); owned != (recorded != "0:0") {
			t.Fatalf("birth %q: member=%v", recorded, owned)
		}
	}
}
