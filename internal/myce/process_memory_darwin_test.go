package myce

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestProcessMemoryDarwinPressureCounters(t *testing.T) {
	const vmstat = "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 65536.\nPages inactive: 131072.\nSwapouts: 23.\n"
	for _, mode := range []string{"complete", "missing swapouts", "unknown pressure"} {
		t.Run(mode, func(t *testing.T) {
			probe := func(_ context.Context, name string, _ ...string) ([]byte, error) {
				if name == "/usr/sbin/sysctl" {
					if mode == "unknown pressure" {
						return []byte("unknown\n"), nil
					}
					return []byte("2\n"), nil
				}
				if mode == "missing swapouts" {
					return []byte(strings.ReplaceAll(vmstat, "Swapouts: 23.\n", "")), nil
				}
				return []byte(vmstat), nil
			}
			p, err := readDarwinProcessPressure(context.Background(), probe)
			if mode != "complete" {
				if err == nil || p != nil {
					t.Fatalf("missing telemetry became zero: %+v %v", p, err)
				}
				return
			}
			if err != nil || p.level != 2 || p.available != 3 || p.swapOut != 23 {
				t.Fatalf("native pressure counters: %+v %v", p, err)
			}
		})
	}
}

// Membership by group or session plus descendants from one table; churned or
// reused members are skipped, never charged; a changed root is unknown; an
// owned process this user cannot inspect asks for the ps fallback.
func TestOwnedResidentFromTable(t *testing.T) {
	rows := []darwinProcRow{
		{pid: 7, ppid: 1, pgid: 7, birth: "b7"},    // root: control, not charged
		{pid: 8, ppid: 7, pgid: 7, birth: "b8"},    // group member
		{pid: 9, ppid: 1, pgid: 9, birth: "b9"},    // same session, own group
		{pid: 10, ppid: 8, pgid: 10, birth: "b10"}, // descendant that left both
		{pid: 11, ppid: 8, pgid: 7, birth: "b11", zombie: true},
		{pid: 12, ppid: 7, pgid: 7, birth: "b12"},  // exits before its read
		{pid: 13, ppid: 7, pgid: 7, birth: "b13"},  // PID reused before its read
		{pid: 99, ppid: 1, pgid: 99, birth: "b99"}, // unrelated
	}
	sid := func(pid int) (int, error) {
		if pid == 9 || pid == 8 || pid == 12 || pid == 13 {
			return 7, nil
		}
		return pid, nil
	}
	for _, mode := range []string{"complete", "reused root", "uninspectable member"} {
		t.Run(mode, func(t *testing.T) {
			resident := func(pid int) (uint64, string, error) {
				switch {
				case pid == 12:
					return 0, "", syscall.ESRCH
				case pid == 13:
					return 1 << 30, "successor", nil
				case pid == 9 && mode == "uninspectable member":
					return 0, "", syscall.EPERM
				case pid == 7 || pid == 99 || pid == 11:
					t.Fatalf("read non-member %d", pid)
				}
				return uint64(pid) << 20, map[int]string{8: "b8", 9: "b9", 10: "b10"}[pid], nil
			}
			birthOf := func(pid int) (string, error) {
				if mode == "reused root" {
					return "", errors.New("gone")
				}
				return "b7", nil
			}
			bytes, members, err, ok := ownedResidentFromTable(map[int]string{7: "b7"}, rows, sid, resident, birthOf)
			switch mode {
			case "reused root":
				if err == nil || !ok {
					t.Fatalf("changed root became valid: %v %t", err, ok)
				}
			case "uninspectable member":
				if ok {
					t.Fatal("uninspectable member did not request the ps fallback")
				}
			default:
				if err != nil || !ok || members != 3 || bytes != 27<<20 {
					t.Fatalf("got %d bytes %d members %v %t", bytes, members, err, ok)
				}
			}
		})
	}
	if _, _, err, ok := ownedResidentFromTable(map[int]string{7: "stale"}, rows, sid, nil, nil); err == nil || !ok {
		t.Fatal("root birth mismatch in the table became valid")
	}
}
