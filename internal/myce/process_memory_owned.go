package myce

import (
	"context"
	"errors"
	"os"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

// Keep the existing RSS inventory and cleanup session boundary. Root birth
// checks fence reuse both before and during the inventory; a changed root makes
// the sample unknown. Members are fenced individually: one that exits, is
// replaced, or leaves the session between the membership list and the RSS
// inventory is dropped from this sample, never charged under a reused PID.
// Failing the whole sample instead let a job that forks short-lived helpers
// reset the guard on every observation and grow without bound (measured:
// 256 MiB/s with /usr/bin/true churn was never stopped).
func ownedSessionResident(roots map[int]string) (uint64, int, error) {
	probeguard.Check()
	return sampleOwnedResident(roots, processBirth, workerGroupMembers, func(pid, root int) bool {
		session, sessionErr := unix.Getsid(pid)
		group, groupErr := syscall.Getpgid(pid)
		return sessionErr == nil && groupErr == nil && (session == root || group == root)
	}, func() ([]byte, error) { return processPS("-ax", "-o", "pid=,ppid=,rss=") })
}

func sampleOwnedResident(roots map[int]string, birthOf func(int) (string, error), membersOf func(int) ([]int, error), belongs func(int, int) bool, inventory func() ([]byte, error)) (uint64, int, error) {
	owned := map[int]bool{}
	births := map[int]string{}
	owners := map[int]int{}
	excluded := map[int]bool{}
	for root, saved := range roots {
		birth, err := birthOf(root)
		if err != nil || saved == "" || birth != saved {
			return 0, 0, errors.New("RSS ownership unavailable")
		}
		excluded[root] = true
		members, err := membersOf(root)
		if err != nil {
			return 0, 0, err
		}
		for _, pid := range members {
			birth, err := birthOf(pid)
			if errors.Is(err, os.ErrNotExist) {
				continue // Exited after listing: it holds no memory now.
			}
			if err != nil || birth == "" {
				return 0, 0, errors.New("RSS member ownership unavailable")
			}
			if !belongs(pid, root) {
				continue
			}
			owned[pid] = true
			births[pid], owners[pid] = birth, root
		}
	}
	out, err := inventory()
	if err != nil {
		return 0, 0, err
	}
	for root, saved := range roots {
		birth, err := birthOf(root)
		if err != nil || birth != saved {
			return 0, 0, errors.New("RSS ownership changed")
		}
	}
	for pid, saved := range births {
		birth, err := birthOf(pid)
		if err != nil || birth != saved || !belongs(pid, owners[pid]) {
			delete(owned, pid) // The inventory row may belong to a successor.
		}
	}
	return ownedTreeRSS(out, owned, excluded)
}

// Only foreground dev uses this Linux fallback. Installed workers use cgroups.
func localDevProcessMemory() *processMemoryProbe {
	if runtime.GOOS != "linux" {
		return localProcessMemory()
	}
	return &processMemoryProbe{OwnedResident: ownedSessionResident,
		Pressure: func(context.Context) (*gpuHostState, error) { return readGPUHostState(osMemoryFS{}) }}
}
