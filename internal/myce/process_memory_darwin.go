package myce

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

func localProcessMemory() *processMemoryProbe {
	return &processMemoryProbe{
		OwnedResident: darwinOwnedResident,
		Pressure: func(ctx context.Context) (*gpuHostState, error) {
			probeguard.Check()
			return readDarwinProcessPressure(ctx, func(ctx context.Context, name string, args ...string) ([]byte, error) {
				cmd := exec.CommandContext(ctx, name, args...)
				cmd.WaitDelay = attemptMemorySamplePeriod
				return cmd.Output()
			})
		},
		Resident: func(groups map[int]bool) (uint64, int, error) {
			probeguard.Check()
			out, err := processPS("-ax", "-o", "pid=,pgid=,rss=")
			if err != nil {
				return 0, 0, err
			}
			return processGroupResident(out, groups)
		},
	}
}

// Darwin reports ru_maxrss in bytes.
func rusageMaxRSSBytes(ru *syscall.Rusage) float64 { return float64(ru.Maxrss) }

func readDarwinProcessPressure(ctx context.Context, command func(context.Context, string, ...string) ([]byte, error)) (*gpuHostState, error) {
	b, err := command(ctx, "/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level")
	if err != nil {
		return nil, err
	}
	level, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || level != 1 && level != 2 && level != 4 {
		return nil, fmt.Errorf("memory pressure level unavailable")
	}
	b, err = command(ctx, "/usr/bin/vm_stat")
	if err != nil {
		return nil, err
	}
	available, err := parseVMStat(b)
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if value, ok := strings.CutPrefix(line, "Swapouts:"); ok {
			n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(value), "."), 10, 64)
			if err == nil {
				return &gpuHostState{available: available, swapOut: n, level: level}, nil
			}
		}
	}
	return nil, fmt.Errorf("swap-out counter unavailable")
}

// Keep the existing RSS inventory and cleanup session boundary. Birth checks
// fence reuse both before and during the inventory; missing evidence is unknown.
func darwinOwnedResident(roots map[int]string) (uint64, int, error) {
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
			if err != nil || birth == "" || !belongs(pid, root) {
				return 0, 0, errors.New("RSS member ownership unavailable")
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
			return 0, 0, errors.New("RSS member ownership changed")
		}
	}
	return ownedTreeRSS(out, owned, excluded)
}
