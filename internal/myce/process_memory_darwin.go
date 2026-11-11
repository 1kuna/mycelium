package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

func localProcessMemory() *processMemoryProbe {
	return &processMemoryProbe{
		OwnedResident: func(roots map[int]string) (uint64, int, error) {
			probeguard.Check()
			if bytes, members, err, ok := darwinFastOwnedResident(roots); ok {
				return bytes, members, err
			}
			return darwinOwnedResident(roots)
		},
		Pressure: func(ctx context.Context) (*gpuHostState, error) {
			probeguard.Check()
			if state, err, ok := darwinFastPressure(); ok {
				return state, err
			}
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

// Keep the existing RSS inventory and cleanup session boundary. Root birth
// checks fence reuse both before and during the inventory; a changed root makes
// the sample unknown. Members are fenced individually: one that exits, is
// replaced, or leaves the session between the membership list and the RSS
// inventory is dropped from this sample, never charged under a reused PID.
// Failing the whole sample instead let a job that forks short-lived helpers
// reset the guard on every observation and grow without bound (measured:
// 256 MiB/s with /usr/bin/true churn was never stopped).
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

type darwinProcRow struct {
	pid, ppid, pgid int
	birth           string
	zombie          bool
}

// One process-table snapshot supplies membership (process group or session),
// descendants and births together. Each owned process's resident size is read
// with its birth in one call; a successor under a reused PID, or a process
// that exited, is skipped rather than charged. Roots stay strictly fenced.
// ok=false asks for the ps fallback (an owned process could not be inspected).
func ownedResidentFromTable(roots map[int]string, rows []darwinProcRow, sid func(int) (int, error), resident func(int) (uint64, string, error), birthOf func(int) (string, error)) (uint64, int, error, bool) {
	byPID := make(map[int]darwinProcRow, len(rows))
	for _, r := range rows {
		byPID[r.pid] = r
	}
	for root, saved := range roots {
		r, ok := byPID[root]
		if !ok || saved == "" || r.zombie || r.birth != saved {
			return 0, 0, errors.New("RSS ownership unavailable"), true
		}
	}
	owned := map[int]string{}
	for _, r := range rows {
		if r.zombie || roots[r.pid] != "" {
			continue
		}
		member := roots[r.pgid] != ""
		if !member {
			session, err := sid(r.pid)
			member = err == nil && roots[session] != ""
		}
		if member {
			owned[r.pid] = r.birth
		}
	}
	for changed := true; changed; {
		changed = false
		for _, r := range rows {
			if _, done := owned[r.pid]; done || r.zombie || roots[r.pid] != "" {
				continue
			}
			if _, parent := owned[r.ppid]; parent || roots[r.ppid] != "" {
				owned[r.pid] = r.birth
				changed = true
			}
		}
	}
	var total uint64
	members := 0
	for pid, birth := range owned {
		rss, current, err := resident(pid)
		if errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return 0, 0, nil, false
		}
		if current != birth {
			continue
		}
		total += rss
		members++
	}
	for root, saved := range roots {
		if birth, err := birthOf(root); err != nil || birth != saved {
			return 0, 0, errors.New("RSS ownership changed"), true
		}
	}
	return total, members, nil, true
}
