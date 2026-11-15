package myce

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"mycelium/internal/probeguard"
)

func localProcessMemory() *processMemoryProbe {
	return &processMemoryProbe{
		OwnedResident: func(roots map[int]string) (uint64, int, error) {
			probeguard.Check()
			if bytes, members, err, ok := darwinFastOwnedResident(roots); ok {
				return bytes, members, err
			}
			return ownedSessionResident(roots)
		},
		OwnedFootprint: darwinOwnedFootprintProbe(),
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
	var read func(int) ([2]uint64, string, error)
	if resident != nil {
		read = func(pid int) ([2]uint64, string, error) {
			rss, birth, err := resident(pid)
			return [2]uint64{rss}, birth, err
		}
	}
	total, members, err, ok := ownedPairFromTable(roots, rows, sid, read, birthOf)
	return total[0], members, err, ok
}

// ownedPairFromTable sums two counters read together per owned process (the
// footprint probe reads phys_footprint and RSS in one call).
func ownedPairFromTable(roots map[int]string, rows []darwinProcRow, sid func(int) (int, error), read func(int) ([2]uint64, string, error), birthOf func(int) (string, error)) ([2]uint64, int, error, bool) {
	var total [2]uint64
	byPID := make(map[int]darwinProcRow, len(rows))
	for _, r := range rows {
		byPID[r.pid] = r
	}
	for root, saved := range roots {
		r, ok := byPID[root]
		if !ok || saved == "" || r.zombie || r.birth != saved {
			return total, 0, errors.New("RSS ownership unavailable"), true
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
	members := 0
	for pid, birth := range owned {
		value, current, err := read(pid)
		if errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return [2]uint64{}, 0, nil, false
		}
		if current != birth {
			continue
		}
		total[0] += value[0]
		total[1] += value[1]
		members++
	}
	for root, saved := range roots {
		if birth, err := birthOf(root); err != nil || birth != saved {
			return [2]uint64{}, 0, errors.New("RSS ownership changed"), true
		}
	}
	return total, members, nil, true
}
