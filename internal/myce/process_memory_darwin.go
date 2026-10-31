package myce

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"mycelium/internal/probeguard"
)

func localProcessMemory() *processMemoryProbe {
	return &processMemoryProbe{
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
