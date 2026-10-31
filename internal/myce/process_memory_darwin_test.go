package myce

import (
	"context"
	"strings"
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
