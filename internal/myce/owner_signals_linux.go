package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

func LocalOwnerSignals(ctx context.Context) OwnerSignals {
	probeguard.Check()
	p := linuxOwnerProbe{host: configuredHostOwnerSignals, read: os.ReadFile, glob: func(pattern string) ([]string, error) {
		if strings.HasPrefix(pattern, "/sys/class/power_supply/") {
			if _, err := os.ReadDir("/sys/class/power_supply"); err != nil {
				return nil, err
			}
		}
		return filepath.Glob(pattern)
	}, command: LocalMachineProbe().Command, monotonic: func() (float64, error) {
		var stamp unix.Timespec
		if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &stamp); err != nil {
			return 0, err
		}
		return float64(stamp.Sec) + float64(stamp.Nsec)/1e9, nil
	}}
	return p.detect(ctx)
}
