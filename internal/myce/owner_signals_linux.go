package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"
)

func LocalOwnerSignals(ctx context.Context) OwnerSignals {
	p := linuxOwnerProbe{read: os.ReadFile, glob: func(pattern string) ([]string, error) {
		if strings.HasPrefix(pattern, "/sys/class/power_supply/") {
			if _, err := os.ReadDir("/sys/class/power_supply"); err != nil {
				return nil, err
			}
		}
		return filepath.Glob(pattern)
	}, command: LocalMachineProbe().Command}
	return p.detect(ctx)
}
