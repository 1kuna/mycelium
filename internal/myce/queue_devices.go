package myce

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// CheckQueueGPUDescriptors refuses inherited GPU handles; the queue never opens GPU devices.
// shortcut: this is not device isolation, use a system service sandbox if isolation is required.
func CheckQueueGPUDescriptors() error {
	if runtime.GOOS != "linux" {
		return nil
	}
	return checkQueueGPUDescriptors("/proc/self/fd")
}

func checkQueueGPUDescriptors(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("inspect queue GPU descriptors: %w", err)
	}
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(dir, entry.Name()))
		// Go's directory enumeration descriptor may already be closed.
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect queue descriptor: %w", err)
		}
		if strings.HasPrefix(target, "/dev/nvidia") || strings.HasPrefix(target, "/dev/dri/") || strings.HasPrefix(target, "/dev/accel/") || target == "/dev/kfd" || target == "/dev/kfd (deleted)" {
			return fmt.Errorf("queue must not hold GPU devices: inherited descriptor %s", entry.Name())
		}
	}
	return nil
}
