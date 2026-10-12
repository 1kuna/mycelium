package myce

import (
	"os"
	"path/filepath"
	"testing"
)

func TestQueueGPUDescriptors(t *testing.T) {
	for _, target := range []string{"/dev/null", "socket:[123]", "/dev/nvidia0", "/dev/nvidiactl", "/dev/dri/renderD128", "/dev/kfd", "/dev/accel/accel0", "/dev/nvidia0 (deleted)"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Symlink(target, filepath.Join(dir, "3")); err != nil {
				t.Fatal(err)
			}
			err := checkQueueGPUDescriptors(dir)
			allowed := target == "/dev/null" || target == "socket:[123]"
			if (err == nil) != allowed {
				t.Fatalf("target %s: %v", target, err)
			}
		})
	}
	if err := checkQueueGPUDescriptors(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing descriptor inventory accepted")
	}
}
