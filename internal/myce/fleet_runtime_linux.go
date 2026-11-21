package myce

import (
	"context"
	"fmt"
	"os"
)

// The kernel retains this process's executable inode across rename/unlink.
func openFleetRunningImage() (*os.File, error) { return os.Open("/proc/self/exe") }
func verifyFleetRunningImage(*os.File) error   { return nil }

func verifyInstallServiceImage(_ context.Context, pid int, slot *os.File, _ func(context.Context, string, ...string) ([]byte, error)) error {
	image, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return err
	}
	info, err := slot.Stat()
	if err != nil || !os.SameFile(info, image) {
		return fleetProofImageMismatch
	}
	return nil
}
