package myce

import "os"

// The kernel retains this process's executable inode across rename/unlink.
func openFleetRunningImage() (*os.File, error) { return os.Open("/proc/self/exe") }
func verifyFleetRunningImage(*os.File) error   { return nil }
