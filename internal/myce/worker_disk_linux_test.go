//go:build linux

package myce

import (
	"syscall"
	"testing"
)

func TestWorkerDiskLinuxUsesFragmentSize(t *testing.T) {
	st := syscall.Statfs_t{Bsize: 4096, Frsize: 1024}
	if got := diskBlockSize(&st); got != 1024 {
		t.Fatalf("used optimal I/O size instead of fragment size: %d", got)
	}
}
