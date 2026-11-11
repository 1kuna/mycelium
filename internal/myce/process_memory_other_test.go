//go:build !darwin

package myce

import (
	"syscall"
	"testing"
)

// Linux attempts use cgroup accounting, so there is no RSS sampler, and the
// kernel reports ru_maxrss in KiB (Darwin reports bytes).
func TestProcessMemoryOtherPlatforms(t *testing.T) {
	if localProcessMemory() != nil {
		t.Fatal("non-Darwin host installed an RSS sampler")
	}
	if got := rusageMaxRSSBytes(&syscall.Rusage{Maxrss: 3}); got != 3*1024 {
		t.Fatalf("ru_maxrss KiB conversion: %v", got)
	}
}
