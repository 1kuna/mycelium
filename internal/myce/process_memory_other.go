//go:build !darwin

package myce

import "syscall"

// Linux attempts use cgroup accounting; other hosts have no sampler.
func localProcessMemory() *processMemoryProbe { return nil }

// Linux and the BSDs report ru_maxrss in KiB.
func rusageMaxRSSBytes(ru *syscall.Rusage) float64 { return float64(ru.Maxrss) * 1024 }
