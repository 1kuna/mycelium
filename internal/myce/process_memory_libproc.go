//go:build darwin && !cgo

package myce

import "time"

// Non-cgo Darwin fallback for process_memory_libproc_darwin.go. Without libproc every sample spawns ps and the pressure probes (~70 ms).
const processMemorySamplePeriod = time.Second

func darwinFastOwnedResident(map[int]string) (uint64, int, error, bool) { return 0, 0, nil, false }

func darwinFastPressure() (*gpuHostState, error, bool) { return nil, nil, false }
