package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const cudaHostMemorySource = "CUDA proc physical upper bound"

// NVIDIA's compute-app counters describe device allocations, not host pins.
// Linux does not expose an exhaustive per-client NVIDIA host allocation total.
// Bound uncharged allocations by ABSOLUTE whole-host non-cache occupancy, not
// a delta that unrelated frees could mask. Subtract only root anon/shmem: both
// are included in that physical figure and charged once across disjoint cgroups.
// Kernel allocations, unrelated host use and possible CUDA overlap stay in the
// residual. This deliberately cannot establish a host/device physical union.
func (a *memoryAttempt) cudaHostMemory() (driver, pinned, physical float64, seen bool, err error) {
	ids := map[int]bool{}
	if err = a.processIDs(ids); err != nil {
		err = hostMemoryReadError{"cuda_process_inventory", err}
		return
	}
	seen = len(ids) > 0
	for pid := range ids {
		var b []byte
		b, err = a.fs.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
		if errors.Is(err, os.ErrNotExist) {
			// cgroup.procs and proc status are not an atomic snapshot. A PID
			// that has left this exact subtree no longer owns live proc pins;
			// the absolute host/pin counters below still bound residual pages.
			live := map[int]bool{}
			if inventoryErr := a.processIDs(live); inventoryErr == nil && !live[pid] {
				continue
			}
		}
		if err != nil {
			err = hostMemoryReadError{"cuda_proc_status", err}
			return
		}
		var values map[string]uint64
		values, err = memoryKiBCounters(b, "VmPin", "VmLck")
		if err != nil {
			// exit_mm can remove these fields before cgroup_exit and before
			// the status file disappears. As for ENOENT, skip only after a
			// successful fresh inventory proves this PID left the subtree.
			// State alone is insufficient: a zombie leader may have live
			// threads still owning the process's memory map and pins.
			live := map[int]bool{}
			if inventoryErr := a.processIDs(live); inventoryErr == nil && !live[pid] {
				err = nil
				continue
			}
			var counter memoryKiBCounterError
			errors.As(err, &counter)
			err = hostMemoryReadError{"cuda_proc_pin_counters", cudaProcPinReadError{pid, procStatusState(b), counter}}
			return
		}
		// May overlap one another, cgroup anon and inherited mappings. Never
		// deduct that overlap without a synchronized unique-page measurement.
		pinned += (float64(values["VmPin"]) + float64(values["VmLck"])) / (1 << 20)
	}
	root := func() (float64, error) {
		// Detached scopes are siblings, not children of this attempt's
		// parent. Use the same disjoint worker/jobs roots as delta attribution,
		// retaining only anon/shmem: kernel and pins still belong in the bound.
		total := 0.0
		for _, path := range a.accountingPaths() {
			b, e := a.fs.ReadFile(filepath.Join(path, "memory.stat"))
			if e != nil {
				return 0, e
			}
			stats := memoryCounters(b)
			anon, aok := stats["anon"]
			shmem, sok := stats["shmem"]
			if !aok || !sok {
				return 0, errAttemptMemoryUnsupported
			}
			total += (float64(anon) + float64(shmem)) / (1 << 30)
		}
		return total, nil
	}
	before, e := root()
	if e != nil {
		err = hostMemoryReadError{"cuda_root_components", e}
		return
	}
	b, e := a.fs.ReadFile("/proc/meminfo")
	if e != nil {
		err = hostMemoryReadError{"cuda_physical_counters", e}
		return
	}
	v, e := memoryKiBCounters(b, "MemTotal", "MemFree", "Buffers", "Cached", "Shmem", "SReclaimable", "Unevictable", "Mlocked", "Dirty", "Writeback")
	if e != nil || v["Shmem"] > v["Cached"] || v["MemFree"] > v["MemTotal"] {
		err = hostMemoryReadError{"cuda_physical_counters", errAttemptMemoryUnsupported}
		return
	}
	// Put shmem back before excluding file cache. Add locked/dirty pages
	// conservatively, even where their counters overlap retained occupancy.
	cache := float64(v["Buffers"]) + float64(v["Cached"]-v["Shmem"]) + float64(v["SReclaimable"])
	used := float64(v["MemTotal"] - v["MemFree"])
	if cache > used {
		err = hostMemoryReadError{"cuda_physical_counters", errAttemptMemoryUnsupported}
		return
	}
	physical = (used - cache + float64(v["Unevictable"]) + float64(v["Mlocked"]) + float64(v["Dirty"]) + float64(v["Writeback"])) / (1 << 20)
	b, err = a.fs.ReadFile("/proc/vmstat")
	if err != nil {
		err = hostMemoryReadError{"cuda_global_pin_counters", err}
		return
	}
	stats := memoryCounters(b)
	acquired, aok := stats["nr_foll_pin_acquired"]
	released, rok := stats["nr_foll_pin_released"]
	if !aok || !rok || released > acquired || a.pageSize <= 0 {
		err = hostMemoryReadError{"cuda_global_pin_counters", errAttemptMemoryUnsupported}
		return
	}
	// GUP-pinned file pages need not appear as unevictable. Retain all
	// outstanding host pins, including unrelated ones and anon overlap.
	pinned += float64(acquired-released) * float64(a.pageSize) / (1 << 30)
	after, e := root()
	if e != nil || min(before, after) > physical {
		err = hostMemoryReadError{"cuda_root_physical_comparison", errAttemptMemoryUnsupported}
		return
	}
	// Read around the host snapshot and subtract the smaller value. Sibling
	// exits cannot hide driver pages through a net MemAvailable delta.
	driver = physical - min(before, after) + pinned
	return
}

// Only fixed field names/classes and the single-letter state leave the worker;
// no status dump, command, raw value or path is retained.
type cudaProcPinReadError struct {
	pid     int
	state   string
	counter memoryKiBCounterError
}

func (e cudaProcPinReadError) Error() string { return e.counter.Error() }

func procStatusState(b []byte) string {
	for _, line := range strings.Split(string(b), "\n") {
		if state, ok := strings.CutPrefix(line, "State:"); ok {
			f := strings.Fields(state)
			if len(f) > 0 && len(f[0]) == 1 && strings.Contains("RSDTtXZPIWK", f[0]) {
				return f[0]
			}
		}
	}
	return "unknown"
}

type memoryKiBCounterError struct{ name, reason string }

func (e memoryKiBCounterError) Error() string { return "memory counter " + e.name + ": " + e.reason }

func memoryKiBCounters(b []byte, required ...string) (map[string]uint64, error) {
	values := map[string]uint64{}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) != 3 || f[2] != "kB" {
			continue
		}
		n, err := strconv.ParseUint(f[1], 10, 64)
		if err == nil {
			values[strings.TrimSuffix(f[0], ":")] = n
		}
	}
	for _, name := range required {
		if _, ok := values[name]; !ok {
			reason := "missing"
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, name+":") {
					reason = "malformed"
					break
				}
			}
			return nil, memoryKiBCounterError{name, reason}
		}
	}
	return values, nil
}
