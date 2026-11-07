package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func processMemoryHeadroom(available int64) (int64, error) {
	return cgroupMemoryHeadroom(available, os.ReadFile)
}

// Resolve the process's actual memory controller mount and namespace root.
// Both unified and v1 memory hierarchies can be tighter than host MemAvailable.
// An advertised but unreadable hierarchy is unknown capacity, never host credit.
func cgroupMemoryHeadroom(available int64, read func(string) ([]byte, error)) (int64, error) {
	unavailable := errors.New("process memory hierarchy unavailable")
	membership, err := read("/proc/self/cgroup")
	if err != nil {
		return 0, unavailable
	}
	var member string
	var unified, found bool
	var v1Member string
	for _, line := range strings.Split(string(membership), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == "0" && parts[1] == "" {
			member, unified, found = parts[2], true, true
		}
		for _, controller := range strings.Split(parts[1], ",") {
			if controller == "memory" {
				v1Member = parts[2]
			}
		}
	}
	if v1Member != "" {
		member, unified, found = v1Member, false, true
	}
	if !found {
		return available, nil
	} // no configured memory controller
	mounts, err := read("/proc/self/mountinfo")
	if err != nil {
		return 0, unavailable
	}
	for _, line := range strings.Split(string(mounts), "\n") {
		left, right, ok := strings.Cut(line, " - ")
		if !ok {
			continue
		}
		fields, fs := strings.Fields(left), strings.Fields(right)
		if len(fields) < 5 || len(fs) < 3 {
			continue
		}
		matches := unified && fs[0] == "cgroup2"
		if !unified && fs[0] == "cgroup" {
			for _, option := range strings.Split(fs[2], ",") {
				matches = matches || option == "memory"
			}
		}
		if !matches {
			continue
		}
		decode := func(v string) (string, error) { return strconv.Unquote(`"` + v + `"`) }
		root, e1 := decode(fields[3])
		mount, e2 := decode(fields[4])
		if e1 != nil || e2 != nil {
			return 0, unavailable
		}
		relative := strings.TrimPrefix(member, root)
		if root != "/" && member != root && !strings.HasPrefix(member, root+"/") {
			if member != "/" {
				continue
			}
			relative = "" // cgroup namespace root
		}
		current := filepath.Join(mount, filepath.Clean("/"+relative))
		maxName, usedName := "memory.max", "memory.current"
		if !unified {
			maxName, usedName = "memory.limit_in_bytes", "memory.usage_in_bytes"
		}
		for {
			maxRaw, e1 := read(filepath.Join(current, maxName))
			usedRaw, e2 := read(filepath.Join(current, usedName))
			// A unified node without the memory controller has neither file;
			// its nearest enabled ancestor can still constrain the process.
			missingController := unified && errors.Is(e1, os.ErrNotExist) && errors.Is(e2, os.ErrNotExist)
			// The real hierarchy root has no configurable memory.max. A
			// delegated/bind-mounted root must expose its physical boundary.
			if unified && current == mount && root == "/" && errors.Is(e1, os.ErrNotExist) && (e2 == nil || missingController) {
				return available, nil
			}
			if missingController && current != mount {
				current = filepath.Dir(current)
				continue
			}
			if e1 != nil || e2 != nil {
				return 0, unavailable
			}
			maxText := strings.TrimSpace(string(maxRaw))
			used, e := strconv.ParseInt(strings.TrimSpace(string(usedRaw)), 10, 64)
			if e != nil || used < 0 {
				return 0, unavailable
			}
			if maxText != "max" {
				limit, e := strconv.ParseInt(maxText, 10, 64)
				if e != nil || limit < 0 {
					return 0, unavailable
				}
				available = min(available, max(0, limit-used))
			}
			if current == mount {
				return available, nil
			}
			current = filepath.Dir(current)
			if current != mount && !strings.HasPrefix(current, mount+"/") {
				return 0, unavailable
			}
		}
	}
	return 0, unavailable
}
