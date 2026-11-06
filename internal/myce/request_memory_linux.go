package myce

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// A container's physical boundary can be smaller than host MemAvailable. Read
// the unified cgroup membership and each ancestor limit, including namespace root.
func processMemoryHeadroom(available int64) int64 {
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return available
	}
	for _, line := range strings.Split(string(membership), "\n") {
		path, ok := strings.CutPrefix(line, "0::")
		if !ok {
			continue
		}
		root := "/sys/fs/cgroup"
		current := filepath.Join(root, filepath.Clean("/"+path))
		for {
			maxRaw, e1 := os.ReadFile(filepath.Join(current, "memory.max"))
			usedRaw, e2 := os.ReadFile(filepath.Join(current, "memory.current"))
			limit, e3 := strconv.ParseInt(strings.TrimSpace(string(maxRaw)), 10, 64)
			used, e4 := strconv.ParseInt(strings.TrimSpace(string(usedRaw)), 10, 64)
			if e1 == nil && e2 == nil && e3 == nil && e4 == nil {
				available = min(available, max(0, limit-used))
			}
			if current == root {
				break
			}
			current = filepath.Dir(current)
			if !strings.HasPrefix(current, root) {
				break
			}
		}
	}
	return available
}
