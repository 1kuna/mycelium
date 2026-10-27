package myce

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var nativeMemoryMetricNames = []string{"memory.peak", "memory.max", "memory.events", "memory.current", "memory.stat"}

// Resolve inside the disposable helper, after attachment and namespace entry.
// The kernel supplies current membership; only the worker supplies other
// payload leaves in the same attempt. Neither source authorizes a directory.
func nativeCgroupMetrics(p nativeLinuxPolicy) ([]string, error) {
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return nil, fmt.Errorf("read current membership: %w", err)
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("read cgroup mounts: %w", err)
	}
	return nativeCgroupMetricFiles(membership, mounts, p)
}

func nativeCgroupMetricFiles(membership, mountinfo []byte, p nativeLinuxPolicy) ([]string, error) {
	mounts, err := nativeCgroupMounts(mountinfo)
	if err != nil {
		return nil, err
	}
	// Operator/runtime grants must not restore broad cgroup access through
	// another mount or symlink. Validate before adding the exact metrics.
	var mountPaths []string
	for _, mount := range mounts {
		mountPaths = append(mountPaths, mount.path)
	}
	for _, grants := range [][]string{p.Read, p.ReadOnly, p.Write} {
		if err := nativeReadOnlyOverlap(mountPaths, grants); err != nil {
			return nil, fmt.Errorf("runtime grant overlaps cgroup mount: %w", err)
		}
	}
	own, err := nativeCurrentCgroupAtMounts(membership, mounts)
	if err != nil {
		return nil, err
	}
	groups := append([]string{own}, p.MetricCgroups...)
	slices.Sort(groups)
	groups = slices.Compact(groups)
	files := make([]string, 0, 5*len(groups))
	for _, group := range groups {
		if !filepath.IsAbs(group) || filepath.Clean(group) != group {
			return nil, errors.New("invalid trusted cgroup metric leaf")
		}
		// memory.peak may be absent on older kernels or when the memory
		// controller is unavailable. Landlock skips absent exact files; it
		// must never replace them with a readable parent directory.
		for _, name := range nativeMemoryMetricNames {
			files = append(files, filepath.Join(group, name))
		}
	}
	return files, nil
}

// mountinfo roots need not be /: a delegated or namespaced cgroup2 mount may
// expose only a subtree. Map the membership relative to that mount's root,
// never by walking upward to find an ancestor with memory files.
func nativeCurrentCgroup(membership, mounts []byte) (string, error) {
	parsed, err := nativeCgroupMounts(mounts)
	if err != nil {
		return "", err
	}
	return nativeCurrentCgroupAtMounts(membership, parsed)
}

func nativeCurrentCgroupAtMounts(membership []byte, mounts []nativeCgroupMount) (string, error) {
	var group string
	for _, line := range strings.Split(string(membership), "\n") {
		if value, ok := strings.CutPrefix(line, "0::"); ok {
			if group != "" || !filepath.IsAbs(value) || filepath.Clean(value) != value {
				return "", errors.New("invalid current cgroup v2 membership")
			}
			group = value
		}
	}
	if group == "" {
		return "", errors.New("current cgroup v2 membership unavailable")
	}
	var own string
	rootLength := -1
	for _, mount := range mounts {
		rel, err := filepath.Rel(mount.root, group)
		if err != nil || rel != "." && !filepath.IsLocal(rel) {
			continue
		}
		if len(mount.root) > rootLength {
			own = filepath.Join(mount.path, rel)
			rootLength = len(mount.root)
		}
	}
	if own == "" {
		return "", errors.New("current cgroup v2 mount unavailable")
	}
	return own, nil
}

type nativeCgroupMount struct{ root, path string }

func nativeCgroupMounts(data []byte) ([]nativeCgroupMount, error) {
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	var mounts []nativeCgroupMount
	for _, line := range strings.Split(string(data), "\n") {
		before, after, ok := strings.Cut(line, " - ")
		if !ok || !strings.HasPrefix(after, "cgroup2 ") {
			continue
		}
		fields := strings.Fields(before)
		if len(fields) < 6 {
			return nil, errors.New("invalid cgroup v2 mountinfo")
		}
		root, mount := unescape.Replace(fields[3]), unescape.Replace(fields[4])
		if !filepath.IsAbs(root) || filepath.Clean(root) != root || !filepath.IsAbs(mount) || filepath.Clean(mount) != mount {
			return nil, errors.New("invalid cgroup v2 mount path")
		}
		mounts = append(mounts, nativeCgroupMount{root: root, path: mount})
	}
	return mounts, nil
}
