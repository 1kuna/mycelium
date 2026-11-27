package myce

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// Installed queue, worker and custodian units share a systemd manager. Only
// follow slices to managed units. A worker leaf excludes sibling payload,
// build and model cgroups, whose CPU is already charged as admitted work.
func installedControlCPUSampler(service ServiceConfig) *controlCPUSampler {
	if runtime.GOOS != "linux" {
		return nil
	}
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return nil
	}
	unit := filepath.Base(service.Service)
	for _, line := range strings.Split(string(membership), "\n") {
		cgroup, ok := strings.CutPrefix(line, "0::/")
		if !ok || filepath.Base(cgroup) != unit {
			continue
		}
		path := filepath.Join("/sys/fs/cgroup", cgroup)
		root := filepath.Dir(path)
		for strings.HasSuffix(filepath.Base(root), ".slice") {
			root = filepath.Dir(root)
		}
		return &controlCPUSampler{read: func() (map[string]controlCPUCounter, error) {
			return readControlCPUCounters(root, path)
		}}
	}
	return nil
}

func readControlCPUCounters(root, queue string) (map[string]controlCPUCounter, error) {
	counters := make(map[string]controlCPUCounter)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // Unit exited during enumeration.
		}
		if err != nil || !d.IsDir() {
			return err
		}
		name := d.Name()
		if path == root || strings.HasSuffix(name, ".slice") {
			return nil
		}
		leaf := path
		switch {
		case path == queue:
		case ValidWorkerUnit(name), isControlCPUCustodian(name):
			leaf = filepath.Join(path, "worker")
		default:
			return fs.SkipDir
		}
		group, err := os.OpenRoot(leaf)
		if errors.Is(err, fs.ErrNotExist) {
			return fs.SkipDir
		}
		if err != nil {
			return err
		}
		info, err := group.Stat(".")
		if err != nil {
			_ = group.Close()
			return err
		}
		data, err := group.ReadFile("cpu.stat")
		_ = group.Close()
		if errors.Is(err, fs.ErrNotExist) {
			return fs.SkipDir
		}
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(data), "\n") {
			if value, ok := strings.CutPrefix(line, "usage_usec "); ok {
				usecs, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					return err
				}
				counters[leaf] = controlCPUCounter{file: info, usecs: usecs}
				return fs.SkipDir
			}
		}
		return errors.New("control cgroup cpu.stat lacks usage_usec")
	})
	if _, ok := counters[queue]; err == nil && !ok {
		err = errors.New("queue cgroup CPU counter unavailable")
	}
	return counters, err
}

func isControlCPUCustodian(unit string) bool {
	id, ok := strings.CutPrefix(unit, "myce-attempt-")
	id, scope := strings.CutSuffix(id, ".scope")
	return ok && scope && validCustodianID(id)
}
