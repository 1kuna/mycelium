package myce

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

func ValidWorkerUnit(unit string) bool {
	if unit == "myce-worker.service" {
		return true
	}
	if !strings.HasPrefix(unit, "myce-worker@") || !strings.HasSuffix(unit, ".service") {
		return false
	}
	instance := strings.TrimSuffix(strings.TrimPrefix(unit, "myce-worker@"), ".service")
	if instance == "" {
		return false
	}
	for _, c := range instance {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// Instance state reuses the generation/receipt contract, but never fences the queue's entire physical host.
func WorkerUnitDirectory(dir, unit string) (string, error) {
	if !ValidWorkerUnit(unit) {
		return "", errors.New("refusing non-Mycelium worker unit")
	}
	if dir == "" {
		var err error
		dir, err = HostStateDirectory()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(dir, "units", unit), nil
}
func unitParent(dir string) string {
	if filepath.Base(filepath.Dir(dir)) == "units" && ValidWorkerUnit(filepath.Base(dir)) {
		return filepath.Dir(filepath.Dir(dir))
	}
	return ""
}

func HostWorkers(dir string) ([]HostWorker, error) { return (HostControl{Dir: dir}).workers() }
func WorkerUnits(dir, target string) ([]string, error) {
	if target != "" && !ValidWorkerUnit(target) {
		return nil, errors.New("refusing non-Mycelium worker unit")
	}
	workers, err := HostWorkers(dir)
	if err != nil {
		return nil, err
	}
	var units []string
	for _, w := range workers {
		if ValidWorkerUnit(w.Unit) && !slices.Contains(units, w.Unit) && (target == "" || target == w.Unit) {
			units = append(units, w.Unit)
		}
	}
	if target != "" && len(units) == 0 {
		return nil, errors.New("worker unit is not registered in this host control directory")
	}
	slices.Sort(units)
	return units, nil
}

// A systemd invocation's kernel cgroup supplies custody even for manually installed units.
func CurrentWorkerUnit() (string, error) {
	if os.Getenv("INVOCATION_ID") == "" {
		return "", nil
	}
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	if unit := workerUnitFromCgroup(b); unit != "" {
		return unit, nil
	}
	return "", errors.New("systemd worker invocation has no allowed worker cgroup")
}
func workerUnitFromCgroup(b []byte) string {
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 || !(fields[0] == "0" && fields[1] == "" || strings.Contains(","+fields[1]+",", ",name=systemd,")) {
			continue
		}
		path := filepath.Clean(fields[2])
		if filepath.Base(path) == "worker" {
			path = filepath.Dir(path)
		}
		if unit := filepath.Base(path); ValidWorkerUnit(unit) {
			return unit
		}
	}
	return ""
}
