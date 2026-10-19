package myce

import (
	"errors"
	"os"
	"path/filepath"
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
