package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func (c ServiceConfig) workerSlice() string {
	if c.Slice == "" {
		return "myce.slice"
	}
	return c.Slice
}

func (c ServiceConfig) custodianSlice() string {
	return strings.TrimSuffix(c.workerSlice(), ".slice") + "-jobs.slice"
}

func (c ServiceConfig) validateWorkerSlice() error {
	name := strings.TrimSuffix(c.workerSlice(), ".slice")
	if name == c.workerSlice() || name == "" || strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") || strings.Contains(name, "--") {
		return errors.New("invalid worker slice name")
	}
	for _, ch := range name {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_') {
			return errors.New("invalid worker slice name")
		}
	}
	return nil
}

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

// A custodian is deliberately outside the worker service. Validate its exact
// manager identity rather than broadening the worker-service allowlist.
func CurrentWorkerUnit(custodian string) (string, error) {
	if custodian != "" {
		if !validCustodianID(custodian) {
			return "", errors.New("invalid detached attempt identity")
		}
		_, err := ownCustodianIsolation(custodian)
		return "", err
	}
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
