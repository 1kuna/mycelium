package myce

import (
	"path/filepath"
	"strings"
)

// DeviceInstall stores each selected device's capacity contract.
type DeviceInstall struct {
	Selector  string    `json:"selector"`
	Detection Detection `json:"detection"`
}

func ValidateDeviceSettings(settings []DeviceInstall) error {
	seen := map[string]bool{}
	for _, s := range settings {
		d := s.Detection
		if s.Selector == "" || singleDeviceMask(s.Selector) != nil || seen[s.Selector] || !validGPU(d.Platform) || !validMemoryValue(d.MemoryGB) || !validMemoryValue(d.ReserveGB) || d.MemoryMode == "fixed" && d.MemoryGB <= d.ReserveGB {
			return migrationError("machine_migration_conflict", "invalid per-device settings")
		}
		if d.MemoryMode != "auto" && d.MemoryMode != "fixed" && d.MemoryMode != "unified" && d.MemoryMode != "dedicated" {
			return migrationError("machine_migration_conflict", "invalid device memory mode")
		}
		seen[s.Selector] = true
	}
	return nil
}

// CheckMachineInstallGate refuses retired per-device service installations.
// Converters for releases below the supported floor live only in source history.
func CheckMachineInstallGate(c ServiceConfig) error {
	if c.Role != "worker" || c.Detection.OS != "linux" {
		return nil
	}
	if strings.Contains(filepath.Base(c.Service), "@") {
		return &HTTPError{Status: 409, Code: "machine_worker_conflict", Message: "split worker installations are below the supported release floor; archive and install a machine worker"}
	}
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(c.Service), "myce-worker@*.service"))
	if err != nil {
		return err
	}
	links, err := filepath.Glob(filepath.Join(filepath.Dir(c.Service), "*.target.wants", "myce-worker@*.service"))
	if err != nil {
		return err
	}
	for _, p := range append(paths, links...) {
		if filepath.Base(p) != "myce-worker@.service" {
			return &HTTPError{Status: 409, Code: "machine_worker_conflict", Message: "retired split worker service present; archive before a fresh machine installation"}
		}
	}
	return nil
}

func migrationError(code, detail string) error {
	return &HTTPError{Status: 409, Code: code, Message: code + ": " + detail}
}
