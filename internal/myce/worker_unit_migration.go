package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const workerUnitMigrationFile = "worker-unit-migration.json"
const workerUnitMigrationDropIn = "50-myce-memory-containment.conf"

func (c ServiceConfig) workerContainmentSettings() string {
	return "OOMScoreAdjust=0\nSlice=" + c.workerSlice() + "\n"
}

// StageWorkerUnitMigration changes only the next invocation's placement. It
// deliberately never stops, restarts or moves a process. Activation is the
// existing fleet upgrade's MY-55 takeover, with its queue custody fences; an
// install rerun or an uncoordinated systemctl restart is not an activation path.
func StageWorkerUnitMigration(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) (*MemoryContainment, error) {
	if c.Role != "worker" || c.Detection.OS != "linux" || !filepath.IsAbs(c.Service) || !filepath.IsAbs(c.State) || !ValidWorkerUnit(filepath.Base(c.Service)) || c.validateWorkerSlice() != nil {
		return nil, errors.New("worker_unit_migration_unsupported")
	}
	unlock, err := commandLock(c.State, "worker-unit-migration.lock", false)
	if err != nil {
		return nil, errors.New("worker_unit_migration_busy")
	}
	defer unlock()
	status := &MemoryContainment{State: "unavailable"}
	finish := func(code, next string, cause error) (*MemoryContainment, error) {
		status.Reason, status.Next = code, next
		err := saveWorkerJSON(c.State, workerUnitMigrationFile, status)
		if cause != nil {
			// Manager output and paths stay local, never in typed public status.
			return status, errors.Join(errors.New(code), err)
		}
		return status, err
	}
	refuse := func(code string) (*MemoryContainment, error) {
		return finish(code, "resolve the unit migration refusal and rerun myce migrate worker-unit", errors.New(code))
	}
	inspect := func() (map[string]string, error) {
		b, err := command(ctx, "systemctl", "--user", "show", filepath.Base(c.Service), "--property=FragmentPath,DropInPaths,Slice,OOMScoreAdjust,ActiveState,ControlGroup")
		if err != nil {
			return nil, err
		}
		v := map[string]string{}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			key, value, ok := strings.Cut(line, "=")
			if _, duplicate := v[key]; !ok || duplicate {
				return nil, errors.New("invalid manager observation")
			}
			v[key] = value
		}
		for _, key := range []string{"FragmentPath", "DropInPaths", "Slice", "OOMScoreAdjust", "ActiveState", "ControlGroup"} {
			if _, ok := v[key]; !ok {
				return nil, errors.New("incomplete manager observation")
			}
		}
		return v, nil
	}
	before, err := inspect()
	if err != nil || before["FragmentPath"] != c.Service || before["ActiveState"] != "active" || filepath.Base(before["ControlGroup"]) != filepath.Base(c.Service) {
		return refuse("worker_unit_migration_identity_unconfirmed")
	}
	path := filepath.Join(c.Service+".d", workerUnitMigrationDropIn)
	text := []byte("# Managed by myce migrate worker-unit; activate through fleet upgrade.\n[Service]\n" + c.workerContainmentSettings())
	read := func(path string) ([]byte, error) {
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || st.Size() > 1<<20 {
			return nil, errors.New("unit file unavailable")
		}
		return os.ReadFile(path)
	}
	// Never overwrite an operator's file, even at our reserved name. This also
	// makes retries after a crash between publication and daemon-reload safe.
	existing, err := os.ReadFile(path)
	if err == nil && string(existing) != string(text) || err != nil && !errors.Is(err, os.ErrNotExist) {
		return refuse("worker_unit_migration_dropin_conflict")
	}
	ownExists := err == nil
	if ownExists {
		if _, err := read(path); err != nil {
			return refuse("worker_unit_migration_dropin_conflict")
		}
	}
	paths := append([]string{c.Service}, strings.Fields(before["DropInPaths"])...)
	for _, p := range paths {
		if !filepath.IsAbs(p) || strings.Contains(p, "\\") {
			return refuse("worker_unit_migration_override_unreadable")
		}
		data, err := read(p)
		if err != nil {
			return refuse("worker_unit_migration_override_unreadable")
		}
		if p == path {
			if string(data) != string(text) {
				return refuse("worker_unit_migration_dropin_conflict")
			}
			continue
		}
		section := ""
		for _, line := range strings.Split(strings.ReplaceAll(strings.ReplaceAll(string(data), "\r\n", "\n"), "\\\n", " "), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "[") {
				section = line
			}
			key, value, ok := strings.Cut(line, "=")
			if !ok || section != "[Service]" {
				continue
			}
			key, value = strings.TrimSpace(key), strings.TrimSpace(value)
			if key == "Slice" && value != c.workerSlice() && (p != c.Service || value != "app.slice" && value != "") || key == "OOMScoreAdjust" && value != "0" && (p != c.Service || value != "200") {
				return refuse("worker_unit_migration_override_conflict")
			}
		}
	}
	if !ownExists {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return refuse("worker_unit_migration_write_failed")
		}
		if err := writeInstallFile(path, text, 0600, installFiles{}, nil); err != nil {
			return refuse("worker_unit_migration_write_failed")
		}
	}
	// Reload does not reparent an active service. Verify both the future
	// settings and the unchanged current cgroup before reporting staging.
	_, reloadErr := command(ctx, "systemctl", "--user", "daemon-reload")
	after, inspectErr := inspect()
	if reloadErr != nil || inspectErr != nil || after["Slice"] != c.workerSlice() || after["OOMScoreAdjust"] != "0" || after["ControlGroup"] != before["ControlGroup"] || after["ActiveState"] != "active" {
		return finish("worker_unit_migration_activation_unconfirmed", "keep serving; inspect the staged drop-in and rerun myce migrate worker-unit before fleet upgrade", errors.New("reload unconfirmed"))
	}
	if _, err := jointMemoryRoot(filepath.Join("/sys/fs/cgroup", after["ControlGroup"]), c.workerSlice()); err == nil {
		// Available is reported only by the worker after runtime MemoryMax
		// readback. Placement alone is not containment proof.
		return finish("worker_unit_migration_placement_ready", "verify worker memory_containment is available after runtime MemoryMax readback", nil)
	}
	return finish("worker_unit_migration_staged", "activate through a normal myce fleet upgrade; the existing MY-55 takeover preserves running work", nil)
}

func (w Worker) unmigratedContainmentStatus() *MemoryContainment {
	status := unmigratedMemoryContainment()
	if w.ServiceConfig == nil {
		return status
	}
	var saved MemoryContainment
	if readCustodianJSON(w.ServiceConfig.State, workerUnitMigrationFile, &saved) == nil && saved.State == "unavailable" && strings.HasPrefix(saved.Reason, "worker_unit_migration_") {
		return &saved
	}
	return status
}
