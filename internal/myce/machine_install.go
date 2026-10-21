package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"syscall"
)

// DeviceInstall preserves each source worker's selector and capacity contract.
type DeviceInstall struct {
	Selector  string    `json:"selector"`
	Detection Detection `json:"detection"`
}

func migrationError(code, detail string) error {
	return &HTTPError{Status: 409, Code: code, Message: code + ": " + detail}
}

// PlanMachineInstall is read-only: explicit sources establish ownership. Cache
// artifacts remain where they are; gpu0 supplies the single machine cache.
func PlanMachineInstall(destination string, paths []string, explicit map[string]bool) (ServiceConfig, error) {
	var zero ServiceConfig
	if data, err := os.ReadFile(destination + ".m4-rollback.json"); err == nil {
		var j workerMigration
		if err = json.Unmarshal(data, &j); err != nil {
			return zero, err
		}
		if j.Phase == "complete" {
			wanted := slices.Clone(paths)
			known := []string{}
			for _, c := range j.Sources {
				known = append(known, c.Config)
			}
			sort.Strings(wanted)
			sort.Strings(known)
			if !slices.Equal(wanted, known) {
				return zero, migrationError("machine_migration_conflict", "completed sources differ")
			}
			return j.Config, nil
		}
	}
	if len(paths) == 0 || !filepath.IsAbs(destination) {
		return zero, migrationError("machine_migration_incomplete", "absolute destination and all source configs required")
	}
	sources := []ServiceConfig{}
	seen := map[string]bool{}
	for _, path := range paths {
		c, err := ReadServiceConfig(path)
		if err != nil {
			return zero, err
		}
		if c.Role != "worker" || c.Config != path || c.HostID == "" || seen[path] || c.Device == "" || singleDeviceMask(c.Device) != nil || c.Detection.OS == "linux" && !ValidWorkerUnit(filepath.Base(c.Service)) {
			return zero, migrationError("machine_migration_conflict", "source identity or device")
		}
		seen[path] = true
		sources = append(sources, c)
	}
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].Device == "0" {
			return true
		}
		if sources[j].Device == "0" {
			return false
		}
		return sources[i].Device < sources[j].Device
	})
	base := sources[0]
	if seen[destination] {
		return zero, migrationError("machine_migration_conflict", "destination must use a fresh config and spool")
	}
	if len(sources) > 1 && base.Device != "0" {
		return zero, migrationError("machine_migration_conflict", "gpu0 cache source is required")
	}
	ignore := map[string]bool{"config": true, "binary": true, "credentials": true, "state_dir": true, "db": true, "upload_spool_dir": true, "service": true, "name": true, "device": true, "detection": true, "model_cache": true, "model_cache_bytes": true, "environment_cache": true, "environment_cache_bytes": true, "machine_devices": true}
	var before map[string]any
	data, _ := json.Marshal(base)
	json.Unmarshal(data, &before)
	for _, c := range sources[1:] {
		data, _ := json.Marshal(c)
		var after map[string]any
		json.Unmarshal(data, &after)
		for key, v := range before {
			if !ignore[key] && !explicit[strings.ReplaceAll(key, "_", "-")] && !reflect.DeepEqual(v, after[key]) {
				return zero, migrationError("machine_migration_conflict", key)
			}
		}
		if c.HostID != base.HostID || c.WorkerHome != base.WorkerHome || c.Detection.OS != base.Detection.OS || c.Detection.Arch != base.Detection.Arch {
			return zero, migrationError("machine_migration_conflict", "host/account/platform identity")
		}
	}
	selectors := []string{}
	base.MachineDevices = nil
	for _, c := range sources {
		if slices.Contains(selectors, c.Device) {
			return zero, migrationError("machine_migration_conflict", "duplicate selector")
		}
		selectors = append(selectors, c.Device)
		base.MachineDevices = append(base.MachineDevices, DeviceInstall{c.Device, c.Detection})
	}
	dir := filepath.Dir(destination)
	base.Config = destination
	base.Binary = filepath.Join(dir, "myce")
	base.Credentials = filepath.Join(dir, "credentials.json")
	base.State = filepath.Join(dir, "state")
	base.DB = filepath.Join(base.State, "myce.db")
	base.UploadSpoolDir = filepath.Join(base.State, "uploads")
	base.Service = filepath.Join(filepath.Dir(sources[0].Service), "myce-worker.service")
	base.Name = base.HostID
	base.Device = strings.Join(selectors, ",")
	base.FromWorkerConfigs = slices.Clone(paths)
	return base, nil
}

// The private journal holds exact file bytes and manager state. No queue DB,
// checkpoint or cache is ever restored by worker rollback.
type migrationFile struct {
	Path    string
	Existed bool
	Data    []byte
	Mode    os.FileMode
}
type migrationUnit struct {
	Path           string
	Enabled        string
	Loaded, Active bool
}
type workerMigration struct {
	Version int
	Phase   string
	Config  ServiceConfig
	Sources []ServiceConfig
	Files   []migrationFile
	Units   []migrationUnit
}

func machineJournal(c ServiceConfig) string { return c.Config + ".m4-rollback.json" }
func journalTransaction(j workerMigration, command func(context.Context, string, ...string) ([]byte, error)) *installTransaction {
	tx := &installTransaction{manager: installManager{j.Config.Detection.OS, command}, originals: map[string]installedFile{}}
	for _, p := range j.Files {
		f := installedFile{path: p.Path, existed: p.Existed, data: p.Data, mode: p.Mode}
		tx.originals[p.Path] = f
		tx.published = append(tx.published, f)
	}
	for _, u := range j.Units {
		tx.units = append(tx.units, installUnit{path: u.Path, state: installUnitState{enabled: u.Enabled, loaded: u.Loaded, active: u.Active}})
	}
	return tx
}
func saveMigration(path string, j workerMigration) error {
	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	return writeInstallFile(path, data, 0600, installFiles{}, nil)
}

// CheckMachineInstallGate also protects redeploy of an old split config after
// conversion. Retired configs are retained only in the private journal.
func CheckMachineInstallGate(c ServiceConfig) error {
	if c.Role != "worker" || c.Detection.OS != "linux" {
		return nil
	}
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(c.Service), "myce-worker@*.service"))
	links, linkErr := filepath.Glob(filepath.Join(filepath.Dir(c.Service), "*.target.wants", "myce-worker@*.service"))
	if linkErr != nil {
		return linkErr
	}
	for _, link := range links {
		paths = append(paths, filepath.Join(filepath.Dir(c.Service), filepath.Base(link)))
	}
	if err != nil {
		return err
	}
	if strings.Contains(filepath.Base(c.Service), "@") {
		return migrationError("machine_worker_conflict", "split installs require --from-worker-config conversion before deploying this binary")
	}
	selected := map[string]bool{}
	for _, p := range c.FromWorkerConfigs {
		s, err := ReadServiceConfig(p)
		if err != nil {
			return err
		}
		selected[s.Service] = true
	}
	for _, p := range paths {
		if filepath.Base(p) == "myce-worker@.service" {
			continue
		}
		if !selected[p] {
			code := "machine_worker_conflict"
			if len(selected) > 0 {
				code = "machine_migration_incomplete"
			}
			return migrationError(code, "unselected split worker "+filepath.Base(p)+"; use --from-worker-config for every worker")
		}
	}
	return nil
}

func ConvertWorkerInstall(ctx context.Context, c ServiceConfig, executable string, dry bool, command func(context.Context, string, ...string) ([]byte, error)) error {
	return convertWorkerInstall(ctx, c, executable, dry, command, installFiles{})
}
func convertWorkerInstall(ctx context.Context, c ServiceConfig, executable string, dry bool, command func(context.Context, string, ...string) ([]byte, error), files installFiles) (result error) {
	journal := machineJournal(c)
	var saved workerMigration
	if data, err := os.ReadFile(journal); err == nil {
		if err = json.Unmarshal(data, &saved); err != nil {
			return err
		}
		if saved.Version != 1 || saved.Config.Config != c.Config {
			return errors.New("invalid machine migration journal")
		}
		if saved.Phase == "complete" {
			if !reflect.DeepEqual(saved.Config.MachineDevices, c.MachineDevices) || InstallConfigDiff(saved.Config, c) != "" {
				return migrationError("machine_migration_conflict", "completed plan differs")
			}
			check := c
			check.FromWorkerConfigs = nil
			return CheckMachineInstallGate(check)
		}
		if saved.Phase != "rolled-back" {
			return errors.New("machine conversion interrupted; run install worker --rollback-machine with this config first")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := CheckMachineInstallGate(c); err != nil {
		return err
	}
	if dry {
		return nil
	}
	if command == nil {
		return errors.New("service runner unavailable")
	}
	// The queue must already hold every source host disabled with exact cleanup.
	client := Client{URL: c.Queue, Token: os.Getenv("MYCE_TOKEN")}
	if client.Token == "" {
		return errors.New("set MYCE_TOKEN through the secret mechanism for migration maintenance verification")
	}
	var status HostMaintenanceStatus
	if err := client.Call(ctx, "GET", "/api/hosts/"+c.HostID+"/maintenance", nil, &status); err != nil {
		return err
	}
	if status.Policy.Mode != "disabled" || !(status.MigrationReady || status.Quiescent && status.Healthy) {
		return migrationError("machine_migration_incomplete", "host must be disabled, fresh and fully settled")
	}
	var snapshot Snapshot
	if err := client.Call(ctx, "GET", "/api/status", nil, &snapshot); err != nil {
		return err
	}
	catalogData, err := machineWorkerCatalog(c.ModelCatalog, snapshot)
	if err != nil {
		return err
	}
	sources := []ServiceConfig{}
	extra := []string{c.ModelCatalog}
	for _, path := range c.FromWorkerConfigs {
		old, err := ReadServiceConfig(path)
		if err != nil {
			return err
		}
		for _, w := range status.Workers {
			if w.Machine.ID == old.Name && w.Machine.Replacement == nil && !w.Fresh {
				return migrationError("machine_migration_incomplete", "stale source worker")
			}
		}
		known := false
		for _, m := range snapshot.Machines {
			if m.ID == old.Name && hostID(m) == c.HostID {
				known = true
				if old.Name != c.Name && (m.Replacement == nil || m.Replacement.Machine != c.Name) {
					return migrationError("machine_migration_incomplete", "convert queue-side names before worker installation")
				}
			}
		}
		if !known {
			return migrationError("machine_migration_incomplete", "source is missing from the maintained queue")
		}
		sources = append(sources, old)
		extra = append(extra, old.Config, old.Binary, old.Credentials, old.Service, old.GPUEnvironmentFile())
	}
	// The current installer has no separate host enable registry: systemd's
	// persistent wants links and the saved source unit/config are the enable set.
	tx, err := snapshotInstall(ctx, c, files, command, extra...)
	if err != nil {
		return err
	}
	for _, s := range sources {
		u := installUnit{path: s.Service}
		u.state, err = tx.manager.inspect(ctx, u)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(tx.units, func(known installUnit) bool { return known.path == u.path }) {
			tx.units = append(tx.units, u)
		}
	}

	if c.EnvironmentFile == "" && c.JoiningFile == "" {
		original := tx.originals[sources[0].Credentials]
		for _, source := range sources {
			other := tx.originals[source.Credentials]
			if !original.existed || !other.existed || !bytes.Equal(original.data, other.data) {
				return migrationError("machine_migration_conflict", "source credentials differ or are missing")
			}
		}
	}
	j := workerMigration{Version: 1, Phase: "prepared", Config: c, Sources: sources}
	for _, f := range tx.originals {
		j.Files = append(j.Files, migrationFile{f.path, f.existed, f.data, f.mode})
	}
	for _, u := range tx.units {
		j.Units = append(j.Units, migrationUnit{u.path, u.state.enabled, u.state.loaded, u.state.active})
	}
	sort.Slice(j.Files, func(a, b int) bool { return j.Files[a].Path < j.Files[b].Path })
	if err = os.MkdirAll(filepath.Dir(journal), 0700); err != nil {
		return err
	}
	if err = saveMigration(journal, j); err != nil {
		return err
	}
	defer func() {
		if result != nil {
			restore := journalTransaction(j, command)
			result = errors.Join(result, restore.restore(ctx))
			if len(restore.differences(ctx)) == 0 {
				j.Phase = "rolled-back"
				result = errors.Join(result, saveMigration(journal, j))
			}
		}
	}()
	for _, s := range sources {
		if _, err = command(ctx, "systemctl", "--user", "disable", "--now", filepath.Base(s.Service)); err != nil {
			return err
		}
		state, err := tx.manager.inspect(ctx, installUnit{path: s.Service})
		if err != nil {
			return err
		}
		if state.active || state.enabled == "enabled" || state.enabled == "enabled-runtime" {
			return errors.New("source worker still active or enabled")
		}
		if err = settledMigrationSpool(s.State); err != nil {
			return err
		}
		if err = tx.retire(s.Service); err != nil {
			return err
		}
		if s.Config != c.Config {
			if err = tx.retire(s.Config); err != nil {
				return err
			}
		}
	}
	// A same-name single-card source (Spark) must have completed its normal
	// leave handshake. Never invent a previous-session chain for a fresh spool.
	for _, source := range sources {
		if source.Name != c.Name {
			continue
		}
		var snap Snapshot
		if err = client.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
			return err
		}
		for _, m := range snap.Machines {
			if m.ID == source.Name && !m.Gone {
				return migrationError("machine_migration_incomplete", "source session must complete leave before its fresh machine worker can join")
			}
		}
	}
	// Preserve credentials and runtime overrides without reading a referenced
	// fleet EnvironmentFile or importing a source spool into the fresh spool.
	if c.EnvironmentFile == "" && c.JoiningFile == "" {
		old := tx.originals[sources[0].Credentials]
		if !old.existed {
			return errors.New("source credentials unavailable")
		}
		if err = os.MkdirAll(filepath.Dir(c.Credentials), 0700); err != nil {
			return err
		}
		if err = tx.put(c.Credentials, old.data, old.mode); err != nil {
			return err
		}
	}
	if old := tx.originals[sources[0].GPUEnvironmentFile()]; old.existed {
		if err = tx.put(c.GPUEnvironmentFile(), old.data, old.mode); err != nil {
			return err
		}
	}
	if catalogData != nil {
		if err = tx.put(c.ModelCatalog, catalogData, 0600); err != nil {
			return err
		}
	}
	c.FromWorkerConfigs = nil
	files.machineConversion = true
	// Credential prepublication above must not be mistaken for an unknown fresh
	// artifact. The inner installer uses the outer snapshot and rollback journal.
	if err = installWithFiles(ctx, c, executable, "", command, files); err != nil {
		return err
	}
	j.Phase = "complete"
	return saveMigration(journal, j)
}

func RollbackMachineInstall(ctx context.Context, config string, dry bool, command func(context.Context, string, ...string) ([]byte, error)) error {
	data, err := os.ReadFile(config + ".m4-rollback.json")
	if err != nil {
		return err
	}
	var j workerMigration
	if err = json.Unmarshal(data, &j); err != nil {
		return err
	}
	if j.Version != 1 || j.Config.Config != config {
		return errors.New("invalid machine rollback journal")
	}
	if dry || j.Phase == "rolled-back" {
		return nil
	}
	c := j.Config
	client := Client{URL: c.Queue, Token: os.Getenv("MYCE_TOKEN")}
	var status HostMaintenanceStatus
	if err = client.Call(ctx, "GET", "/api/hosts/"+c.HostID+"/maintenance", nil, &status); err != nil {
		return err
	}
	if status.Policy.Mode != "disabled" || !(status.MigrationReady || status.Quiescent && status.Healthy) {
		return migrationError("machine_migration_incomplete", "rollback requires fresh disabled host and exact cleanup")
	}
	tx := journalTransaction(j, command)
	// Stop the machine generation before any source definition can be restored.
	if err = tx.manager.stop(ctx, installUnit{path: c.Service}); err != nil {
		return err
	}
	if _, err = command(ctx, "systemctl", "--user", "disable", filepath.Base(c.Service)); err != nil {
		return err
	}
	if err = tx.restore(ctx); err != nil {
		return err
	}
	j.Phase = "rolled-back"
	return saveMigration(machineJournal(c), j)
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

func settledMigrationSpool(dir string) error {
	// The exact service has stopped. Keep its spool locked until inspected; do
	// not adopt, merge or modify any attempt or buffered result.
	lock, err := os.OpenFile(filepath.Join(dir, "worker.lock"), os.O_RDWR, 0)
	if err == nil {
		defer lock.Close()
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return migrationError("machine_migration_incomplete", "source spool still in use")
		}
		defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	paths, err := filepath.Glob(filepath.Join(dir, "state-attempt-*.json"))
	if err != nil {
		return err
	}
	paths = append(paths, filepath.Join(dir, "state.json"), filepath.Join(dir, "legacy-state.json"))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var s workerState
		if err = decodeWorkerState(data, &s); err != nil {
			return err
		}
		if s.Job != nil || s.Result != nil || s.LaunchPending {
			return migrationError("machine_migration_incomplete", "source spool has unresolved attempt or result")
		}
	}
	return nil
}

// Queue retirement records are explicit ownership evidence, not parsed names.
// Rewrite a local catalog independently so its local environment/secret values
// remain intact; the existing transaction snapshots exact bytes for rollback.
func machineWorkerCatalog(path string, snapshot Snapshot) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	original, err := ReadModels(path)
	if err != nil {
		return nil, err
	}
	converted := original
	converted.Models = slices.Clone(original.Models)
	mapping := []MachineRename{}
	for _, m := range snapshot.Machines {
		if m.Replacement != nil {
			mapping = append(mapping, MachineRename{m.ID, m.Replacement.Machine, m.Replacement.Device})
		}
	}
	for i := range converted.Models {
		m := &converted.Models[i]
		m.AllowedMachines, m.Device, err = migrationNames(m.AllowedMachines, mapping, m.Device, true)
		if err != nil {
			return nil, err
		}
	}
	if reflect.DeepEqual(original, converted) {
		return nil, nil
	}
	return yaml.Marshal(converted)
}
