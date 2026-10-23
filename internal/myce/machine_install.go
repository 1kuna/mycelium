package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
)

// DeviceInstall preserves each source worker's resolved selector and capacity contract.
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
	return PlanMachineInstallWithProbe(context.Background(), destination, paths, explicit, LocalMachineProbe())
}

// PlanMachineInstallWithProbe resolves automatic sources using the same vendor
// inventory as a fresh worker. Explicit source selectors need no hardware probe.
func PlanMachineInstallWithProbe(ctx context.Context, destination string, paths []string, explicit map[string]bool, probe MachineProbe) (ServiceConfig, error) {
	var zero ServiceConfig
	if data, err := os.ReadFile(destination + ".m4-rollback.json"); err == nil {
		var j workerMigration
		if err = json.Unmarshal(data, &j); err != nil {
			return zero, err
		}
		if j.Version != 1 || j.Config.Config != destination {
			return zero, errors.New("invalid machine migration journal")
		}
		if j.Phase != "rolled-back" {
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
			j.Config.FromWorkerConfigs = slices.Clone(paths)
			return j.Config, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return zero, err
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
		if !explicit["host-memory-budget-gb"] {
			c.HostMemoryBudgetGB, err = machineInstallMemoryAllowance(path, c.HostMemoryBudgetGB)
			if err != nil {
				return zero, err
			}
		}
		if c.Role != "worker" || c.Config != path || c.HostID == "" || seen[path] || singleDeviceMask(c.Device) != nil || c.Detection.OS == "linux" && !ValidWorkerUnit(filepath.Base(c.Service)) {
			return zero, migrationError("machine_migration_conflict", "source identity or device")
		}
		if c.Device == "" && len(paths) != 1 {
			return zero, migrationError("machine_migration_conflict", "automatic selection requires a single source worker")
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
		if c.Device == "" {
			if c.Detection.OS != probe.OS || c.Detection.Arch != probe.Arch {
				return zero, migrationError("machine_migration_conflict", "automatic selection requires the source platform")
			}
			devices, err := probe.Accelerators(ctx)
			var partial *acceleratorProbeError
			if err != nil && (!errors.As(err, &partial) || partial.failed(c.Detection.Platform)) {
				return zero, err
			}
			for _, d := range devices {
				if d.Platform == c.Detection.Platform {
					selectors = append(selectors, d.Identity)
					base.MachineDevices = append(base.MachineDevices, DeviceInstall{d.Identity, c.Detection})
				}
			}
			if len(selectors) == 0 {
				return zero, migrationError("machine_migration_conflict", "automatic selection found no matching devices")
			}
			continue
		}
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

// Project only the production lineage's effective allowance into the new saved
// config. Retired local host control remains inert; no runtime decoder is added.
func machineInstallMemoryAllowance(config string, fallback float64) (float64, error) {
	data, err := os.ReadFile(config)
	if err != nil {
		return 0, err
	}
	var legacy struct {
		HostStateDir string `json:"host_state_dir"`
	}
	if err = json.Unmarshal(data, &legacy); err != nil {
		return 0, err
	}
	// Configs without retired local control already use queue-owned policy and
	// keep their saved bootstrap value. Never sum values across source workers.
	if legacy.HostStateDir == "" {
		return fallback, nil
	}
	refuse := func() (float64, error) {
		return 0, migrationError("machine_memory_allowance_unavailable", "legacy effective host allowance unavailable; supply --host-memory-budget-gb explicitly")
	}
	if !filepath.IsAbs(legacy.HostStateDir) {
		return refuse()
	}
	data, err = os.ReadFile(filepath.Join(legacy.HostStateDir, "host-state.json"))
	if err != nil {
		return refuse()
	}
	var state struct {
		Bytes int64 `json:"memory_allowance_bytes"`
	}
	if json.Unmarshal(data, &state) != nil || state.Bytes <= 0 {
		return refuse()
	}
	gib := float64(state.Bytes) / (1 << 30)
	if _, err = hostMemoryAllowance(gib); err != nil {
		return refuse()
	}
	return gib, nil
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
	Version      int
	Phase        string
	Config       ServiceConfig
	Sources      []ServiceConfig
	Files        []migrationFile
	Units        []migrationUnit
	Publications []migrationPublication
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
	registries, err := legacyMigrationRegistries(sources)
	if err != nil {
		return err
	}
	extra = append(extra, registries...)
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
	files = j.publishing(files)
	tx.files = files
	defer func() {
		if result != nil {
			restore, checkErr := checkedJournalTransaction(j, command)
			if checkErr != nil {
				result = errors.Join(result, checkErr)
				return
			}
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
		if err = settledMigrationSpool(s.State, snapshot); err != nil {
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
	for _, path := range registries {
		if err = tx.retire(path); err != nil {
			return err
		}
	}
	if err = settledMigrationSpool(c.State, snapshot); err != nil {
		return err
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
		if err = tx.put(c.ModelCatalog, catalogData, tx.originals[c.ModelCatalog].mode); err != nil {
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
	if j.Phase == "rolled-back" {
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
	tx, err := checkedJournalTransaction(j, command)
	if err != nil {
		return err
	}
	var snapshot Snapshot
	if err = client.Call(ctx, "GET", "/api/status", nil, &snapshot); err != nil {
		return err
	}
	if err = inspectMigrationSpool(c.State, snapshot); err != nil {
		return err
	}
	fmt.Println("Rollback: stop/disable " + filepath.Base(c.Service))
	for _, p := range tx.published {
		fmt.Println("Restore:", filepath.Base(p.path))
	}
	for _, u := range tx.units {
		fmt.Printf("Unit: %s enabled=%s active=%t\n", u.name(), u.state.enabled, u.state.active)
	}
	if dry {
		return nil
	}
	// Stop the machine generation before any source definition can be restored.
	if err = tx.manager.stop(ctx, installUnit{path: c.Service}); err != nil {
		return err
	}
	if err = settledMigrationSpool(c.State, snapshot); err != nil {
		return err
	}
	current, err := tx.manager.inspect(ctx, installUnit{path: c.Service})
	if err != nil {
		return err
	}
	if current.enabled != "not-found" {
		if _, err = command(ctx, "systemctl", "--user", "disable", filepath.Base(c.Service)); err != nil {
			return err
		}
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

// PlanMachineCacheMigration changes only generated source defaults. A custom
// cache needs an explicit cache flag; the conversion journal proves provenance
// after source configs have been retired. No cache contents are copied/deleted.
func PlanMachineCacheMigration(c ServiceConfig, explicit map[string]bool) (ServiceConfig, string, error) {
	if c.Role != "worker" || strings.Contains(filepath.Base(c.Service), "@") {
		return c, "", errors.New("--machine-cache-root requires a machine worker")
	}
	var sources []ServiceConfig
	data, err := os.ReadFile(machineJournal(c))
	if err == nil {
		var j workerMigration
		if err = json.Unmarshal(data, &j); err != nil {
			return c, "", err
		}
		if j.Version != 1 || j.Config.Config != c.Config {
			return c, "", errors.New("invalid machine migration journal")
		}
		sources = j.Sources
	} else if !errors.Is(err, os.ErrNotExist) {
		return c, "", err
	} else {
		for _, path := range c.FromWorkerConfigs {
			source, e := ReadServiceConfig(path)
			if e != nil {
				return c, "", e
			}
			sources = append(sources, source)
		}
	}
	before := c
	for _, s := range sources {
		if !explicit["model-cache"] && before.ModelCache == filepath.Join(s.State, "model-cache") {
			c.ModelCache = filepath.Join(c.State, "model-cache")
		}
		if !explicit["environment-cache"] && before.EnvironmentCache == filepath.Join(s.State, "environment-cache") {
			c.EnvironmentCache = filepath.Join(c.State, "environment-cache")
		}
	}
	var preview strings.Builder
	for _, cache := range []struct{ name, old, next string }{{"Model", before.ModelCache, c.ModelCache}, {"Environment", before.EnvironmentCache, c.EnvironmentCache}} {
		size, e := environmentDiskUsage(cache.old)
		if e != nil {
			return c, "", e
		}
		root := cache.next
		var free int64
		for {
			free, e = diskFree(root)
			if e == nil {
				break
			}
			if !errors.Is(e, os.ErrNotExist) || filepath.Dir(root) == root {
				return c, "", e
			}
			root = filepath.Dir(root)
		}
		fmt.Fprintf(&preview, "%s cache: %s -> %s; old directory retained (%d allocated bytes); destination filesystem %d available bytes\n", cache.name, cache.old, cache.next, size, free)
	}
	preview.WriteString("Custom paths are preserved unless explicitly overridden. Cache limits are preserved; environments at a new root will be prepared again. No cache files are moved or deleted.\n")
	return c, preview.String(), nil
}

// Reuse the queue's custody/health facts, without changing maintenance policy.
func CheckMachineCacheMigration(ctx context.Context, c ServiceConfig) error {
	token := os.Getenv("MYCE_TOKEN")
	if token == "" {
		var err error
		token, err = ReadServiceToken(c)
		if err != nil {
			return err
		}
	}
	client := Client{URL: c.Queue, Token: token}
	var status HostMaintenanceStatus
	if err := client.Call(ctx, "GET", "/api/hosts/"+c.HostID+"/maintenance", nil, &status); err != nil {
		return err
	}
	if !status.Healthy || !status.Quiescent || status.Policy.Mode != "disabled" && status.Policy.Mode != "drained" {
		return migrationError("cache_migration_not_ready", "machine must be disabled or drained, idle and Healthy")
	}
	return nil
}
