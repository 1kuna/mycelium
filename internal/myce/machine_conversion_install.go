package myce

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// Open only the existing DB under the queue's exclusive lock. Unlike Open,
// preview performs no startup writes or probes and cannot create an empty DB.
func openConversionQueue(c ServiceConfig, clock Clock) (*Queue, error) {
	lock, err := os.OpenFile(c.DB+".lock", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("stop the queue service before offline conversion")
	}
	db, err := sql.Open("sqlite", "file:"+c.DB+"?mode=rw")
	if err != nil {
		lock.Close()
		return nil, err
	}
	q := &Queue{db: db, lock: lock, clock: clock, root: c.Checkpoints, unreported: map[string]bool{}, calls: map[string]*ModelCall{}, lossTimeouts: [3]time.Duration{ProductionLossAfter, DeadlineLossAfter, LossAfter}}
	var data []byte
	if err = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&data); err == nil {
		err = json.Unmarshal(data, &q.s)
	}
	if err != nil {
		q.Close()
		return nil, err
	}
	if q.s.Schema != persistedSchema {
		q.Close()
		return nil, errors.New("unsupported queue schema")
	}
	return q, nil
}

// ConvertQueueInstall publishes the small policy/config files through the same
// install transaction as worker installs. SQLite owns logical state; the journal
// survives interruption and a retry completes publication before queue startup.
func ConvertQueueInstall(ctx context.Context, c ServiceConfig, mapping []MachineRename, dry, rollback bool, clock Clock, command func(context.Context, string, ...string) ([]byte, error)) (result error) {
	if c.Role != "queue" || clock == nil {
		return errors.New("queue config and Clock required")
	}
	return convertQueueInstall(ctx, c, mapping, dry, rollback, clock, command, installFiles{})
}
func convertQueueInstall(ctx context.Context, c ServiceConfig, mapping []MachineRename, dry, rollback bool, clock Clock, command func(context.Context, string, ...string) ([]byte, error), files installFiles) (result error) {
	q, err := openConversionQueue(c, clock)
	if err != nil {
		return err
	}
	defer q.Close()
	models, err := ReadModels(c.ModelCatalog)
	if err != nil {
		return err
	}
	q.models = models
	var plan *MachineConversion
	if rollback {
		if err = q.RollbackMachines(true); err != nil {
			return err
		}
		plan = q.s.MachineConversion
	} else {
		plan, err = q.ConvertMachines(mapping, models, true)
		if err != nil {
			return err
		}
	}
	artifacts, err := conversionPolicyFiles(c, plan)
	if err != nil {
		return err
	}
	for _, a := range artifacts {
		current, err := a.read(a.path)
		if err != nil {
			return err
		}
		equal := func(expected any) bool {
			x, _ := json.Marshal(current)
			y, _ := json.Marshal(expected)
			return bytes.Equal(x, y)
		}
		if !equal(a.before) && (q.s.MachineConversion == nil || !equal(a.after)) {
			return migrationError("machine_migration_conflict", "policy file differs from saved conversion: "+a.path)
		}
	}
	if plan != nil {
		for _, change := range plan.Changes {
			fmt.Println(change)
		}
	}
	if dry {
		return nil
	}
	journal := c.Config + ".m4-rollback.json"
	var saved workerMigration
	if data, err := os.ReadFile(journal); err == nil {
		if err = json.Unmarshal(data, &saved); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if rollback {
		if saved.Version != 1 || saved.Config.Config != c.Config {
			return errors.New("queue rollback journal unavailable")
		}
		// Keep upgraded binary, database and current checkpoints. Restore only the
		// policy artifacts owned by this transaction, never a snapshot of jobs.
		tx := journalTransaction(saved, command)
		tx.units = nil
		if err = q.rollbackMachines(false, true); err != nil {
			return err
		}
		if err = tx.restoreFiles(); err != nil {
			return err
		}
		if err = q.update(func() error { q.s.MachineConversion.PublicationPending = false; return nil }); err != nil {
			return err
		}
		saved.Phase = "rolled-back"
		return saveMigration(journal, saved)
	}
	if saved.Version == 0 || saved.Phase == "rolled-back" {
		tx, err := snapshotInstall(ctx, c, installFiles{}, command, c.ProjectConfig, c.HostConfig, c.ModelCatalog)
		if err != nil {
			return err
		}
		saved = workerMigration{Version: 1, Phase: "prepared", Config: c}
		for _, path := range []string{c.ProjectConfig, c.HostConfig, c.ModelCatalog} {
			if path != "" {
				f := tx.originals[path]
				saved.Files = append(saved.Files, migrationFile{f.path, f.existed, f.data, f.mode})
			}
		}
		if err = saveMigration(journal, saved); err != nil {
			return err
		}
	}
	// A saved mapping is the idempotence fence. The state commit also retains the
	// full original pins/policies and historical source records for rollback.
	plan, err = q.convertMachines(mapping, models, false, true)
	if err != nil {
		return err
	}
	if err = q.update(func() error { q.s.MachineConversion.PublicationPending = true; return nil }); err != nil {
		return err
	}
	tx := journalTransaction(saved, command)
	tx.published = nil
	tx.files = files
	for _, a := range artifacts {
		data, err := yaml.Marshal(a.after)
		if err != nil {
			return err
		}
		if err = tx.put(a.path, data, 0600); err != nil {
			return fmt.Errorf("policy publication interrupted; keep queue stopped and rerun conversion: %w", err)
		}
	}

	if err = q.update(func() error { q.s.MachineConversion.PublicationPending = false; return nil }); err != nil {
		return err
	}
	saved.Phase = "complete"
	return saveMigration(journal, saved)
}

// MachineConversionDiff is intentionally structural: model configuration may
// contain private environment values, which do not belong in installer output.
func MachineConversionDiff(mapping []MachineRename) string {
	lines := []string{}
	for _, r := range mapping {
		lines = append(lines, fmt.Sprintf("%s -> %s device %s (policies, pins, servers, host policy; preserve jobs/keys/counters/checkpoints)", r.From, r.To, r.Device))
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}

// Compare typed policy values, so comments and YAML formatting never authorize
// overwriting an unknown placement fence. Runtime model fields are not serialized.
type conversionPolicyFile struct {
	path          string
	before, after any
	read          func(string) (any, error)
}

func conversionPolicyFiles(c ServiceConfig, p *MachineConversion) ([]conversionPolicyFile, error) {
	projects := map[string]ProjectPolicy{}
	for name, v := range p.Policies {
		var err error
		v.AllowedMachines, _, err = migrationNames(v.AllowedMachines, p.Mapping, "", false)
		if err != nil {
			return nil, err
		}
		projects[name] = v
	}
	beforeHosts, hosts := map[string]HostPolicy{}, map[string]HostPolicy{}
	for name, v := range p.Hosts {
		beforeHosts[name] = v.Policy
		for _, r := range p.Mapping {
			if r.From == name {
				name = r.To
				break
			}
		}
		hosts[name] = v.Policy
	}
	type hostFile struct {
		Hosts map[string]HostPolicy `yaml:"hosts"`
	}
	all := []conversionPolicyFile{
		{c.ProjectConfig, p.Policies, projects, func(path string) (any, error) { return ReadProjectPolicies(path) }},
		{c.HostConfig, hostFile{beforeHosts}, hostFile{hosts}, func(path string) (any, error) { v, e := ReadHostPolicies(path); return hostFile{v}, e }},
		{c.ModelCatalog, p.Models, p.ConvertedModels, func(path string) (any, error) { return ReadModels(path) }},
	}
	return slices.DeleteFunc(all, func(a conversionPolicyFile) bool { return a.path == "" }), nil
}
