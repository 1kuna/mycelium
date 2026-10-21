package myce

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
)

// MachineRename is an operator-supplied ownership map, never a name heuristic.
type MachineRename struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Device string `json:"device"`
}
type MachineConversion struct {
	VerifiedEmptyDrains []VerifiedEmptyDrainReceipt `json:"verified_empty_drains,omitempty"`
	Changes             []string                    `json:"changes,omitempty"`
	PublicationPending  bool                        `json:"publication_pending,omitempty"`
	Mapping             []MachineRename             `json:"mapping"`
	RolledBack          bool                        `json:"rolled_back"`
	Policies            map[string]ProjectPolicy    `json:"policies"`
	Pins                []ModelPin                  `json:"pins"`
	Hosts               map[string]*HostState       `json:"hosts"`
	Machines            map[string]*Machine         `json:"machines"`
	Models              ModelConfig                 `json:"models"`
	ModelsHash          string                      `json:"models_hash"`
	ConvertedModelsHash string                      `json:"converted_models_hash"`
	ConvertedModels     ModelConfig                 `json:"converted_models"`
}

// Refusal identifies the exact logical jobs; keyed intent is never rewritten.
type QueuedRetiredMachinesError struct {
	Jobs     []string
	Machines []string
}

func (e *QueuedRetiredMachinesError) Error() string {
	return fmt.Sprintf("machine_migration_queued_jobs: cancel/resubmit jobs %s referencing retiring machines %s", strings.Join(e.Jobs, ","), strings.Join(e.Machines, ","))
}

func migrationNames(names []string, mapping []MachineRename, device string, cardFence bool) ([]string, string, error) {
	if len(names) == 0 {
		return names, device, nil
	}
	result := []string{}
	selected := map[string]map[string]bool{}
	all := map[string]map[string]bool{}
	for _, r := range mapping {
		if r.From == r.To {
			continue
		}
		if all[r.To] == nil {
			all[r.To] = map[string]bool{}
		}
		all[r.To][r.Device] = true
	}
	for _, name := range names {
		next := name
		for _, r := range mapping {
			if r.From == name && r.From != r.To {
				next = r.To
				if selected[next] == nil {
					selected[next] = map[string]bool{}
				}
				selected[next][r.Device] = true
			}
		}
		if !slices.Contains(result, next) {
			result = append(result, next)
		}
	}
	for host, devices := range selected {
		if slices.Contains(names, host) {
			continue
		}
		if device != "" {
			if !devices[device] {
				return nil, "", migrationError("machine_migration_conflict", "device restriction has an empty intersection")
			}
			continue
		}
		if len(devices) == len(all[host]) {
			continue
		}
		if !cardFence || len(devices) != 1 || len(result) != 1 {
			return nil, "", migrationError("machine_migration_conflict", "partial-card allowed_machines fence cannot be represented without widening")
		}
		for d := range devices {
			device = d
		}
	}
	return result, device, nil
}

func (q *Queue) machineConversionPlan(mapping []MachineRename, models ModelConfig, drains []VerifiedEmptyDrainReceipt) (*MachineConversion, error) {
	if len(mapping) == 0 {
		return nil, migrationError("machine_migration_incomplete", "explicit machine map is required")
	}
	seen := map[string]bool{}
	components := map[string]bool{}
	hosts := map[string]bool{}
	retiring := map[string]bool{}
	for _, r := range mapping {
		if r.From == "" || r.To == "" || !canonicalDeviceID(r.Device) || seen[r.From] {
			return nil, migrationError("machine_migration_conflict", "invalid machine map")
		}
		component := r.To + ":" + r.Device
		if components[component] {
			return nil, migrationError("machine_migration_conflict", "duplicate destination device")
		}
		components[component] = true
		seen[r.From] = true
		m := q.s.Machines[r.From]
		if m == nil || m.Replacement != nil || hostID(m) != r.To {
			return nil, migrationError("machine_migration_conflict", "source machine or physical host identity")
		}
		hosts[r.To] = true
		if r.From != r.To {
			retiring[r.From] = true
		}
	}
	blocked := &QueuedRetiredMachinesError{}
	for _, j := range q.s.Jobs {
		if j.State != "queued" {
			continue
		}
		names := append(slices.Clone(j.Spec.AllowedMachines), j.Machine)
		names = append(names, q.s.Policies[j.Spec.Project].AllowedMachines...)
		for _, pin := range q.s.Pins {
			if pin.Project == j.Spec.Project && pin.Model == j.Spec.Needs.Model {
				names = append(names, pin.Machine, pin.Assigned)
			}
		}
		for _, model := range models.Models {
			if model.Name == j.Spec.Needs.Model {
				names = append(names, model.AllowedMachines...)
			}
		}
		for _, name := range names {
			if retiring[name] {
				if !slices.Contains(blocked.Jobs, j.ID) {
					blocked.Jobs = append(blocked.Jobs, j.ID)
				}
				if !slices.Contains(blocked.Machines, name) {
					blocked.Machines = append(blocked.Machines, name)
				}
			}
		}
	}
	if len(blocked.Jobs) > 0 {
		sort.Strings(blocked.Jobs)
		sort.Strings(blocked.Machines)
		return nil, blocked
	}
	for host := range hosts {
		if q.s.HostMaintenance[host].Mode != "disabled" || len(q.hostObligations(host)) > 0 {
			return nil, migrationError("machine_migration_incomplete", "disable host and reconcile every obligation")
		}
		for _, m := range q.s.Machines {
			if hostID(m) != host || m.Replacement != nil || q.migrationHistoricalMachine(m) {
				continue
			}
			if !seen[m.ID] {
				return nil, migrationError("machine_migration_incomplete", "missing source "+m.ID)
			}
			if (!q.productionSourceSettled(m) && !verifiedEmptySource(drains, m) && (m.Gone || m.LastSeen.After(q.clock.Now()) || q.clock.Now().Sub(m.LastSeen) >= min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2])))) || m.DockerCleanup != "" {
				return nil, migrationError("machine_migration_incomplete", "source is stale, missing or has cleanup")
			}
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") {
					return nil, migrationError("machine_migration_incomplete", "active source attempt")
				}
			}
		}
	}
	copyJSON := func(from, to any) { data, _ := json.Marshal(from); _ = json.Unmarshal(data, to) }
	out := &MachineConversion{Mapping: slices.Clone(mapping), VerifiedEmptyDrains: drains}
	copyJSON(q.s.Policies, &out.Policies)
	copyJSON(q.s.Pins, &out.Pins)
	copyJSON(q.s.Hosts, &out.Hosts)
	copyJSON(q.s.Machines, &out.Machines)
	out.Models = migrationModelFences(models)
	out.ConvertedModels = migrationModelFences(models)
	out.ModelsHash = modelConfigHash(models)
	return out, nil
}

// ConvertMachines performs a small durable SQLite transaction. Callers publish
// external policy files outside q.mu and keep the queue stopped until both agree.
func (q *Queue) ConvertMachines(mapping []MachineRename, models ModelConfig, dry bool) (*MachineConversion, error) {
	return q.convertMachines(mapping, models, dry, false)
}
func (q *Queue) convertMachines(mapping []MachineRename, models ModelConfig, dry, publicationPending bool, evidence ...[]VerifiedEmptyDrainReceipt) (*MachineConversion, error) {
	var drains []VerifiedEmptyDrainReceipt
	if len(evidence) > 0 {
		drains = evidence[0]
	}
	var result *MachineConversion
	operation := func() error {
		if previous := q.s.MachineConversion; previous != nil && !previous.RolledBack {
			if !reflect.DeepEqual(previous.Mapping, mapping) {
				return migrationError("machine_migration_conflict", "different conversion already recorded")
			}
			result = previous
			return nil
		}
		plan, err := q.machineConversionPlan(mapping, models, drains)
		if err != nil {
			return err
		}
		policies := maps.Clone(q.s.Policies)
		for name, p := range policies {
			p.AllowedMachines, _, err = migrationNames(p.AllowedMachines, mapping, "", false)
			if err != nil {
				return fmt.Errorf("project %s: %w", name, err)
			}
			plan.Changes = append(plan.Changes, fmt.Sprintf("project %s allowed_machines: %v -> %v", name, q.s.Policies[name].AllowedMachines, p.AllowedMachines))
			policies[name] = p
		}
		pins := slices.Clone(q.s.Pins)
		for i := range pins {
			p := &pins[i]
			for _, r := range mapping {
				if p.Machine == r.From && r.From != r.To {
					if p.Device != "" && p.Device != r.Device {
						return migrationError("machine_migration_conflict", "pin device")
					}
					p.Machine, p.Device = r.To, r.Device
				}
				if p.Assigned == r.From {
					p.Assigned, p.AssignedDevice = "", ""
				}
			}
		}
		hosts := map[string]*HostState{}
		for host, p := range q.s.Hosts {
			next := host
			for _, r := range mapping {
				if host == r.From {
					next = r.To
				}
			}
			if old := hosts[next]; old != nil && !reflect.DeepEqual(old, p) {
				return migrationError("machine_migration_conflict", "host policies")
			}
			plan.Changes = append(plan.Changes, fmt.Sprintf("host policy: %s -> %s (unchanged allowance)", host, next))
			hosts[next] = p
		}
		for i := range plan.ConvertedModels.Models {
			m := &plan.ConvertedModels.Models[i]
			m.AllowedMachines, m.Device, err = migrationNames(m.AllowedMachines, mapping, m.Device, true)
			if err != nil {
				return fmt.Errorf("server %s: %w", m.Name, err)
			}
		}
		converted := migrationApplyModelFences(models, plan.ConvertedModels)
		plan.ConvertedModelsHash = modelConfigHash(converted)
		for i, p := range pins {
			plan.Changes = append(plan.Changes, fmt.Sprintf("pin %s/%s: %s device %s -> %s device %s", p.Project, p.Model, q.s.Pins[i].Machine, q.s.Pins[i].Device, p.Machine, p.Device))
		}
		for i, m := range plan.ConvertedModels.Models {
			old := models.Models[i]
			plan.Changes = append(plan.Changes, fmt.Sprintf("server %s allowed_machines/device: %v/%s -> %v/%s", m.Name, old.AllowedMachines, old.Device, m.AllowedMachines, m.Device))
		}
		sort.Strings(plan.Changes)
		result = plan
		if dry {
			return nil
		}
		q.s.Policies, q.s.Pins, q.s.Hosts = policies, pins, hosts
		for _, r := range mapping {
			if r.From == r.To {
				if verifiedEmptySource(drains, q.s.Machines[r.From]) {
					q.s.Machines[r.From].Gone = true
				}
				continue
			}
			m := q.s.Machines[r.From]
			m.Gone = true
			m.Replacement = &MachineReplacement{Machine: r.To, Device: r.Device}
		}
		plan.PublicationPending = publicationPending
		q.s.MachineConversion = plan
		return nil
	}
	var err error
	if dry {
		q.mu.Lock()
		err = operation()
		q.mu.Unlock()
	} else {
		err = q.update(operation)
	}
	return result, err
}
func (q *Queue) RollbackMachines(dry bool) error {
	return q.rollbackMachines(dry, false)
}
func (q *Queue) rollbackMachines(dry, publicationPending bool) error {
	operation := func() error {
		plan := q.s.MachineConversion
		if plan == nil {
			return migrationError("machine_migration_incomplete", "no saved conversion")
		}
		if plan.RolledBack {
			return nil
		}
		for _, r := range plan.Mapping {
			if q.s.HostMaintenance[r.To].Mode != "disabled" || len(q.hostObligations(r.To)) > 0 {
				return migrationError("machine_migration_incomplete", "rollback requires disabled hosts and exact cleanup")
			}
			for _, j := range q.s.Jobs {
				if m := q.s.Machines[j.Machine]; m != nil && hostID(m) == r.To && !j.ReleaseConfirmed && (j.State == "running" || j.State == "stopping") {
					return migrationError("machine_migration_incomplete", "rollback has an active attempt")
				}
			}
		}

		// Accepted keyed intent is immutable. Even terminal resumable/requeueable
		// jobs must be cancelled/resubmitted explicitly, never silently renamed.
		blocked := &QueuedRetiredMachinesError{}
		for _, j := range q.s.Jobs {
			if j.State == "succeeded" {
				continue
			}
			for _, r := range plan.Mapping {
				if r.From == r.To {
					continue
				}
				if slices.Contains(j.Spec.AllowedMachines, r.To) {
					if !slices.Contains(blocked.Jobs, j.ID) {
						blocked.Jobs = append(blocked.Jobs, j.ID)
					}
					if !slices.Contains(blocked.Machines, r.To) {
						blocked.Machines = append(blocked.Machines, r.To)
					}
				}
			}
		}
		if len(blocked.Jobs) > 0 {
			slices.Sort(blocked.Jobs)
			slices.Sort(blocked.Machines)
			return blocked
		}

		pins := slices.Clone(q.s.Pins)
		for i := range pins {
			p := &pins[i]
			for _, r := range plan.Mapping {
				if r.From == r.To {
					continue
				}
				if p.Assigned == r.To {
					p.Assigned, p.AssignedDevice = "", ""
				}
				if p.Machine != r.To {
					continue
				}
				if p.Device == r.Device {
					p.Machine, p.Device = r.From, ""
					break
				}
				if p.Device == "" {
					unchanged := slices.ContainsFunc(plan.Pins, func(old ModelPin) bool {
						return old.Project == p.Project && old.Model == p.Model && old.Machine == p.Machine && old.Device == ""
					})
					if !unchanged {
						return migrationError("machine_migration_conflict", "rollback cannot represent host-wide pin; select a legacy device first")
					}
				}
			}
		}
		if dry {
			return nil
		}
		q.s.Policies, q.s.Pins, q.s.Hosts = plan.Policies, pins, plan.Hosts
		for _, r := range plan.Mapping {
			if r.From != r.To {
				if m := q.s.Machines[r.To]; m != nil {
					m.Gone = true
				}
			}
			old := plan.Machines[r.From]
			copy := cloneMachine(old)
			copy.Gone = true
			copy.Replacement = nil
			q.s.Machines[r.From] = &copy
		}
		plan.RolledBack = true
		plan.PublicationPending = publicationPending
		return nil
	}
	if dry {
		q.mu.Lock()
		defer q.mu.Unlock()
		return operation()
	}
	return q.update(operation)
}

func (q *Queue) migrationHistoricalMachine(m *Machine) bool {
	p := q.s.MachineConversion
	if p == nil || !p.RolledBack || !m.Gone {
		return false
	}
	for _, r := range p.Mapping {
		if r.To == m.ID && r.From != r.To {
			return true
		}
	}
	return false
}
func (q *Queue) migrationInventoryBlocked(m *Machine) bool {
	p := q.s.MachineConversion
	if p == nil || !p.RolledBack || m.Devices == nil {
		return false
	}
	for _, r := range p.Mapping {
		if r.To == m.ID {
			return true
		}
	}
	return false
}

// Retired sources cannot be healthy workers. This narrow installation receipt
// is separate from health/quiescence and never permits admission or resume.
func (q *Queue) machineMigrationReady(host string) bool {
	p := q.s.MachineConversion
	if p == nil || q.s.HostMaintenance[host].Mode != "disabled" || len(q.hostObligations(host)) != 0 {
		return false
	}
	found := false
	for _, r := range p.Mapping {
		if r.To != host {
			continue
		}
		found = true
		m := q.s.Machines[r.From]
		saved := p.Machines[r.From]
		if m == nil || saved == nil || m.Session != saved.Session || !m.Gone || m.DockerCleanup != "" {
			return false
		}
	}
	if !found {
		return false
	}
	for _, m := range q.s.Machines {
		if hostID(m) == host && !m.Gone {
			return false
		}
	}
	for _, j := range q.s.Jobs {
		if m := q.s.Machines[j.Machine]; m != nil && hostID(m) == host && (j.State == "running" || j.State == "stopping") && !j.ReleaseConfirmed {
			return false
		}
	}
	return true
}

// A queue-first catalog rewrite must still receive the exact legacy Spark
// session under maintenance. It contributes no model endpoint or admission.
func (q *Queue) migrationLegacyCatalog(m *Machine) bool {
	p := q.s.MachineConversion
	if p == nil || p.RolledBack || m.Devices != nil || q.s.HostMaintenance[hostID(m)].Mode != "disabled" || len(q.hostObligations(hostID(m))) != 0 || m.ModelConfigHash != p.ModelsHash {
		return false
	}
	for _, r := range p.Mapping {
		old := p.Machines[r.From]
		if r.From == r.To && m.ID == r.From && hostID(m) == r.To && old != nil && m.Session == old.Session {
			m.ModelURL = ""
			return true
		}
	}
	return false
}

// Only placement fields enter SQLite. Environment values and secret references
// stay in the catalog and its private installer journal, never queue snapshots.
func migrationModelFences(models ModelConfig) ModelConfig {
	out := ModelConfig{}
	for _, m := range models.Models {
		out.Models = append(out.Models, Model{Name: m.Name, AllowedMachines: slices.Clone(m.AllowedMachines), Device: m.Device})
	}
	return out
}
func migrationApplyModelFences(models, fences ModelConfig) ModelConfig {
	models.Models = slices.Clone(models.Models)
	for i := range models.Models {
		for _, f := range fences.Models {
			if models.Models[i].Name == f.Name {
				models.Models[i].AllowedMachines = slices.Clone(f.AllowedMachines)
				models.Models[i].Device = f.Device
			}
		}
	}
	return models
}
