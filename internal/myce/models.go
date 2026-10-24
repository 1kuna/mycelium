package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

type ModelFile struct {
	Path      string `yaml:"path" json:"path"`
	SizeBytes int64  `yaml:"size_bytes" json:"size_bytes"`
	SHA256    string `yaml:"sha256" json:"sha256"`
}
type VLLMPreset struct {
	ServedName           string  `yaml:"served_name" json:"served_name"`
	MaxContext           int     `yaml:"max_context" json:"max_context"`
	KVCacheDType         string  `yaml:"kv_cache_dtype" json:"kv_cache_dtype"`
	GPUMemoryUtilization float64 `yaml:"gpu_memory_utilization" json:"gpu_memory_utilization"`
}
type Model struct {
	Project         string            `yaml:"project,omitempty" json:"project,omitempty"`
	AllowedMachines []string          `yaml:"allowed_machines,omitempty" json:"allowed_machines,omitempty"`
	Env             map[string]string `yaml:"env,omitempty" json:"env,omitempty"`
	SecretEnv       map[string]string `yaml:"secret_env,omitempty" json:"secret_env,omitempty"`
	LocalSource     string            `yaml:"local_source,omitempty" json:"local_source,omitempty"`
	secretRoot      string
	fleetToken      string

	FileStore       bool              `yaml:"file_store,omitempty" json:"file_store,omitempty"`
	RequestTimeout  time.Duration     `yaml:"request_timeout,omitempty" json:"request_timeout,omitempty"`
	RetrySafe       bool              `yaml:"retry_safe,omitempty" json:"retry_safe,omitempty"`
	Logs            string            `yaml:"logs,omitempty" json:"logs,omitempty"`
	HostMemoryGB    float64           `yaml:"host_memory_gb,omitempty" json:"host_memory_gb,omitempty"`
	MaxInFlight     int               `yaml:"max_in_flight,omitempty" json:"max_in_flight,omitempty"`
	Files           []ModelFile       `yaml:"files,omitempty" json:"files,omitempty"`
	Environment     string            `yaml:"environment,omitempty" json:"environment,omitempty"`
	StartCommand    []string          `yaml:"start_command,omitempty" json:"start_command,omitempty"`
	HealthURL       string            `yaml:"health_url,omitempty" json:"health_url,omitempty"`
	ReadyTimeout    time.Duration     `yaml:"ready_timeout,omitempty" json:"ready_timeout,omitempty"`
	StopCommand     []string          `yaml:"stop_command,omitempty" json:"stop_command,omitempty"`
	StopTimeout     time.Duration     `yaml:"stop_timeout,omitempty" json:"stop_timeout,omitempty"`
	Device          string            `yaml:"device,omitempty" json:"device,omitempty"`
	DeviceEnv       map[string]string `yaml:"device_env,omitempty" json:"device_env,omitempty"`
	VLLM            *VLLMPreset       `yaml:"vllm,omitempty" json:"vllm,omitempty"`
	EnvironmentPath string            `yaml:"-" json:"-"`

	SizeBytes int64    `yaml:"size_bytes,omitempty" json:"size_bytes,omitempty"`
	SHA256    string   `yaml:"sha256,omitempty" json:"sha256,omitempty"`
	DirectNAS bool     `yaml:"direct_nas,omitempty" json:"direct_nas,omitempty"`
	Name      string   `yaml:"name" json:"name"`
	Engine    string   `yaml:"engine" json:"engine"`
	Path      string   `yaml:"path" json:"path"`
	MemoryGB  float64  `yaml:"memory_gb" json:"memory_gb"`
	Platforms []string `yaml:"platforms" json:"platforms"`
	Args      []string `yaml:"args,omitempty" json:"args,omitempty"`
}
type ModelConfig struct {
	Models []Model       `yaml:"models"`
	Idle   time.Duration `yaml:"idle_timeout"`
	Wait   time.Duration `yaml:"max_wait"`
}

func ReadModels(path string) (ModelConfig, error) {
	c := ModelConfig{Idle: 10 * time.Minute, Wait: 5 * time.Minute}
	if path == "" {
		return c, nil
	}
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if e = d.Decode(&c); e != nil {
		return c, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return c, errors.New("expected one model config")
	}
	return c, validateModels(c)
}
func validateModels(c ModelConfig) error {
	if c.Idle <= 0 || c.Wait <= 0 {
		return errors.New("model timeouts must be positive")
	}
	seen := map[string]bool{}
	for _, m := range c.Models {
		if err := singleDeviceMask(m.Device); err != nil {
			return err
		}
		for k, v := range m.DeviceEnv {
			if dockerDeviceEnvironment(k) && k != "ZE_ENABLE_PCI_ID_DEVICE_ORDER" {
				if err := singleDeviceMask(v); err != nil {
					return err
				}
			}
		}
		if !validMemoryValue(m.HostMemoryGB) {
			return errors.New("host_memory_gb must be finite and nonnegative")
		}
		if m.RequestTimeout < 0 {
			return errors.New("request_timeout must be positive (zero uses 10m)")
		}
		if m.Logs != "" && m.Logs != "keep" && m.Logs != "none" {
			return errors.New("logs must be keep or none")
		}
		if m.MaxInFlight < 0 {
			return errors.New("max_in_flight must be nonnegative (zero means unlimited)")
		}
		if m.Name == "" || seen[m.Name] || (m.Path == "" && !m.FileStore && m.LocalSource == "") || m.MemoryGB <= 0 || math.IsNaN(m.MemoryGB) || math.IsInf(m.MemoryGB, 0) || len(m.Platforms) == 0 {
			return errors.New("models require unique names, path, positive memory and platforms")
		}
		if m.Engine != "fake" && (m.SizeBytes <= 0 || len(m.SHA256) != 64) {
			return fmt.Errorf("model %s requires size_bytes and sha256", m.Name)
		}
		if m.LocalSource != "" && (!localName(m.LocalSource) || m.FileStore || m.DirectNAS || m.Path != "" || len(m.Files) == 0) {
			return errors.New("local_source requires a named approval and files; omit path, file_store and direct_nas")
		}
		if err := validateProjectIdentifier(m.Project); err != nil {
			return err
		}
		if strings.TrimSpace(m.Project) != m.Project {
			return errors.New("model project must not contain surrounding whitespace")
		}
		for _, machine := range m.AllowedMachines {
			if strings.TrimSpace(machine) == "" {
				return errors.New("model allowed_machines requires nonempty names")
			}
		}
		if err := validateModelEnv(m); err != nil {
			return err
		}
		if m.FileStore && (m.DirectNAS || m.SizeBytes <= 0) {
			return errors.New("file_store models require verified cached bytes, not direct_nas")
		}
		if m.SHA256 != "" {
			if _, err := hex.DecodeString(m.SHA256); err != nil || len(m.SHA256) != 64 {
				return errors.New("invalid model sha256")
			}
		}
		seen[m.Name] = true
		if m.Engine != "llama.cpp" && m.Engine != "fake" && m.Engine != "server" && m.Engine != "vllm" {
			return fmt.Errorf("unsupported engine %q", m.Engine)
		}
		if err := validateServer(m); err != nil {
			return err
		}
		if len(m.Files) > 0 {
			size, hash, err := ModelFilesIdentity(m.Files)
			if err != nil {
				return err
			}
			if size != m.SizeBytes || hash != m.SHA256 {
				return fmt.Errorf("model %s bundle size/hash does not match manifest", m.Name)
			}
		}
		if m.Engine == "llama.cpp" || m.Engine == "fake" {
			for _, arg := range m.Args {
				if strings.HasPrefix(arg, "--hf-") || strings.HasPrefix(arg, "--model-url") || strings.HasPrefix(arg, "--mmproj-url") {
					return errors.New("remote model args are forbidden; use myce model fetch")
				}
				for _, reserved := range []string{"--host", "--port", "--model", "--alias", "-m", "--model-url", "-mu", "--hf-repo", "-hf", "--hf-file", "-hff", "--hf-token", "-hft", "--mmproj-url"} {
					if arg == reserved || strings.HasPrefix(arg, reserved+"=") {
						return fmt.Errorf("model args must not override %s", reserved)
					}
				}
			}
		}
		for _, p := range m.Platforms {
			if !validGPU(p) {
				return fmt.Errorf("invalid model platform %q", p)
			}
		}
	}
	return nil
}

type ModelSlot struct {
	Device               string              `json:"device,omitempty"`
	PreemptionCount      int                 `json:"preemption_count,omitempty"`
	RunSince             *time.Time          `json:"run_since,omitempty"`
	Class                string              `json:"class,omitempty"`
	PriorityStop         *PriorityPreemption `json:"priority_stop,omitempty"`
	Project              string              `json:"project,omitempty"`
	RuntimeFailed        bool                `json:"runtime_failed,omitempty"`
	LoadUntil            *time.Time          `json:"load_until,omitempty"`
	LoadTimedOut         bool                `json:"load_timed_out,omitempty"`
	RetryReserved        bool                `json:"retry_reserved,omitempty"`
	FailureCounted       bool                `json:"failure_counted,omitempty"`
	Attempt              string              `json:"attempt,omitempty"`
	CrashCount           int                 `json:"crash_count,omitempty"`
	HealthySince         *time.Time          `json:"healthy_since,omitempty"`
	HealthRevision       uint64              `json:"health_revision,omitempty"`
	ApprovalUnknownSince *time.Time          `json:"approval_unknown_since,omitempty"`
	Placement            *ModelPlacement     `json:"placement,omitempty"`

	RetryAt           *time.Time         `json:"retry_at,omitempty"`
	RetryDelay        time.Duration      `json:"retry_delay,omitempty"`
	ReleasingCalls    bool               `json:"releasing_calls,omitempty"`
	Recovering        bool               `json:"recovering,omitempty"`
	ProductionUsed    bool               `json:"production_used,omitempty"`
	StopAt            *time.Time         `json:"stop_at,omitempty"`
	EnvironmentRecipe *EnvironmentRecipe `json:"environment_recipe,omitempty"`
	Model             string             `json:"model"`
	Machine           string             `json:"machine"`
	Session           string             `json:"session"`
	State             string             `json:"state"`
	Priority          string             `json:"priority"`
	LastUsed          time.Time          `json:"last_used"`
	InFlight          int                `json:"calls_in_flight"`
	Error             string             `json:"error,omitempty"`
	IdleSeconds       float64            `json:"idle_seconds"`
}
type ModelReport struct {
	Device         string `json:"device,omitempty"`
	Attempt        string `json:"attempt,omitempty"`
	RuntimeFailed  bool   `json:"runtime_failed,omitempty"`
	HealthRevision uint64 `json:"health_revision,omitempty"`
	HealthDegraded bool   `json:"health_degraded,omitempty"`
	InFlight       int    `json:"calls_in_flight,omitempty"`
	CallsMeasured  bool   `json:"calls_measured,omitempty"`
	Name           string `json:"name"`
	State          string `json:"state"`
	Error          string `json:"error,omitempty"`
}
type ModelCall struct {
	Device                           string
	PreemptionCount                  int
	Class                            string
	PriorityStop                     *PriorityPreemption
	Attempt, ExcludeAttempt          string
	StopAt                           *time.Time
	ProductionHost                   string
	WaitingSince                     time.Time
	StartedAt                        *time.Time
	ID, Model, Project, Priority     string
	Deadline                         *time.Time
	Queued                           time.Time
	Machine, Session, Exclude, Error string
}

func slotKey(machine, model string, device ...string) string {
	if len(device) > 0 && device[0] != "" {
		return machine + "/" + device[0] + "/" + model
	}
	return machine + "/" + model
}
func targetDevice(m *Machine) string {
	if m.Devices != nil {
		return m.Device
	}
	return ""
}
func targetSlotKey(m *Machine, model string) string { return slotKey(m.ID, model, targetDevice(m)) }
func slotOnTarget(s *ModelSlot, m *Machine) bool {
	return s.Machine == m.ID && (m.Devices == nil || s.Device == m.Device && m.GPU != "cpu")
}
func slotMachine(m *Machine, s *ModelSlot) *Machine {
	if s.Device == "" {
		return m
	}
	return allocationMachine(m, []string{s.Device})
}

func (q *Queue) ConfigureModels(c ModelConfig) error {
	if e := validateModels(c); e != nil {
		return e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.calls != nil {
		for _, p := range q.s.Pins {
			found := false
			for _, m := range c.Models {
				if m.Name == p.Model {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("catalog removes pinned model %s; unpin first", p.Model)
			}
		}
	}
	for _, m := range c.Models {
		if err := q.validateMachineNames(m.AllowedMachines); err != nil {
			return err
		}
		if m.Environment != "" {
			recipes, ok := q.environments.Environments[m.Environment]
			if !ok {
				return fmt.Errorf("model %s: unknown environment %s", m.Name, m.Environment)
			}
			for _, p := range m.Platforms {
				if _, ok := recipes[p]; !ok {
					return fmt.Errorf("model %s: environment lacks %s recipe", m.Name, p)
				}
			}
		}
	}
	q.models = c
	q.calls = map[string]*ModelCall{}
	if q.s.ModelSlots == nil {
		q.s.ModelSlots = map[string]*ModelSlot{}
	}
	// HTTP requests do not survive queue restart. Reconnected workers reconcile slots.
	for _, s := range q.s.ModelSlots {
		m := q.s.Machines[s.Machine]
		if m != nil {
			if h := q.s.Hosts[hostID(m)]; h != nil && h.Policy.ProductionHold {
				s.Recovering = true
			}
		}
		s.InFlight = 0
	}
	return nil
}
func (q *Queue) model(name string) (Model, bool) {
	for _, m := range q.models.Models {
		if m.Name == name {
			return m, true
		}
	}
	return Model{}, false
}
func (q *Queue) modelMemory(machine string) float64 {
	n := 0.0
	for _, s := range q.s.ModelSlots {
		if s.Machine == machine && slotReservesMemory(s) {
			m, _ := q.model(s.Model)
			n += m.MemoryGB
		}
	}
	return n
}

// Native free memory already reflects resident and unloading engines. Until a
// load is reported resident, reserve its declared budget conservatively. For
// configured budgets none of the model usage is included in the report.
func (q *Queue) modelReservation(m *Machine, reports []ModelReport) float64 {
	if !m.MemoryMeasured {
		return q.modelMemory(m.ID)
	}
	resident := map[string]bool{}
	for _, r := range reports {
		if slot := q.s.ModelSlots[slotKey(m.ID, r.Name, r.Device)]; slot != nil && (r.Attempt == "" || slot.Attempt != r.Attempt) {
			continue
		}
		if contains([]string{"ready", "draining", "unloading", "unload_failed"}, r.State) {
			resident[r.Name] = true
		}
	}
	reservation := 0.0
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && slotReservesMemory(s) && !resident[s.Model] {
			model, _ := q.model(s.Model)
			reservation += model.MemoryGB
		}
	}
	return reservation
}
func (q *Queue) reconcileModels(m *Machine, reports []ModelReport) {
	if m.ModelURL == "" {
		return
	}
	for key, s := range q.s.ModelSlots {
		if s.Machine != m.ID {
			continue
		}
		if s.Session != m.Session {
			delete(q.s.ModelSlots, key)
			continue
		}
		for _, r := range reports {
			if r.Name == s.Model && r.Device == s.Device {
				if r.Attempt == "" || r.Attempt != s.Attempt {
					continue
				}
				if r.CallsMeasured && r.InFlight == 0 {
					s.ReleasingCalls = false
				}
				if s.Recovering {
					if !r.CallsMeasured {
						continue
					}
					s.InFlight = max(0, r.InFlight)
					if s.InFlight == 0 {
						s.Recovering = false
					} else if !q.slotPinned(s) {
						s.State = "draining"
					}
				}

				q.applyEngineReport(key, s, r)
			}
		}
	}
	hints := []string{}
	for _, name := range m.Models {
		if _, managed := q.model(name); !managed {
			hints = append(hints, name)
		}
	}
	m.Models = hints
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && s.State == "ready" {
			m.Models = append(m.Models, s.Model)
		}
	}
}
func (q *Queue) maintainModels() {
	if q.s.ModelSlots == nil {
		q.s.ModelSlots = map[string]*ModelSlot{}
	}
	now := q.clock.Now()
	q.maintainPins()
	for k, s := range q.s.ModelSlots {
		m := slotMachine(q.s.Machines[s.Machine], s)
		if m == nil || m.Gone || m.Session != s.Session {
			if m != nil && (q.maintenanceKnown(hostID(m)) || m.Session == s.Session && (m.HostMemoryBudgetGB > 0 || m.hostBudgetGB > 0 || q.s.Hosts[hostID(m)] != nil && q.s.Hosts[hostID(m)].Policy.ProductionHold)) {
				continue
			}
			delete(q.s.ModelSlots, k)
			continue
		}
		model, exists := q.model(s.Model)
		q.expireEngineLoad(s)
		if s.State == "failed" {
			q.maintainEngineRetry(k, s, m, model)
			continue
		}
		if s.State != "unloading" && s.State != "unload_failed" && ((!exists || !q.modelUsableMachine(model, m)) || availabilityBlocked(m) || m.Leaving || !q.slotPinned(s) && s.State == "ready" && s.InFlight == 0 && now.Sub(s.LastUsed) >= q.models.Idle) {
			s.State = "draining"
		}
	}
	if q.networkQuiet {
		return
	}
	// Recompute the device winners after each admission or preparation. A call
	// can only use machines on which the shared planner gives it the next turn.
	// Trying a loading/blocked call once still permits backfill on other resources.
	attempted := map[string]bool{}
	for _, c := range q.calls {
		if _, ok := q.model(c.Model); !ok && c.Machine == "" && c.Error == "" {
			c.Error = "unknown model"
		}
	}
	for {
		var c *ModelCall
		for _, m := range q.plannerMachines() {
			winner := q.candidate(m, attempted)
			if winner == nil || q.calls[winner.ID] == nil {
				continue
			}
			if c == nil || q.before(winner, callJob(c), m) {
				c = q.calls[winner.ID]
			}
		}
		if c == nil {
			break
		}
		machines := []*Machine{}
		for _, m := range q.plannerMachines() {
			if winner := q.candidate(m, attempted); winner != nil && winner.ID == c.ID {
				machines = append(machines, m)
			}
		}
		attempted[c.ID] = true
		model, ok := q.model(c.Model)
		if !ok {
			c.Error = "unknown model"
			continue
		}
		// Compare every compatible placement before mutating any victim. A free
		// load beats interrupting a ready engine's call or another worker's build.
		sort.SliceStable(machines, func(i, j int) bool {
			rank := func(m *Machine) int {
				if s := q.s.ModelSlots[targetSlotKey(m, c.Model)]; s != nil {
					if s.State == "ready" {
						return 0
					}
					if s.State == "loading" {
						return 1
					}
				}
				return 2
			}
			if a, b := rank(machines[i]), rank(machines[j]); a != b {
				return a < b
			}
			a, b := q.loadSeconds(c.Model, machines[i]), q.loadSeconds(c.Model, machines[j])
			if a != b {
				return a < b
			}
			return machines[i].ID < machines[j].ID
		})
		var best *placementPlan
		for _, m := range machines {
			var p *placementPlan
			s := q.s.ModelSlots[targetSlotKey(m, c.Model)]
			switch {
			case s != nil && s.State == "ready":
				p = q.callSlotPlan(m, c, s, model.MaxInFlight)
			case s != nil && s.State == "loading":
				p = &placementPlan{machine: m}
			case s == nil || s.State == "failed" && s.RetryAt != nil && !now.Before(*s.RetryAt):
				p = q.roomPlan(callJob(c), m, false)
			}
			if p != nil && q.completionPlanBetter(callJob(c), p, best) {
				best = p
			}
		}
		if best == nil {
			continue
		}
		q.applyPlacement(best, callJob(c))
		m := best.machine
		s := q.s.ModelSlots[targetSlotKey(m, c.Model)]
		if s != nil && s.State == "ready" {
			if model.MaxInFlight > 0 && s.InFlight >= model.MaxInFlight {
				continue
			}
			c.Machine, c.Session, c.Attempt, c.Device = m.ID, m.Session, s.Attempt, s.Device
			c.StartedAt = &now
			if c.Priority == "production" {
				s.ProductionUsed = true
			}
			if s.InFlight == 0 || q.comparePriority(callJob(c).Spec, Spec{Project: s.Project, Priority: s.Priority, Class: s.Class}) < 0 {
				s.Priority, s.Class, s.Project = c.Priority, c.Class, c.Project
			}
			s.InFlight++
			s.LastUsed = now
			continue
		}
		if s != nil && s.State == "loading" {
			if q.comparePriority(callJob(c).Spec, Spec{Project: s.Project, Priority: s.Priority, Class: s.Class}) < 0 {
				s.Priority, s.Class, s.Project = c.Priority, c.Class, c.Project
				if c.Priority == "production" {
					s.ProductionUsed = true
				}
			}
			continue
		}
		if m.FreeMemoryGB < model.MemoryGB || !cpuAllocationFits(m, 1) || !hostFits(m, model.MemoryGB, model.HostMemoryGB) || q.preparationActive(m) || q.preparationStopping(m) {
			continue
		}
		if s != nil {
			q.beginEngineAttempt(s, c.Priority)
			s.Class, s.Project = c.Class, c.Project
		} else {
			s = &ModelSlot{Project: c.Project, Model: c.Model, Device: targetDevice(m), Machine: m.ID, Session: m.Session, State: "loading", LoadUntil: q.engineLoadDeadline(), Attempt: ID(), Priority: c.Priority, Class: c.Class, ProductionUsed: c.Priority == "production", LastUsed: now}
			q.s.ModelSlots[targetSlotKey(m, c.Model)] = s
		}
		q.initEnginePreemption(s)
		m.FreeMemoryGB -= model.MemoryGB
		q.refreshHostMemory()
	}

	// Each worker release covers one batch. Remove both that worker and its
	// winner before selecting the next release through the shared planner.
	planned := map[string]bool{}
	handled := map[string]bool{}
	for {
		var j *Job
		for _, m := range q.plannerMachines() {
			if planned[targetKey(m)] {
				continue
			}
			winner := q.candidate(m, handled)
			if winner != nil && q.calls[winner.ID] == nil && (j == nil || q.before(winner, j, m)) {
				j = winner
			}
		}
		if j == nil {
			break
		}
		if machine := q.makeBatchRoom(j, planned, handled); machine != "" {
			planned[machine] = true
		}
		handled[j.ID] = true
	}
}
func callJob(c *ModelCall) *Job {
	j := &Job{ProductionHost: c.ProductionHost, ID: c.ID, Queued: c.Queued, WaitingSince: c.WaitingSince, StartedAt: c.StartedAt, Spec: Spec{Project: c.Project, Priority: c.Priority, Class: c.Class, Deadline: c.Deadline}}
	j.Spec.Needs.Model = c.Model
	return j
}

// A pending request may precede a batch only on a worker where it can actually
// run or make room. Counting every resident model incorrectly blocks batches
// behind requests that cannot reclaim higher-priority work.
func (q *Queue) callCanUse(m *Machine, c *ModelCall) bool {
	model, ok := q.model(c.Model)
	if !ok || !q.callPlacementAllowed(c, m) || c.Machine != "" || c.Error != "" || q.capReasonFor(callJob(c), m) != "" || m.MemoryError != "" || m.Gone || m.Leaving || q.callExcludes(c, m.ID, targetDevice(m)) || m.ModelURL == "" || !q.modelUsableMachine(model, m) || q.unreported[m.ID] {
		return false
	}
	if s := q.s.ModelSlots[targetSlotKey(m, c.Model)]; s != nil && s.State == "ready" {
		return q.callSlotPlan(m, c, s, model.MaxInFlight) != nil
	}
	if s := q.s.ModelSlots[targetSlotKey(m, c.Model)]; s != nil && s.State == "failed" && (s.RetryAt == nil || q.clock.Now().Before(*s.RetryAt)) {
		return false
	}
	if s := q.s.ModelSlots[targetSlotKey(m, c.Model)]; s != nil && s.State == "loading" {
		return true
	}
	return q.roomPlan(callJob(c), m, false) != nil
}

// Idle cache residency must not outrank runnable same-tier demand. Reuse this
// fence in every GPU/RAM release estimate and action; active work and loading
// attempts retain the existing strictly-lower-tier preemption rule.
func (q *Queue) slotReclaimableFor(s *ModelSlot, demand Spec) bool {
	if s.State == "unload_failed" || s.State == "failed" {
		return false
	}
	n := q.comparePriority(q.slotSpec(s), demand)
	if n > 0 && s.PriorityStop == nil && q.preemptionProtected(s.PreemptionCount, s.RunSince) {
		return false
	}
	return n > 0 || !q.slotPinned(s) && n == 0 && s.InFlight == 0 && (s.State == "ready" || s.State == "draining")
}

func (q *Queue) modelPlan(machine string) []ModelSlot {
	out := []ModelSlot{}
	for _, s := range q.s.ModelSlots {
		if s.Machine == machine {
			slot := *s
			model, _ := q.model(s.Model)
			if s.State == "loading" && !logPolicyFits(model.Logs, q.s.Machines[machine]) {
				continue
			}
			slot.Placement = q.modelPlacement(model)
			if model.Environment != "" {
				recipe := q.environments.Environments[model.Environment][slotMachine(q.s.Machines[machine], s).GPU]
				slot.EnvironmentRecipe = &recipe
			}
			out = append(out, slot)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}
func (q *Queue) addCall(c *ModelCall) error {
	if err := validateProjectIdentifier(c.Project); err != nil {
		return err
	}
	return q.update(func() error {
		if _, ok := q.model(c.Model); !ok {
			return errors.New("unknown model")
		}
		if e := q.validateProjectCapacity(callJob(c)); e != nil {
			return e
		}
		if !q.simulation || c.ID == "" {
			c.ID = ID()
		}
		c.Queued = q.clock.Now()
		q.calls[c.ID] = c
		q.maintain()
		return nil
	})
}
func (q *Queue) finishCall(id string) error {
	return q.update(func() error {
		c := q.calls[id]
		if c != nil {
			if s := q.s.ModelSlots[slotKey(c.Machine, c.Model, c.Device)]; s != nil && s.Session == c.Session && s.Attempt == c.Attempt {
				s.InFlight = max(0, s.InFlight-1)
				s.LastUsed = q.clock.Now()
			}
			delete(q.calls, id)
			if c.Machine != "" && c.Error == "" {
				q.recordModelUse(c.Model)
			}
			q.refreshModelPriority(c)
		}
		q.maintain()
		return nil
	})
}

// Only current use protects a model from reclamation. A completed high-priority
// request must not protect remaining experiments (or an entirely idle model).
func (q *Queue) refreshModelPriority(c *ModelCall) {
	s := q.s.ModelSlots[slotKey(c.Machine, c.Model, c.Device)]
	if s == nil || s.Session != c.Session || s.Attempt != c.Attempt {
		return
	}
	s.Priority = "experiment"
	s.Class = "experiment"
	s.Project = ""
	found := false
	for _, live := range q.calls {
		if live.Machine == c.Machine && live.Session == c.Session && live.Attempt == c.Attempt && live.Model == c.Model && live.Error == "" && (!found || q.comparePriority(callJob(live).Spec, Spec{Project: s.Project, Priority: s.Priority, Class: s.Class}) < 0) {
			s.Priority = live.Priority
			s.Class = live.Class
			s.Project = live.Project
			found = true
		}
	}

}
func modelError(w http.ResponseWriter, status int, message string) {
	if message == priorityPreemptionCause {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"type": "interruption", "code": priorityPreemptionCause, "message": errPriorityPreemption.Error()}})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message, "type": "mycelium_error"}})
}
func (a API) modelCall(w http.ResponseWriter, r *http.Request) {
	c := &ModelCall{Project: r.Header.Get("X-Myce-Project"), Priority: r.Header.Get("X-Myce-Priority"), Class: r.Header.Get("X-Myce-Class")}
	var data []byte
	var spool *spooledModelRequest
	var bodySize int64
	var e error
	var body struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	generic := strings.HasPrefix(r.URL.Path, "/models/")
	endpoint := r.URL.RequestURI()
	if generic {
		if err := a.Queue.callDefaults(c, Model{}, r.Header, false); err != nil {
			modelError(w, 400, err.Error())
			return
		}
		parts := strings.SplitN(strings.TrimPrefix(r.URL.EscapedPath(), "/models/"), "/", 2)
		if len(parts) != 2 || parts[0] == "" {
			modelError(w, 400, "expected /models/NAME/PATH")
			return
		}
		body.Model, e = url.PathUnescape(parts[0])
		if e != nil {
			modelError(w, 400, "invalid model request")
			return
		}
		body.Stream = true
		endpoint = "/" + parts[1]
		if r.URL.RawQuery != "" {
			endpoint += "?" + r.URL.RawQuery
		}
		spool, bodySize, e = a.Queue.uploadSpool.request(r.Context(), r.Body, maxGenericRequestBytes, r.ContentLength)
		if e != nil {
			status := 400
			if errors.Is(e, errModelRequestTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			if errors.Is(e, errUploadSpoolFull) {
				status = http.StatusInsufficientStorage
			}
			message := "model upload failed"
			if errors.Is(e, errModelRequestTooLarge) {
				message = "model request too large"
			}
			if errors.Is(e, errUploadSpoolFull) {
				message = errUploadSpoolFull.Error()
			}
			http.Error(w, message, status)
			return
		}
		defer func() {
			if err := spool.Close(); err != nil {
				log.Print("model upload spool cleanup failed")
			}
		}()
	} else {
		data, e = io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
		if e != nil {
			modelError(w, 400, "invalid model request")
			return
		}
		if e = json.Unmarshal(data, &body); e != nil {
			modelError(w, 400, "invalid OpenAI JSON request")
			return
		}
	}
	a.Queue.mu.Lock()
	model, _ := a.Queue.model(body.Model)
	a.Queue.mu.Unlock()
	if !generic {
		if err := a.Queue.callDefaults(c, model, r.Header, true); err != nil {
			modelError(w, 400, err.Error())
			return
		}
	}
	retrySafe := !generic || model.RetrySafe
	var requestUntil time.Time
	c.Model = body.Model
	if !generic && body.Stream && r.URL.Path != "/v1/chat/completions" {
		modelError(w, 400, "streaming is supported for chat only")
		return
	}
	if e = a.Queue.addCall(c); e != nil {
		modelError(w, 400, "invalid model request")
		return
	}
	defer a.Queue.finishCall(c.ID)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	attempt := 0
	for {
		if r.Context().Err() != nil {
			return
		}
		var call ModelCall
		var target string
		var timedOut, requestExpired, engineLost bool
		e = a.Queue.update(func() error {
			a.Queue.maintain()
			call = *a.Queue.calls[c.ID]
			timedOut = a.Queue.clock.Now().Sub(call.Queued) >= a.Queue.models.Wait
			requestExpired = !requestUntil.IsZero() && !a.Queue.clock.Now().Before(requestUntil)
			engineLost = a.Queue.engineAttemptLost(&call)
			if m := a.Queue.s.Machines[call.Machine]; m != nil && !m.Gone && m.Session == call.Session && a.Queue.callAttemptLive(&call) {
				target = m.ModelURL
			}
			return nil
		})
		if e != nil {
			modelError(w, 503, "model call failed")
			return
		}
		if requestExpired {
			modelError(w, 504, errModelRequestTimeout.Error())
			return
		}
		if call.Error != "" {
			if call.PriorityStop != nil {
				modelError(w, 503, priorityPreemptionCause)
			} else {
				modelError(w, 503, call.Error)
			}
			return
		}
		if timedOut {
			modelError(w, 504, "maximum model wait exceeded")
			return
		}
		if target == "" && call.Machine != "" {
			if !retrySafe || engineLost && !model.RetrySafe || attempt >= 1 {
				modelError(w, 503, "model call failed; retry unavailable or exhausted")
				return
			}
			attempt++
			if e = a.Queue.retryCall(c.ID); e != nil {
				modelError(w, 503, "model call failed")
				return
			}
			continue
		}
		if target != "" {
			if requestUntil.IsZero() {
				a.Queue.mu.Lock()
				requestUntil = a.Queue.clock.Now().Add(model.requestTimeout())
				a.Queue.mu.Unlock()
			}
			proxyCtx, cancelCause := context.WithCancelCause(r.Context())
			cancel := func() { cancelCause(context.Canceled) }
			done := make(chan struct{})
			go func() {
				defer close(done)
				wake := time.NewTicker(20 * time.Millisecond)
				defer wake.Stop()
				for {
					select {
					case <-proxyCtx.Done():
						return
					case <-wake.C:
						a.Queue.mu.Lock()
						m := a.Queue.s.Machines[call.Machine]
						live := a.Queue.calls[c.ID]
						expired := !a.Queue.clock.Now().Before(requestUntil)
						preempted := live != nil && live.PriorityStop != nil && live.Error != ""
						lost := m == nil || m.Gone || m.Session != call.Session || live != nil && live.Error != "" || !a.Queue.callAttemptLive(&call)
						a.Queue.mu.Unlock()
						if expired {
							cancelCause(errModelRequestTimeout)
							return
						}
						if preempted {
							cancelCause(errPriorityPreemption)
							return
						}
						if lost {
							cancel()
							return
						}
					}
				}
			}()
			var requestBody io.Reader = bytes.NewReader(data)
			if generic {
				if _, e = spool.Seek(0, io.SeekStart); e != nil {
					cancel()
					<-done
					modelError(w, 503, "model call failed")
					return
				}
				// LimitReader intentionally exposes only io.Reader, so the HTTP
				// transport cannot close the replayable spool after attempt one.
				requestBody = io.LimitReader(spool, bodySize)
			}
			proxyPath := endpoint
			if generic {
				proxyPath = "/models/" + url.PathEscape(body.Model) + endpoint
			}
			req, e := http.NewRequestWithContext(proxyCtx, r.Method, target+proxyPath, requestBody)
			if e != nil {
				cancel()
				<-done
				modelError(w, 503, "model call failed")
				return
			}
			req.Header = proxyHeaders(r.Header)
			req.Header.Set("Authorization", "Bearer "+a.Token)
			req.Header.Set("X-Myce-Session", call.Session)
			req.Header.Set("X-Myce-Engine-Attempt", call.Attempt)
			if generic {
				req.ContentLength = bodySize
			}

			res, e := modelHTTPClient.Do(req)
			notForwarded := res != nil && call.Attempt != "" && res.Header.Get("X-Myce-Engine-Rejected") == call.Attempt
			a.Queue.mu.Lock()
			expired := !a.Queue.clock.Now().Before(requestUntil)
			a.Queue.mu.Unlock()
			if expired {
				if res != nil {
					res.Body.Close()
				}
				cancelCause(errModelRequestTimeout)
				<-done
				modelError(w, 504, errModelRequestTimeout.Error())
				return
			}
			a.Queue.mu.Lock()
			attemptLost := !a.Queue.callAttemptLive(&call)
			engineLost = a.Queue.engineAttemptLost(&call)
			a.Queue.mu.Unlock()
			if notForwarded {
				res.Body.Close()
				res, e = nil, errors.New("engine rejected before forwarding")
			} else if res != nil && (attemptLost || call.Attempt != "" && res.Header.Get("X-Myce-Engine-Failed") == call.Attempt) {
				engineLost = true
				res.Body.Close()
				res = nil
				e = errors.New("engine attempt lost")
			}
			if e == nil && res.StatusCode >= 400 && (generic || res.StatusCode < 500) {
				status := res.StatusCode
				res.Body.Close()
				cancel()
				<-done
				modelError(w, status, "upstream model request failed")
				return
			}
			if e == nil && res.StatusCode < 400 {
				if !body.Stream {
					response, readErr := io.ReadAll(res.Body)
					if readErr == nil {
						copyProxyHeaders(w.Header(), res.Header)
						w.WriteHeader(res.StatusCode)
						_, _ = w.Write(response)
						res.Body.Close()
						cancel()
						<-done
						return
					}
					e = readErr // Buffer non-stream responses so a lost worker can be retried safely.
				} else {
					started := false
					buf := make([]byte, 16384)
					for {
						n, readErr := res.Body.Read(buf)
						if n > 0 {
							if !started {
								copyProxyHeaders(w.Header(), res.Header)
								w.Header().Add("Trailer", "X-Myce-Error-Code")
								w.WriteHeader(res.StatusCode)
								started = true
							}
							if _, e = w.Write(buf[:n]); e != nil {
								res.Body.Close()
								cancel()
								<-done
								return
							}
							if f, ok := w.(http.Flusher); ok {
								f.Flush()
							}
						}
						if readErr != nil {
							// Unload can close the transport before the watcher runs.
							a.Queue.mu.Lock()
							live := a.Queue.calls[c.ID]
							preempted := live != nil && live.PriorityStop != nil && live.Error != ""
							a.Queue.mu.Unlock()
							if preempted {
								cancelCause(errPriorityPreemption)
							}
							if (readErr != io.EOF || errors.Is(context.Cause(proxyCtx), errPriorityPreemption)) && started && !generic {
								message := "worker lost after streaming response started; unsafe to retry"
								if errors.Is(context.Cause(proxyCtx), errModelRequestTimeout) {
									message = errModelRequestTimeout.Error()
								}
								if errors.Is(context.Cause(proxyCtx), errPriorityPreemption) {
									_, _ = fmt.Fprintf(w, "\ndata: {\"error\":{\"type\":\"interruption\",\"code\":\"priority_preemption\",\"message\":%q}}\n\n", errPriorityPreemption.Error())
								} else {
									_, _ = fmt.Fprintf(w, "\ndata: {\"error\":{\"message\":%q}}\n\n", message)
								}
								if f, ok := w.(http.Flusher); ok {
									f.Flush()
								}
							}
							if generic && started && errors.Is(context.Cause(proxyCtx), errPriorityPreemption) {
								w.Header().Set("X-Myce-Error-Code", priorityPreemptionCause)
								res.Body.Close()
								cancel()
								<-done
								return
							}
							if generic && started && readErr != io.EOF {
								res.Body.Close()
								cancel()
								<-done
								panic(http.ErrAbortHandler)
							}
							if started || readErr == io.EOF {
								if !started {
									copyProxyHeaders(w.Header(), res.Header)
									w.WriteHeader(res.StatusCode)
								}
								res.Body.Close()
								cancel()
								<-done
								return
							}
							e = readErr
							break
						}
					}
				}
			}
			if res != nil {
				res.Body.Close()
			}
			a.Queue.mu.Lock()
			expired = !a.Queue.clock.Now().Before(requestUntil)
			a.Queue.mu.Unlock()
			if expired {
				cancelCause(errModelRequestTimeout)
			}
			cancel()
			<-done
			if errors.Is(context.Cause(proxyCtx), errModelRequestTimeout) {
				modelError(w, 504, errModelRequestTimeout.Error())
				return
			}
			if r.Context().Err() != nil {
				return
			}
			a.Queue.mu.Lock()
			live := a.Queue.calls[c.ID]
			engineLost = engineLost || a.Queue.engineAttemptLost(&call)
			preempted := ""
			if live != nil {
				preempted = live.Error
				if preempted != "" && live.PriorityStop != nil {
					preempted = priorityPreemptionCause
				}
			}
			a.Queue.mu.Unlock()
			if preempted != "" {
				modelError(w, 503, preempted)
				return
			}
			if !notForwarded && (!retrySafe || engineLost && !model.RetrySafe) || attempt >= 1 {
				modelError(w, 503, "model call failed; retry unavailable or exhausted")
				return
			}
			attempt++
			e = a.Queue.retryCall(c.ID)
			if e != nil {
				modelError(w, 503, "model call failed")
				return
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
		}
	}
}

func (q *Queue) retryCall(id string) error {
	return q.update(func() error {
		live := q.calls[id]
		if live == nil {
			return errors.New("model call no longer active")
		}
		previous := *live
		if s := q.s.ModelSlots[slotKey(live.Machine, live.Model, live.Device)]; s != nil && s.Session == live.Session && s.Attempt == live.Attempt {
			s.InFlight = max(0, s.InFlight-1)
			s.LastUsed = q.clock.Now()
		}
		live.Exclude = live.Machine
		live.ExcludeAttempt = live.Attempt
		live.Attempt = ""
		live.Machine = ""
		live.StartedAt = nil
		live.WaitingSince = q.clock.Now()
		live.Session = ""
		q.refreshModelPriority(&previous)
		q.maintain()
		return nil
	})
}

func modelConfigHash(c ModelConfig) string {
	models := append([]Model(nil), c.Models...)
	sort.Slice(models, func(i, j int) bool { return models[i].Name < models[j].Name })
	data, _ := json.Marshal(models)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// Batch workers run one command at a time. Plan its release together with
// resident models, rather than requiring either allocation alone to suffice.
func (q *Queue) makeBatchRoom(j *Job, planned, handled map[string]bool) string {
	var best *placementPlan
	var selected *Job
	for _, m := range q.plannerMachines() {
		if !q.placementAllowed(j, m) || q.unreported[m.ID] || planned[targetKey(m)] {
			continue
		}
		winner := q.candidate(m, handled)
		if winner == nil || winner.ID != j.ID {
			continue
		}
		if p := q.roomPlan(winner, m, true); p != nil && q.completionPlanBetter(winner, p, best) {
			best, selected = p, winner
		}
	}
	if best == nil {
		return ""
	}
	q.applyPlacement(best, selected)
	return targetKey(best.machine)
}

var errPriorityPreemption = errors.New("model call interrupted by higher priority demand")

var errModelRequestTimeout = errors.New("model request timeout exceeded")

func (m Model) requestTimeout() time.Duration {
	if m.RequestTimeout == 0 {
		return 10 * time.Minute
	}
	return m.RequestTimeout
}

func (q *Queue) modelPlacement(model Model) *ModelPlacement {
	if model.Project == "" && len(model.AllowedMachines) == 0 {
		return nil
	}
	return &ModelPlacement{Project: model.Project, AllowedMachines: append([]string(nil), model.AllowedMachines...), ProjectMachines: append([]string(nil), q.s.Policies[model.Project].AllowedMachines...)}
}
