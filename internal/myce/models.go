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
		if m.Name == "" || seen[m.Name] || (m.Path == "" && !m.FileStore) || m.MemoryGB <= 0 || math.IsNaN(m.MemoryGB) || math.IsInf(m.MemoryGB, 0) || len(m.Platforms) == 0 {
			return errors.New("models require unique names, path, positive memory and platforms")
		}
		if m.Engine != "fake" && (m.SizeBytes <= 0 || len(m.SHA256) != 64) {
			return fmt.Errorf("model %s requires size_bytes and sha256", m.Name)
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
	InFlight      int    `json:"calls_in_flight,omitempty"`
	CallsMeasured bool   `json:"calls_measured,omitempty"`
	Name          string `json:"name"`
	State         string `json:"state"`
	Error         string `json:"error,omitempty"`
}
type ModelCall struct {
	StopAt                           *time.Time
	ProductionHost                   string
	WaitingSince                     time.Time
	StartedAt                        *time.Time
	ID, Model, Project, Priority     string
	Deadline                         *time.Time
	Queued                           time.Time
	Machine, Session, Exclude, Error string
}

func slotKey(machine, model string) string { return machine + "/" + model }
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
		if s.Machine == machine {
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
		if contains([]string{"ready", "draining", "unloading", "unload_failed"}, r.State) {
			resident[r.Name] = true
		}
	}
	reservation := 0.0
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && !resident[s.Model] {
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
			if r.Name == s.Model {
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

				if r.State == "failed" {
					for _, c := range q.calls {
						if c.Model == s.Model && c.Machine == "" {
							c.Error = "model load failed"
						}
					}
					if q.slotPinned(s) {
						if s.State != "failed" || s.RetryAt == nil {
							if s.RetryDelay == 0 {
								s.RetryDelay = time.Second
							} else {
								s.RetryDelay = min(s.RetryDelay*2, 30*time.Second)
							}
							next := q.clock.Now().Add(s.RetryDelay)
							s.RetryAt = &next
						}
						s.State = "failed"
						s.Error = ceilingDiagnostic(r.Error, "model load or unload failed")
					} else {
						delete(q.s.ModelSlots, key)
					}
				} else if r.State == "unload_failed" {
					s.State = "unload_failed"
					s.Error = ceilingDiagnostic(r.Error, "model load or unload failed")
				} else if r.State == "ready" && s.State == "loading" {
					s.State = "ready"
					s.Error = ""
					s.RetryAt = nil
					s.RetryDelay = 0
					s.LastUsed = q.clock.Now()
				} else if r.State == "unloaded" && s.State == "draining" && s.InFlight == 0 {
					delete(q.s.ModelSlots, key)
				}
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
		m := q.s.Machines[s.Machine]
		if m == nil || m.Gone || m.Session != s.Session {
			if m != nil && m.Session == s.Session && (m.HostMemoryBudgetGB > 0 || m.hostBudgetGB > 0 || q.s.Hosts[hostID(m)] != nil && q.s.Hosts[hostID(m)].Policy.ProductionHold) {
				continue
			}
			delete(q.s.ModelSlots, k)
			continue
		}
		if availabilityBlocked(m) || m.Leaving || !q.slotPinned(s) && s.State == "ready" && s.InFlight == 0 && now.Sub(s.LastUsed) >= q.models.Idle {
			s.State = "draining"
		}
	}
	if q.networkQuiet {
		return
	}
	pending := []*ModelCall{}
	for _, c := range q.calls {
		if c.Machine == "" && c.Error == "" && q.capReason(callJob(c)) == "" {
			pending = append(pending, c)
		}
	}
	sort.Slice(pending, func(i, j int) bool { return q.fairBefore(callJob(pending[i]), callJob(pending[j]), &Machine{}) })
	for _, c := range pending {
		if q.capReason(callJob(c)) != "" {
			continue
		}
		model, ok := q.model(c.Model)
		if !ok {
			c.Error = "unknown model"
			continue
		}
		// Ready affinity precedes loading a second copy.
		machines := []*Machine{}
		for _, m := range q.s.Machines {
			machines = append(machines, m)
		}
		sort.Slice(machines, func(i, j int) bool {
			a, b := q.loadSeconds(c.Model, machines[i]), q.loadSeconds(c.Model, machines[j])
			if a != b {
				return a < b
			}
			ac, bc := contains(machines[i].CachedModels, c.Model), contains(machines[j].CachedModels, c.Model)
			if ac != bc {
				return ac
			}
			return machines[i].ID < machines[j].ID
		})
		for _, m := range machines {
			if !q.placementAllowed(callJob(c), m) || q.capReasonFor(callJob(c), m) != "" || m.MemoryError != "" || m.Gone || m.Leaving || m.ID == c.Exclude || m.ModelURL == "" || !modelFitsMachine(model, m) || q.unreported[m.ID] {
				continue
			}
			s := q.s.ModelSlots[slotKey(m.ID, c.Model)]
			if s != nil && s.State == "ready" && (model.MaxInFlight == 0 || s.InFlight < model.MaxInFlight) && !q.batchBeforeCall(c, m) {
				c.Machine = m.ID
				if c.Priority == "production" {
					s.ProductionUsed = true
				}
				c.StartedAt = &now
				c.Session = m.Session
				if s.InFlight == 0 || rank(c.Priority) < rank(s.Priority) {
					s.Priority = c.Priority
				}
				s.InFlight++
				s.LastUsed = now
				break
			}
		}
		if c.Machine != "" {
			continue
		}

		eligible := []*Machine{}
		loading := false
		for _, m := range machines {
			if !q.placementAllowed(callJob(c), m) || q.capReasonFor(callJob(c), m) != "" || m.MemoryError != "" || m.Gone || m.Leaving || m.ID == c.Exclude || m.ModelURL == "" || !modelFitsMachine(model, m) || q.unreported[m.ID] || q.batchBeforeCall(c, m) {
				continue
			}
			if s := q.s.ModelSlots[slotKey(m.ID, c.Model)]; s != nil {
				if s.State == "loading" {
					if rank(c.Priority) < rank(s.Priority) {
						s.Priority = c.Priority
						if c.Priority == "production" {
							s.ProductionUsed = true
						}
					}
					loading = true
				}
				continue
			}
			eligible = append(eligible, m)
		}
		if loading {
			continue
		}
		allocated := false
		// Check all free workers before draining a model or stopping any batch.
		for _, m := range eligible {
			if m.FreeMemoryGB >= model.MemoryGB && hostFits(m, model.MemoryGB, model.HostMemoryGB) {
				q.s.ModelSlots[slotKey(m.ID, c.Model)] = &ModelSlot{Model: c.Model, Machine: m.ID, Session: m.Session, State: "loading", Priority: c.Priority, ProductionUsed: c.Priority == "production", LastUsed: now}
				m.FreeMemoryGB -= model.MemoryGB
				q.refreshHostMemory()
				allocated = true
				break
			}
		}
		if allocated {
			continue
		}
		for _, m := range eligible {
			if _, _, ok := q.hostReleases(m, model.MemoryGB, model.HostMemoryGB, c.Priority); !ok {
				continue
			}
			possible := m.FreeMemoryGB
			for _, s := range q.s.ModelSlots {
				if s.Machine == m.ID && !q.slotPinned(s) && s.State != "unload_failed" && rank(s.Priority) > rank(c.Priority) {
					mm, _ := q.model(s.Model)
					possible += mm.MemoryGB
				}
			}
			var victim *Job
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") && (j.Spec.Resumable || c.Priority == "production") && (c.Priority == "production" || (!m.CheckpointUnavailable && q.checkpointsAvailable())) && rank(j.Spec.Priority) > rank(c.Priority) {
					victim = j
					possible += j.Spec.Needs.MemoryGB
				}
			}
			if possible < model.MemoryGB {
				continue
			}
			q.reclaimHost(m, model.MemoryGB, model.HostMemoryGB, c.Priority, "model call")
			q.makeModelRoom(m, model.MemoryGB, c.Priority)
			if victim != nil && m.FreeMemoryGB+q.modelMemory(m.ID) < model.MemoryGB {
				q.stopFor(victim, c.Priority, "model call")
			}
			// Prefer draining models alone; if that cannot suffice, save the batch.
			reclaimable := m.FreeMemoryGB
			for _, s := range q.s.ModelSlots {
				if s.Machine == m.ID && s.State == "draining" {
					mm, _ := q.model(s.Model)
					reclaimable += mm.MemoryGB
				}
			}
			if reclaimable < model.MemoryGB && victim != nil {
				q.stopFor(victim, c.Priority, "model call")
			}
			break
		}

	}

	waiting := []*Job{}
	for _, j := range q.s.Jobs {
		if j.State == "queued" {
			waiting = append(waiting, j)
		}
	}
	sort.Slice(waiting, func(i, j int) bool { return before(waiting[i], waiting[j], &Machine{}) })
	// Each worker runs one batch command; a release can cover only one waiting job.
	planned := map[string]bool{}
	for _, j := range waiting {
		if j.Spec.Priority == "experiment" || !q.checkpointReady(j, &Machine{}) {
			continue
		} // Nothing has a lower priority to reclaim.
		if machine := q.makeBatchRoom(j, planned); machine != "" {
			planned[machine] = true
		}
	}
}
func callJob(c *ModelCall) *Job {
	j := &Job{ProductionHost: c.ProductionHost, ID: c.ID, Queued: c.Queued, WaitingSince: c.WaitingSince, StartedAt: c.StartedAt, Spec: Spec{Project: c.Project, Priority: c.Priority, Deadline: c.Deadline}}
	j.Spec.Needs.Model = c.Model
	return j
}

// A pending request may precede a batch only on a worker where it can actually
// run or make room. Counting every resident model incorrectly blocks batches
// behind requests that cannot reclaim higher-priority work.
func (q *Queue) callCanUse(m *Machine, c *ModelCall) bool {
	model, ok := q.model(c.Model)
	if !ok || !q.placementAllowed(callJob(c), m) || c.Machine != "" || c.Error != "" || q.capReasonFor(callJob(c), m) != "" || m.MemoryError != "" || m.Gone || m.Leaving || m.ID == c.Exclude || m.ModelURL == "" || !modelFitsMachine(model, m) || q.unreported[m.ID] {
		return false
	}
	if s := q.s.ModelSlots[slotKey(m.ID, c.Model)]; s != nil && s.State == "ready" {
		return model.MaxInFlight == 0 || s.InFlight < model.MaxInFlight
	}
	possible := m.FreeMemoryGB
	for _, s := range q.s.ModelSlots {
		if s.Machine != m.ID || s.State == "unload_failed" {
			continue
		}
		if s.Model == c.Model && s.State == "loading" {
			return true
		}
		if !q.slotPinned(s) && s.InFlight == 0 && rank(s.Priority) > rank(c.Priority) {
			resident, _ := q.model(s.Model)
			possible += resident.MemoryGB
		}
	}
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") && (j.Spec.Resumable || c.Priority == "production") && (c.Priority == "production" || (!m.CheckpointUnavailable && q.checkpointsAvailable())) && rank(j.Spec.Priority) > rank(c.Priority) {
			possible += j.Spec.Needs.MemoryGB
		}
	}
	_, _, hostOK := q.hostReleases(m, model.MemoryGB, model.HostMemoryGB, c.Priority)
	return possible >= model.MemoryGB && hostOK
}
func (q *Queue) batchBeforeCall(c *ModelCall, m *Machine) bool {
	// Reclaimable room depends on priority, not on the individual waiting job.
	room := [3]float64{m.FreeMemoryGB, m.FreeMemoryGB, m.FreeMemoryGB}
	for priority := 0; priority < 3; priority++ {
		for _, s := range q.s.ModelSlots {
			if s.Machine == m.ID && !q.slotPinned(s) && s.State != "unload_failed" && rank(s.Priority) > priority {
				model, _ := q.model(s.Model)
				room[priority] += model.MemoryGB
			}
		}
		for _, r := range q.s.Jobs {
			if r.Machine == m.ID && (r.State == "running" || r.State == "stopping") && (r.Spec.Resumable || priority == 0) && rank(r.Spec.Priority) > priority {
				room[priority] += r.Spec.Needs.MemoryGB
			}
		}
	}
	scores := map[string]float64{}
	call := callJob(c)
	score := func(j *Job) float64 {
		if n, ok := scores[j.Spec.Project]; ok {
			return n
		}
		n := q.fairScore(j, m)
		scores[j.Spec.Project] = n
		return n
	}
	for _, j := range q.s.Jobs {
		if j.State != "queued" || rank(j.Spec.Priority) > rank(c.Priority) || q.capReason(j) != "" || !q.placementAllowed(j, m) || !fits(j, m, room[rank(j.Spec.Priority)]) {
			continue
		}
		if j.Spec.Priority == "experiment" && c.Priority == "experiment" && j.Spec.Project != c.Project {
			a, b := score(j), score(call)
			if math.Abs(a-b) > 1e-6 {
				if a < b {
					return true
				}
				continue
			}
		}
		if before(j, call, m) {
			return true
		}
	}
	return false
}

func (q *Queue) makeModelRoom(m *Machine, need float64, priority string) {
	slots := []*ModelSlot{}
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID && !q.slotPinned(s) && s.State != "unload_failed" && rank(s.Priority) > rank(priority) {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool {
		if rank(slots[i].Priority) != rank(slots[j].Priority) {
			return rank(slots[i].Priority) > rank(slots[j].Priority)
		}
		return slots[i].Model < slots[j].Model
	})
	room := m.FreeMemoryGB
	for _, s := range slots {
		if room >= need {
			break
		}
		s.State = "draining"
		model, _ := q.model(s.Model)
		room += model.MemoryGB
	}
}
func (q *Queue) modelPlan(machine string) []ModelSlot {
	out := []ModelSlot{}
	for _, s := range q.s.ModelSlots {
		if s.Machine == machine {
			slot := *s
			model, _ := q.model(s.Model)
			if model.Environment != "" {
				recipe := q.environments.Environments[model.Environment][q.s.Machines[machine].GPU]
				slot.EnvironmentRecipe = &recipe
			}
			out = append(out, slot)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}
func (q *Queue) addCall(c *ModelCall) error {
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
			if s := q.s.ModelSlots[slotKey(c.Machine, c.Model)]; s != nil && s.Session == c.Session {
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
	s := q.s.ModelSlots[slotKey(c.Machine, c.Model)]
	if s == nil || s.Session != c.Session {
		return
	}
	s.Priority = "experiment"
	for _, live := range q.calls {
		if live.Machine == c.Machine && live.Session == c.Session && live.Model == c.Model && rank(live.Priority) < rank(s.Priority) {
			s.Priority = live.Priority
		}
	}
}
func modelError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message, "type": "mycelium_error"}})
}
func (a API) modelCall(w http.ResponseWriter, r *http.Request) {
	c := &ModelCall{Project: r.Header.Get("X-Myce-Project"), Priority: r.Header.Get("X-Myce-Priority")}
	if c.Project == "" || rank(c.Priority) == 3 {
		modelError(w, 400, "X-Myce-Project and X-Myce-Priority are required")
		return
	}
	if c.Priority == "deadline" {
		d, err := time.Parse(time.RFC3339, r.Header.Get("X-Myce-Deadline"))
		if err != nil {
			modelError(w, 400, "deadline priority requires X-Myce-Deadline (RFC3339)")
			return
		}
		c.Deadline = &d
	}
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
		var timedOut, requestExpired bool
		e = a.Queue.update(func() error {
			a.Queue.maintain()
			call = *a.Queue.calls[c.ID]
			timedOut = a.Queue.clock.Now().Sub(call.Queued) >= a.Queue.models.Wait
			requestExpired = !requestUntil.IsZero() && !a.Queue.clock.Now().Before(requestUntil)
			if m := a.Queue.s.Machines[call.Machine]; m != nil && !m.Gone && m.Session == call.Session {
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
			modelError(w, 503, call.Error)
			return
		}
		if timedOut {
			modelError(w, 504, "maximum model wait exceeded")
			return
		}
		if target == "" && call.Machine != "" {
			if !retrySafe || attempt >= 1 {
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
						lost := m == nil || m.Gone || m.Session != call.Session || live != nil && live.Error != ""
						a.Queue.mu.Unlock()
						if expired {
							cancelCause(errModelRequestTimeout)
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
			if generic {
				req.ContentLength = bodySize
			}

			res, e := modelHTTPClient.Do(req)
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
							if readErr != io.EOF && started && !generic {
								message := "worker lost after streaming response started; unsafe to retry"
								if errors.Is(context.Cause(proxyCtx), errModelRequestTimeout) {
									message = errModelRequestTimeout.Error()
								}
								_, _ = fmt.Fprintf(w, "\ndata: {\"error\":{\"message\":%q}}\n\n", message)
								if f, ok := w.(http.Flusher); ok {
									f.Flush()
								}
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
			preempted := ""
			if live != nil {
				preempted = live.Error
			}
			a.Queue.mu.Unlock()
			if preempted != "" {
				modelError(w, 503, preempted)
				return
			}
			if !retrySafe || attempt >= 1 {
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
		if s := q.s.ModelSlots[slotKey(live.Machine, live.Model)]; s != nil && s.Session == live.Session {
			s.InFlight = max(0, s.InFlight-1)
			s.LastUsed = q.clock.Now()
		}
		live.Exclude = live.Machine
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
func (q *Queue) makeBatchRoom(j *Job, planned map[string]bool) string {
	type plan struct {
		machine string
		batch   *Job
		slots   []*ModelSlot
		stops   int
		cost    int
	}
	var best *plan
	for _, m := range q.s.Machines {
		if !q.placementAllowed(j, m) || q.unreported[m.ID] || planned[m.ID] {
			continue
		}
		p := &plan{machine: m.ID}
		room := m.FreeMemoryGB
		if release, ok := q.s.BatchReleases[m.ID]; ok && release.Session == m.Session && q.clock.Now().Before(release.Until) {
			room += release.MemoryGB
		}
		blocked := false
		for _, r := range q.s.Jobs {
			if r.Machine != m.ID || (r.State != "running" && r.State != "stopping") {
				continue
			}
			if r.State == "running" && (rank(r.Spec.Priority) <= rank(j.Spec.Priority) || (!r.Spec.Resumable && j.Spec.Priority != "production")) {
				blocked = true
				break
			}
			p.batch = r
			room += r.Spec.Needs.MemoryGB
			if r.State == "running" {
				p.stops++
				if !r.Spec.Resumable {
					p.cost++
				}
			}
		}
		if blocked {
			continue
		}
		slots := []*ModelSlot{}
		for _, s := range q.s.ModelSlots {
			if s.Machine != m.ID || q.slotPinned(s) || s.State == "unload_failed" || s.State == "failed" || rank(s.Priority) <= rank(j.Spec.Priority) {
				continue
			}
			model, _ := q.model(s.Model)
			if s.State == "draining" {
				room += model.MemoryGB
			} else {
				slots = append(slots, s)
			}
		}
		// Largest allocations first establish the minimum number of releases.
		sort.Slice(slots, func(a, b int) bool {
			ma, _ := q.model(slots[a].Model)
			mb, _ := q.model(slots[b].Model)
			if ma.MemoryGB != mb.MemoryGB {
				return ma.MemoryGB > mb.MemoryGB
			}
			return slots[a].Model < slots[b].Model
		})
		possible, count := room, 0
		for _, s := range slots {
			if possible >= j.Spec.Needs.MemoryGB {
				break
			}
			model, _ := q.model(s.Model)
			possible += model.MemoryGB
			count++
		}
		hostJobs, hostSlots, hostOK := q.hostReleases(m, j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB, j.Spec.Priority)
		if !hostOK || !fitsWithoutHostMemory(j, m, possible) {
			continue
		}
		// Within that count prefer idle models, only when the remaining largest
		// allocations can still cover the deficit. Preference cannot block work.
		for count > 0 {
			chosen := 0
			for i, s := range slots {
				if s.InFlight != 0 {
					continue
				}
				model, _ := q.model(s.Model)
				capacity, n := room+model.MemoryGB, 0
				for k, other := range slots {
					if k != i && n < count-1 {
						model, _ := q.model(other.Model)
						capacity += model.MemoryGB
						n++
					}
				}
				if capacity >= j.Spec.Needs.MemoryGB {
					chosen = i
					break
				}
			}
			s := slots[chosen]
			model, _ := q.model(s.Model)
			room += model.MemoryGB
			p.slots = append(p.slots, s)
			p.stops++
			if s.InFlight > 0 {
				p.cost++
			}
			slots = append(slots[:chosen], slots[chosen+1:]...)
			count--
		}
		for _, victim := range hostJobs {
			if victim != p.batch && victim.State == "running" {
				p.stops++
				if !victim.Spec.Resumable {
					p.cost++
				}
			}
		}
		for _, victim := range hostSlots {
			found := false
			for _, s := range p.slots {
				if s == victim {
					found = true
				}
			}
			if !found && victim.State != "draining" {
				p.stops++
				if victim.InFlight > 0 {
					p.cost++
				}
			}
		}
		if best == nil || p.stops < best.stops || p.stops == best.stops && (p.cost < best.cost || p.cost == best.cost && p.machine < best.machine) {
			best = p
		}
	}
	if best == nil {
		return ""
	}
	q.reclaimHost(q.s.Machines[best.machine], j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB, j.Spec.Priority, "job")
	if best.batch != nil && rank(best.batch.Spec.Priority) > rank(j.Spec.Priority) {
		q.stopFor(best.batch, j.Spec.Priority, "job")
	}
	for _, s := range best.slots {
		s.State = "draining"
	}
	return best.machine
}

var errModelRequestTimeout = errors.New("model request timeout exceeded")

func (m Model) requestTimeout() time.Duration {
	if m.RequestTimeout == 0 {
		return 10 * time.Minute
	}
	return m.RequestTimeout
}
