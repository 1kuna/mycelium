package myce

import (
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Placement is copied into load plans so a worker checks the queue's current
// fences as well as its catalog. Caller project remains independent of owner.
type ModelPlacement struct {
	Project         string   `json:"project,omitempty"`
	AllowedMachines []string `json:"allowed_machines,omitempty"`
	ProjectMachines []string `json:"project_machines,omitempty"`
}

func (q *Queue) modelFitsMachine(model Model, m *Machine) bool {
	return logPolicyFits(model.Logs, m) && modelFitsMachine(model, m) && q.projectAllows(model.Project, m.ID)
}

// Unknown storage checks preserve only ready slots from this worker session.
// All remaining fences (owner, machine, platform, log policy) still apply.
func (q *Queue) residentModel(model Model, m *Machine) Model {
	if model.LocalSource != "" && m.ModelApprovalStates[model.LocalSource] == approvalUnknown {
		if s := q.s.ModelSlots[slotKey(m.ID, model.Name)]; s != nil && s.Session == m.Session && s.State == "ready" && (s.ApprovalUnknownSince == nil || q.clock.Now().Sub(*s.ApprovalUnknownSince) <= localApprovalUnknownGrace) {
			model.LocalSource = ""
		}
	}
	return model
}

const localApprovalUnknownGrace = 5 * time.Minute

func (q *Queue) maintainApprovalGrace() {
	now := q.clock.Now()
	for _, slot := range q.s.ModelSlots {
		model, ok := q.model(slot.Model)
		machine := q.s.Machines[slot.Machine]
		if !ok || model.LocalSource == "" || machine == nil || machine.Session != slot.Session || machine.ModelApprovalStates[model.LocalSource] != approvalUnknown {
			slot.ApprovalUnknownSince = nil
			continue
		}
		if slot.State != "ready" {
			continue
		}
		if slot.ApprovalUnknownSince == nil {
			since := now
			slot.ApprovalUnknownSince = &since
		}
		if now.Sub(*slot.ApprovalUnknownSince) > localApprovalUnknownGrace {
			slot.State = "draining"
			slot.Error = "local model approval unknown for more than 5m; draining until the source is verified or re-approved"
			if !now.Before(q.nextApprovalWarning) {
				log.Print(slot.Error)
				q.nextApprovalWarning = now.Add(time.Minute)
			}
		}
	}
}

func (q *Queue) modelUsableMachine(model Model, m *Machine) bool {
	return q.modelFitsMachine(q.residentModel(model, m), m)
}

func (q *Queue) callPlacementAllowed(c *ModelCall, m *Machine) bool {
	return q.placementAllowedForUse(callJob(c), m, true)
}

var localNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var environmentNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func localName(name string) bool { return localNamePattern.MatchString(name) && name != "MYCE_TOKEN" }
func reservedModelEnv(k string) bool {
	if strings.HasPrefix(k, "MYCE_") || strings.HasPrefix(k, "LLAMA_ARG_") {
		return true
	}
	switch k {
	case "CUDA_VISIBLE_DEVICES", "ZE_AFFINITY_MASK", "ONEAPI_DEVICE_SELECTOR", "HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "PATH", "VIRTUAL_ENV", "PYTHONHOME", "PYTHONPATH", "LD_PRELOAD", "LD_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH":
		return true
	}
	return false
}
func validateModelEnv(m Model) error {
	for k, v := range m.Env {
		if !environmentNamePattern.MatchString(k) || reservedModelEnv(k) || strings.ContainsRune(v, 0) {
			return errors.New("invalid or worker-owned model environment variable")
		}
		if _, exists := m.SecretEnv[k]; exists {
			return errors.New("env and secret_env must not overlap")
		}
	}
	for k, name := range m.SecretEnv {
		if !environmentNamePattern.MatchString(k) || reservedModelEnv(k) || !localName(name) {
			return errors.New("invalid named secret environment reference")
		}
	}
	if len(m.SecretEnv) > 0 && m.Logs != "none" {
		return errors.New("secret_env requires logs: none to prevent process output from retaining secrets")
	}
	return nil
}

// Secrets are read only at launch, never attached to a Model or a worker record.
// Errors deliberately omit filesystem errors, which can contain private paths.
func readModelSecrets(root string, refs map[string]string, fleetToken string) (map[string]string, error) {
	out := map[string]string{}
	if len(refs) == 0 {
		return out, nil
	}
	unavailable := errors.New("local secret unavailable; check provisioning and mode 0600")
	if !privateLocalDirectory(root) {
		return nil, unavailable
	}
	for key, name := range refs {
		if !localName(name) {
			return nil, unavailable
		}
		path := filepath.Join(root, name)
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
			return nil, unavailable
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, unavailable
		}
		actual, err := f.Stat()
		if err != nil || (!os.SameFile(st, actual) || actual.Mode().Perm() != 0600) {
			f.Close()
			return nil, unavailable
		}
		data, err := io.ReadAll(io.LimitReader(f, (64<<10)+1))
		closeErr := f.Close()
		if err != nil || closeErr != nil || len(data) == 0 || len(data) > 64<<10 || strings.ContainsRune(string(data), 0) {
			return nil, unavailable
		}
		value := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
		token := os.Getenv("MYCE_TOKEN")
		if value == "" || (token != "" && strings.Contains(value, token) || fleetToken != "" && strings.Contains(value, fleetToken)) {
			return nil, unavailable
		}
		out[key] = value
	}
	return out, nil
}

func (q *Queue) callDefaults(c *ModelCall, model Model, h http.Header, defaults bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, name := range []string{"X-Myce-Project", "X-Myce-Priority", "X-Myce-Deadline"} {
		if values, supplied := h[name]; supplied && len(values) != 1 {
			return errors.New("duplicate model routing header: supply each X-Myce header once")
		}
	}
	c.Project = strings.TrimSpace(c.Project)
	if defaults && model.Project != "" {
		if _, supplied := h["X-Myce-Project"]; !supplied {
			c.Project = model.Project
		}
		if _, supplied := h["X-Myce-Priority"]; !supplied {
			c.Priority = q.s.Policies[c.Project].DefaultCallPriority
			if c.Priority == "" {
				c.Priority = "experiment"
			}
		}
	}
	if c.Project == "" || c.Priority == "" {
		if !defaults {
			return errors.New("X-Myce-Project and X-Myce-Priority are required for generic model routes")
		}
		return errors.New("X-Myce-Project and X-Myce-Priority are required (or set the entry's project/default_call_priority)")
	}
	if legacyRank(c.Priority) == 3 {
		return errors.New("X-Myce-Priority must be production, deadline, or experiment")
	}
	c.Deadline = nil
	if c.Priority == "deadline" {
		deadline, err := time.Parse(time.RFC3339, h.Get("X-Myce-Deadline"))
		if err != nil {
			return errors.New("X-Myce-Deadline must be RFC3339 for deadline priority")
		}
		c.Deadline = &deadline
	}
	return nil
}

func modelFenceAllows(model Model, m *Machine) bool {
	return (len(model.AllowedMachines) == 0 || contains(model.AllowedMachines, m.ID)) && (model.LocalSource == "" || m.ModelApprovalStates[model.LocalSource] == approvalVerified && m.ApprovedModels[model.LocalSource] == model.SHA256)
}
func (p *ModelPlacement) allows(machine string) bool {
	return p == nil || (len(p.AllowedMachines) == 0 || contains(p.AllowedMachines, machine)) && (len(p.ProjectMachines) == 0 || contains(p.ProjectMachines, machine))
}

func privateLocalDirectory(root string) bool {
	info, err := os.Lstat(root)
	return err == nil && info.IsDir() && info.Mode().Perm() == 0700
}
