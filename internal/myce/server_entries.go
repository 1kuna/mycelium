package myce

import (
	"errors"
	"io"
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
	if c.Project == "" || strings.TrimSpace(c.Project) != c.Project || rank(c.Priority) == 3 {
		return errors.New("project and priority required")
	}
	// Supplied malformed deadlines are errors even when priority is not deadline.
	if _, supplied := h["X-Myce-Deadline"]; supplied || c.Priority == "deadline" {
		deadline, err := time.Parse(time.RFC3339, h.Get("X-Myce-Deadline"))
		if err != nil {
			return errors.New("invalid deadline header")
		}
		c.Deadline = &deadline
	}
	for _, name := range []string{"X-Myce-Project", "X-Myce-Priority", "X-Myce-Deadline"} {
		if values, supplied := h[name]; supplied && len(values) != 1 {
			return errors.New("ambiguous routing header")
		}
	}
	return nil
}

func modelFenceAllows(model Model, m *Machine) bool {
	return (len(model.AllowedMachines) == 0 || contains(model.AllowedMachines, m.ID)) && (model.LocalSource == "" || m.ApprovedModels[model.LocalSource] == model.SHA256)
}
func (p *ModelPlacement) allows(machine string) bool {
	return p == nil || (len(p.AllowedMachines) == 0 || contains(p.AllowedMachines, machine)) && (len(p.ProjectMachines) == 0 || contains(p.ProjectMachines, machine))
}

func privateLocalDirectory(root string) bool {
	info, err := os.Lstat(root)
	return err == nil && info.IsDir() && info.Mode().Perm() == 0700
}
