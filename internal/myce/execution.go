package myce

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"
)

// RunSpec is the existing worker wire contract. Composition lowers into it;
// there is no separate extension executor.
type RunSpec struct {
	Docker           bool              `json:"docker,omitempty" yaml:"docker,omitempty"`
	DockerNetwork    string            `json:"docker_network,omitempty" yaml:"docker_network,omitempty"`
	DockerMemoryGB   float64           `json:"docker_memory_gb,omitempty" yaml:"docker_memory_gb,omitempty"`
	DockerMemorySwap *float64          `json:"docker_memory_swap,omitempty" yaml:"docker_memory_swap,omitempty"`
	DockerSidecars   []DockerSidecar   `json:"docker_sidecars,omitempty" yaml:"docker_sidecars,omitempty"`
	Command          []string          `json:"command" yaml:"command"`
	Workdir          string            `json:"workdir,omitempty" yaml:"workdir,omitempty"`
	Env              map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	Sidecars         []NativeSidecar   `json:"sidecars,omitempty" yaml:"sidecars,omitempty"`
}

type ResourceNeeds struct {
	MemoryGB     float64  `json:"memory_gb" yaml:"memory_gb"`
	HostMemoryGB float64  `json:"host_memory_gb,omitempty" yaml:"host_memory_gb,omitempty"`
	Platform     Platform `json:"platform" yaml:"platform"`
	Model        string   `json:"model,omitempty" yaml:"model,omitempty"`
}

type ExecutionSpec struct {
	Main     ExecutionMember `json:"main" yaml:"main"`
	Sidecars []NativeSidecar `json:"sidecars,omitempty" yaml:"sidecars,omitempty"`
}

type ExecutionMember struct {
	Command     []string          `json:"command" yaml:"command"`
	Environment string            `json:"environment,omitempty" yaml:"environment,omitempty"`
	Workdir     string            `json:"workdir,omitempty" yaml:"workdir,omitempty"`
	Env         map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	MemoryGB    float64           `json:"memory_gb,omitempty" yaml:"memory_gb,omitempty"`
}

// NativeSidecar reserves D4's main-plus-members shape. E1 cannot launch groups.
// Port nil is a plain helper; zero requests an allocated loopback port.
type NativeSidecar struct {
	Name        string            `json:"name" yaml:"name"`
	Command     []string          `json:"command" yaml:"command"`
	Environment string            `json:"environment,omitempty" yaml:"environment,omitempty"`
	Env         map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	MemoryGB    float64           `json:"memory_gb" yaml:"memory_gb"`
	Port        *int              `json:"port,omitempty" yaml:"port,omitempty"`
	Readiness   *NativeReadiness  `json:"readiness,omitempty" yaml:"readiness,omitempty"`
}
type NativeReadiness struct {
	Path    string `json:"path" yaml:"path"`
	Timeout string `json:"timeout" yaml:"timeout"`
}

func (e ExecutionSpec) validate() error {
	if len(e.Main.Command) == 0 || e.Main.Command[0] == "" {
		return errors.New("execution.main.command is required")
	}
	if !validMemoryValue(e.Main.MemoryGB) {
		return errors.New("invalid main memory cap")
	}
	names := map[string]bool{}
	for _, s := range e.Sidecars {
		normalized := strings.ToUpper(strings.ReplaceAll(s.Name, "-", "_"))
		if !extensionName.MatchString(s.Name) || names[normalized] || len(s.Command) == 0 || s.Command[0] == "" || !validMemoryValue(s.MemoryGB) || s.MemoryGB == 0 {
			return errors.New("sidecars require unique names, commands and positive finite RAM caps")
		}
		names[normalized] = true
		if s.Port != nil && (*s.Port < 0 || *s.Port > 65535) {
			return errors.New("invalid sidecar port")
		}
		if s.Readiness != nil && (s.Port == nil || !strings.HasPrefix(s.Readiness.Path, "/") || strings.HasPrefix(s.Readiness.Path, "//") || !positiveDuration(s.Readiness.Timeout)) {
			return errors.New("readiness requires a local port, absolute path and positive timeout")
		}
		if err := validateMemberEnv(s.Env); err != nil {
			return err
		}
	}
	return validateMemberEnv(e.Main.Env)
}
func validateMemberEnv(env map[string]string) error {
	for k := range env {
		if strings.HasPrefix(strings.ToUpper(k), "MYCE_") || dockerDeviceEnvironment(strings.ToUpper(k)) {
			return fmt.Errorf("execution env cannot override worker control %s", k)
		}
	}
	return nil
}

// Old run descriptions and composed descriptions meet at this one adapter.
func lowerExecution(s *Spec) error {
	if s.Execution == nil {
		if len(s.Run.Sidecars) > 0 {
			return errors.New("native sidecars require D4")
		}
		return nil
	}
	if !reflect.ValueOf(s.Run).IsZero() {
		return errors.New("choose run or execution, not both")
	}
	e := s.Execution
	if err := e.validate(); err != nil {
		return err
	}
	if len(e.Sidecars) > 0 || e.Main.MemoryGB > 0 {
		return errors.New("native sidecars and member RAM caps require D4")
	}
	if e.Main.Environment != "" {
		if s.Environment.Key() != "" {
			return errors.New("choose environment or execution.main.environment")
		}
		s.Environment = Environment{Name: e.Main.Environment}
	}
	s.Run = RunSpec{Command: slices.Clone(e.Main.Command), Workdir: e.Main.Workdir, Env: maps.Clone(e.Main.Env)}
	s.Execution = nil
	return nil
}

func positiveDuration(s string) bool { d, e := time.ParseDuration(s); return e == nil && d > 0 }

// The finite substitutions are literal argv/env replacement, never shell text.
func executionReplacer(work, root, reference string, target ExecutionTarget) *strings.Replacer {
	pairs := []string{"{work}", work}
	if root != "" && reference != "" {
		python := filepath.Join(root, "venv", "bin", "python")
		if target.OS == "windows" {
			python = filepath.Join(root, "venv", "Scripts", "python.exe")
		}
		pairs = append(pairs, "{env."+reference+".python}", python, "{env."+reference+".root}", root)
	}
	return strings.NewReplacer(pairs...)
}
func expandArgs(args []string, r *strings.Replacer) []string {
	out := slices.Clone(args)
	for i := range out {
		out[i] = r.Replace(out[i])
	}
	return out
}
func expandExecution(s *Spec, work, root string, target ExecutionTarget) {
	r := executionReplacer(work, root, s.Environment.Name, target)
	s.Run.Command = expandArgs(s.Run.Command, r)
	s.Run.Workdir = r.Replace(s.Run.Workdir)
	for k, v := range s.Run.Env {
		s.Run.Env[k] = r.Replace(v)
	}
}
func bindEnvironment(env map[string]string, root string) {
	env["PATH"] = filepath.Join(root, "venv", "bin") + string(os.PathListSeparator) + filepath.Join(root, "bin") + string(os.PathListSeparator) + env["PATH"]
	env["VIRTUAL_ENV"] = filepath.Join(root, "venv")
	env["MYCE_ENVIRONMENT_DIR"] = root
}
