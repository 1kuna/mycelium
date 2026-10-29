package myce

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"time"
)

// RunSpec is the existing worker wire contract. Composition lowers into it;
// there is no separate extension executor.
type RunSpec struct {
	MemoryGB         float64           `json:"memory_gb,omitempty" yaml:"memory_gb,omitempty"`
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

type GPUSelection struct {
	Devices []string `json:"devices,omitempty" yaml:"devices,omitempty"`
	Count   *int     `json:"count,omitempty" yaml:"count,omitempty"`
}

type ResourceNeeds struct {
	Network      bool          `json:"network,omitempty" yaml:"network,omitempty"`
	Memory       string        `json:"memory,omitempty" yaml:"memory,omitempty"`
	GPU          *GPUSelection `json:"gpu,omitempty" yaml:"gpu,omitempty"`
	CPUCores     int           `json:"cpu_cores,omitempty" yaml:"cpu_cores,omitempty"`
	MemoryGB     float64       `json:"memory_gb" yaml:"memory_gb"`
	HostMemoryGB float64       `json:"host_memory_gb,omitempty" yaml:"host_memory_gb,omitempty"`
	Platform     Platform      `json:"platform" yaml:"platform"`
	Model        string        `json:"model,omitempty" yaml:"model,omitempty"`
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

// NativeSidecar is an attempt-owned native helper. MemoryGB is enforced by
// delegated Linux child cgroups; other hosts report admission-only accounting.
// Port nil is a plain helper; zero requests an allocated loopback port.
type NativeSidecar struct {
	Name        string            `json:"name" yaml:"name"`
	Command     []string          `json:"command" yaml:"command"`
	Environment string            `json:"environment,omitempty" yaml:"environment,omitempty"`
	Env         map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	MemoryGB    float64           `json:"memory_gb,omitempty" yaml:"memory_gb,omitempty"`
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
		normalized := strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(s.Name))
		if s.Name == "main" || !extensionName.MatchString(s.Name) || names[normalized] || len(s.Command) == 0 || s.Command[0] == "" || !validMemoryValue(s.MemoryGB) {
			return errors.New("sidecars require unique names, commands and finite nonnegative RAM caps")
		}
		names[normalized] = true
		if s.Port != nil && (*s.Port < 0 || *s.Port > 65535) {
			return errors.New("invalid sidecar port")
		}
		if s.Readiness != nil {
			path, err := url.ParseRequestURI(s.Readiness.Path)
			if err != nil || path.Host != "" || path.Scheme != "" || s.Port == nil || !strings.HasPrefix(s.Readiness.Path, "/") || strings.HasPrefix(s.Readiness.Path, "//") || !positiveDuration(s.Readiness.Timeout) {
				return errors.New("readiness requires a local port, absolute path and positive timeout")
			}
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
		if len(s.Run.Sidecars) > 0 || s.Run.MemoryGB != 0 {
			if s.Run.Docker {
				return errors.New("native sidecars cannot use Docker")
			}
			if err := (ExecutionSpec{Main: ExecutionMember{Command: s.Run.Command, Env: s.Run.Env, MemoryGB: s.Run.MemoryGB}, Sidecars: s.Run.Sidecars}).validate(); err != nil {
				return err
			}
			return validateNativeMemory(s)
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
	if err := validateNativeMemory(s); err != nil {
		return err
	}
	if e.Main.Environment != "" {
		if s.Environment.Key() != "" {
			return errors.New("choose environment or execution.main.environment")
		}
		s.Environment = Environment{Name: e.Main.Environment}
	}
	s.Run = RunSpec{MemoryGB: e.Main.MemoryGB, Command: slices.Clone(e.Main.Command), Workdir: e.Main.Workdir, Env: maps.Clone(e.Main.Env), Sidecars: slices.Clone(e.Sidecars)}
	for i := range s.Run.Sidecars {
		s.Run.Sidecars[i].Env = maps.Clone(s.Run.Sidecars[i].Env)
	}
	s.Execution = nil
	return nil
}

func positiveDuration(s string) bool { d, e := time.ParseDuration(s); return e == nil && d > 0 }

// The finite substitutions are literal argv/env replacement, never shell text.
func executionReplacer(work, root, reference string, target ExecutionTarget) *strings.Replacer {
	pairs := []string{"{work}", work}
	if root != "" && reference != "" {
		python := environmentPython(root, target)
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
	wr := strings.NewReplacer("{work}", work)
	for i := range s.Run.Sidecars {
		side := &s.Run.Sidecars[i]
		side.Command = expandArgs(side.Command, wr)
		side.Env = maps.Clone(side.Env)
		for k, v := range side.Env {
			side.Env[k] = wr.Replace(v)
		}
	}
}
func bindEnvironment(env map[string]string, root string) {
	clearEnvironmentLibraries(env, root)
	// Switching a member's environment must not inherit main's conda activation.
	if previous := env["MYCE_ENVIRONMENT_DIR"]; previous != "" && previous != root {
		for k, v := range environmentActivation(previous) {
			if !reservedActivation(k) && env[k] == v {
				delete(env, k)
			}
		}
		if env["PATH"] == "" {
			env["PATH"] = os.Getenv("PATH")
		}
	}
	env["PATH"] = filepath.Join(root, "venv", "bin") + string(os.PathListSeparator) + filepath.Join(root, "bin") + string(os.PathListSeparator) + env["PATH"]
	env["VIRTUAL_ENV"] = filepath.Join(root, "venv")
	if activation := environmentActivation(root); activation != nil {
		delete(env, "VIRTUAL_ENV")
		for k, v := range activation {
			if !reservedActivation(k) {
				env[k] = v
			}
		}
	}
	prependEnvironmentLibraries(env, root)
	env["MYCE_ENVIRONMENT_DIR"] = root
}

// Dynamic-loader paths are independent of the accelerator and its selectors.
// Keep host paths as a fallback for drivers absent from the locked environment.
func environmentLibraryVariables() []string {
	if runtime.GOOS == "darwin" {
		return []string{"DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH"}
	}
	if runtime.GOOS == "linux" {
		return []string{"LD_LIBRARY_PATH"}
	}
	return nil
}

func environmentLibraryDirectories(root string) []string {
	prefix := filepath.Join(root, "venv")
	if activation := environmentActivation(root); activation["CONDA_PREFIX"] != "" {
		prefix = activation["CONDA_PREFIX"]
	}
	return []string{filepath.Join(prefix, "lib"), filepath.Join(prefix, "lib64"), filepath.Join(root, "lib"), filepath.Join(root, "lib64")}
}

func clearEnvironmentLibraries(env map[string]string, root string) {
	previous := env["MYCE_ENVIRONMENT_DIR"]
	if previous == "" {
		return
	}
	dirs := environmentLibraryDirectories(previous)
	for _, key := range environmentLibraryVariables() {
		paths := []string{}
		for _, path := range filepath.SplitList(env[key]) {
			// Rebinding is idempotent; switching must also drop package-specific
			// directories owned by the previous member's environment.
			rel, err := filepath.Rel(previous, path)
			owned := err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
			if slices.Contains(dirs, path) || previous != root && owned {
				continue
			}
			paths = append(paths, path)
		}
		if len(paths) == 0 {
			delete(env, key)
		} else {
			env[key] = strings.Join(paths, string(os.PathListSeparator))
		}
	}
}

func prependEnvironmentLibraries(env map[string]string, root string) {
	dirs := strings.Join(environmentLibraryDirectories(root), string(os.PathListSeparator))
	for _, key := range environmentLibraryVariables() {
		// An absent Darwin fallback variable keeps dyld's system default.
		if key == "DYLD_FALLBACK_LIBRARY_PATH" && env[key] == "" {
			continue
		}
		if env[key] == "" {
			env[key] = dirs
		} else {
			env[key] = dirs + string(os.PathListSeparator) + env[key]
		}
	}
}

// macOS strips DYLD_* when the protected shell supervisor starts. Restore only
// a bound environment's loader paths after that shell, using literal argv.
func environmentCommand(argv, environ []string) []string {
	if runtime.GOOS != "darwin" {
		return argv
	}
	values := map[string]string{}
	for _, entry := range environ {
		key, value, _ := strings.Cut(entry, "=")
		values[key] = value
	}
	if values["MYCE_ENVIRONMENT_DIR"] == "" {
		return argv
	}
	wrapped := []string{"/usr/bin/env"}
	for _, key := range environmentLibraryVariables() {
		if value, ok := values[key]; ok {
			wrapped = append(wrapped, key+"="+value)
		}
	}
	return append(wrapped, argv...)
}

// Member ceilings fit the whole simultaneous host envelope; they are not extra
// reservations or a sum of independent peaks.
func validateNativeMemory(s *Spec) error {
	main := s.Run.MemoryGB
	members := s.Run.Sidecars
	if s.Execution != nil {
		members = s.Execution.Sidecars
		main = s.Execution.Main.MemoryGB
	}
	deferredEnvelope := s.Needs.Memory == "auto" && s.Needs.HostMemoryGB == 0
	if !validMemoryValue(main) || !deferredEnvelope && main > s.Needs.HostMemoryGB {
		return errors.New("invalid main RAM cap or cap exceeds whole-job host memory reservation")
	}
	for _, m := range members {
		if !deferredEnvelope && m.MemoryGB > s.Needs.HostMemoryGB {
			return errors.New("sidecar RAM exceeds whole-job host memory reservation")
		}
	}
	return nil
}
