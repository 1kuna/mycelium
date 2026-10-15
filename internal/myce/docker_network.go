package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

type DockerReadiness struct {
	Port    int    `json:"port" yaml:"port"`
	Path    string `json:"path" yaml:"path"`
	Timeout string `json:"timeout" yaml:"timeout"`
}
type DockerSidecar struct {
	Name       string            `json:"name" yaml:"name"`
	Command    []string          `json:"command" yaml:"command"`
	Env        map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	MemorySwap *float64          `json:"memory_swap,omitempty" yaml:"memory_swap,omitempty"`
	MemoryGB   float64           `json:"memory_gb" yaml:"memory_gb"`
	Readiness  *DockerReadiness  `json:"readiness,omitempty" yaml:"readiness,omitempty"`
}

func (s Spec) validateDockerGroup() error {
	r := s.Run
	if r.DockerNetwork != "" && r.DockerNetwork != "private" {
		return errors.New("run.docker_network must be private (internal bridge; no outbound access)")
	}
	if (r.DockerNetwork != "" || len(r.DockerSidecars) > 0 || r.DockerMemoryGB != 0 || r.DockerMemorySwap != nil) && !r.Docker {
		return errors.New("Docker network/sidecars/memory require run.docker")
	}
	if !validMemoryValue(r.DockerMemoryGB) {
		return errors.New("run.docker_memory_gb must be finite and nonnegative")
	}
	mainMemory := r.DockerMemoryGB
	if mainMemory == 0 {
		mainMemory = s.Needs.HostMemoryGB
	}
	if !validDockerSwap(r.DockerMemorySwap, mainMemory) {
		return errors.New("docker_memory_swap is total RAM plus swap GiB and must be at least the main memory limit")
	}
	if len(r.DockerSidecars) == 0 && r.DockerMemoryGB == 0 {
		return nil
	}
	if r.DockerNetwork != "private" || r.DockerMemoryGB <= 0 {
		return errors.New("per-container memory requires docker_network: private and positive docker_memory_gb")
	}
	if r.DockerMemoryGB > s.Needs.HostMemoryGB {
		return errors.New("main Docker memory must fit needs.host_memory_gb")
	}
	names := map[string]bool{"main": true}
	for _, c := range r.DockerSidecars {
		if c.Name == "" || len(c.Name) > 63 || names[c.Name] || strings.Trim(c.Name, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" || c.Name[0] == '-' || c.Name[len(c.Name)-1] == '-' {
			return errors.New("sidecar name must be a unique lowercase DNS name (main is reserved)")
		}
		names[c.Name] = true
		if !validMemoryValue(c.MemoryGB) || c.MemoryGB <= 0 {
			return errors.New("sidecar memory_gb must be positive and finite")
		}
		if c.MemoryGB > s.Needs.HostMemoryGB {
			return errors.New("each Docker container memory must fit needs.host_memory_gb")
		}
		if !validDockerSwap(c.MemorySwap, c.MemoryGB) {
			return errors.New("sidecar memory_swap is total RAM plus swap GiB and must be at least memory_gb")
		}
		if _, err := dockerRunArgs(c.Command); err != nil {
			return fmt.Errorf("sidecar %s: %w", c.Name, err)
		}
		for name := range c.Env {
			if strings.HasPrefix(name, "MYCE_") || strings.HasPrefix(name, "DOCKER_") || dockerDeviceEnvironment(name) {
				return fmt.Errorf("sidecar %s env overrides worker-owned %s", c.Name, name)
			}
		}
		if c.Readiness != nil {
			t, err := time.ParseDuration(c.Readiness.Timeout)
			if err != nil || t <= 0 || c.Readiness.Port < 1 || c.Readiness.Port > 65535 || !strings.HasPrefix(c.Readiness.Path, "/") || strings.ContainsAny(c.Readiness.Path, "\r\n") {
				return errors.New("sidecar readiness requires port, absolute HTTP path and positive timeout")
			}
		}
	}
	return nil
}

type dockerMember struct {
	Name           string `json:"name"`
	Memory         int64  `json:"memory"`
	Swap           int64  `json:"swap"`
	Started        bool   `json:"started"`
	TermDispatched bool   `json:"term_dispatched,omitempty"`
	TermOutcome    string `json:"term_outcome,omitempty"`
	ExitCode       *int   `json:"exit_code,omitempty"`
	StderrTail     string `json:"stderr_tail,omitempty"`
}

type dockerNetworkInspection struct {
	ID         string `json:"Id"`
	Name       string
	Driver     string
	Internal   bool
	Labels     map[string]string
	Containers map[string]json.RawMessage
}

func (d *dockerAttempt) networks() ([]dockerNetworkInspection, error) {
	out, e := d.call("network", "ls", "--quiet", "--no-trunc", "--filter", dockerAttemptFilter(d.state.Identity))
	if e != nil {
		return nil, e
	}
	ids := strings.Fields(string(out))
	if d.state.NetworkID != "" && !d.state.NetworkRemoved && !slices.Contains(ids, d.state.NetworkID) {
		ids = append(ids, d.state.NetworkID)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	out, e = d.call(append([]string{"network", "inspect"}, ids...)...)
	if e != nil {
		var exit *exec.ExitError
		if len(ids) == 1 && ids[0] == d.state.NetworkID && errors.As(e, &exit) && (strings.HasPrefix(string(exit.Stderr), "Error: No such network:") || strings.HasPrefix(string(exit.Stderr), "Error response from daemon: network ") && strings.Contains(string(exit.Stderr), " not found")) {
			return nil, nil
		}
		return nil, e
	}
	var nets []dockerNetworkInspection
	if e = json.Unmarshal(out, &nets); e != nil {
		return nil, e
	}
	if len(nets) != len(ids) {
		return nil, errors.New("incomplete Docker network inventory")
	}
	seen := map[string]bool{}
	for _, n := range nets {
		if n.ID == "" || !slices.Contains(ids, n.ID) || seen[n.ID] || n.Labels["myce.attempt"] == "" || (d.state.Identity != "" && n.Labels["myce.attempt"] != d.state.Identity) {
			return nil, errors.New("Docker network ownership mismatch")
		}
		seen[n.ID] = true
	}
	return nets, nil
}
func (d *dockerAttempt) removeNetworks(containers []dockerInspection) error {
	nets, e := d.networks()
	if e != nil {
		return fmt.Errorf("Docker network cleanup unconfirmed: %w", e)
	}
	for _, n := range nets {
		for id := range n.Containers {
			found := false
			for _, c := range containers {
				if c.ID == id && !*c.State.Running {
					found = true
				}
			}
			if !found {
				return errors.New("Docker network has an unconfirmed or unowned endpoint; cleanup retained")
			}
		}
		// Retain stopped containers as evidence while releasing their network endpoints.
		for id := range n.Containers {
			if _, e = d.call("network", "disconnect", "--force", n.ID, id); e != nil {
				return e
			}
		}
		if _, e = d.call("network", "rm", n.ID); e != nil {
			return fmt.Errorf("Docker network removal unconfirmed: %w", e)
		}
		if n.ID == d.state.NetworkID {
			d.mu.Lock()
			d.state.NetworkRemoved = true
			e = d.saveLocked()
			d.mu.Unlock()
			if e != nil {
				return e
			}
		}
	}
	nets, e = d.networks()
	if e != nil {
		return e
	}
	if len(nets) > 0 {
		return errors.New("Docker labeled network still present")
	}
	return nil
}
func (d *dockerAttempt) containerLimit(id string) (int64, int64) {
	if m, ok := d.state.Containers[id]; ok {
		return m.Memory, m.Swap
	}
	return d.state.MemoryLimitBytes, d.state.MemorySwapBytes
}
func (d *dockerAttempt) containerViolation(c dockerInspection) string {
	if len(c.HostConfig.PortBindings) > 0 || c.HostConfig.PublishAllPorts {
		return "Docker containment violation: publishing host ports is forbidden"
	}
	for _, m := range c.Mounts {
		if strings.Contains(m.Source, "docker.sock") || strings.Contains(m.Destination, "docker.sock") {
			return "Docker containment violation: Docker socket mount is forbidden"
		}
	}
	expected := "none"
	if d.state.Network != "" {
		expected = d.state.Network
	}
	if c.HostConfig.NetworkMode != expected {
		return "Docker containment violation: host networking or joining another network is forbidden"
	}
	for network := range c.NetworkSettings.Networks {
		if network != expected {
			return "Docker containment violation: joining another network is forbidden"
		}
	}
	// Cleanup deliberately disconnects stopped containers before removing the network.
	if d.state.Network != "" && len(c.NetworkSettings.Networks) != 1 && !(d.state.Stopped && !*c.State.Running) {
		return "Docker containment violation: private network attachment missing"
	}
	return ""
}
func (d *dockerAttempt) observeGroup(containers []dockerInspection) error {
	networkFailure := ""
	d.mu.Lock()
	stopped := d.state.Stopped
	d.mu.Unlock()
	if d.state.Network != "" && !stopped {
		nets, e := d.networks()
		if e != nil {
			return e
		}
		if len(nets) != 1 || nets[0].Name != d.state.Network || nets[0].Driver != "bridge" || !nets[0].Internal {
			networkFailure = "Docker containment violation: private network must be an internal bridge"
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if networkFailure != "" {
		d.state.Failure = networkFailure
		d.state.Stopped = true
		d.state.Forced = true
	}
	for _, c := range containers {
		d.state.OOMKilled = d.state.OOMKilled || c.State.OOMKilled
		if why := d.containerViolation(c); why != "" {
			d.state.Failure = why
			d.state.Stopped = true
			d.state.Forced = true
		}
	}
	if !d.state.Stopped && d.state.Network != "" {
		for _, c := range containers {
			if c.ID == d.state.Container && !*c.State.Running {
				d.state.MainFinished = true
				d.state.ExitCode = c.State.ExitCode
				d.state.Stopped = true
				deadline := d.clock.Now().Add(StopGrace)
				d.state.StopAt = &deadline
				break
			}
		}
		if !d.state.MainFinished {
			for id, m := range d.state.Containers {
				if m.Name == "main" || !m.Started {
					continue
				}
				alive := false
				for _, c := range containers {
					if c.ID == id && *c.State.Running {
						alive = true
					}
				}
				if !alive {
					d.state.Failure = "Docker sidecar " + m.Name + " exited before main"
					d.state.Stopped = true
					d.state.Forced = true
					break
				}
			}
		}
	}
	return d.saveLocked()
}
func (d *dockerAttempt) launchContainer(base, args []string, name string, memory float64, memorySwap *float64, env map[string]string) (string, error) {
	create := append([]string(nil), base...)
	limit, swap := d.state.MemoryLimitBytes, d.state.MemorySwapBytes
	if memory > 0 {
		limit = int64(math.Ceil(memory * (1 << 30)))
		swap = 0
	}
	if memorySwap != nil {
		swap = int64(math.Ceil(*memorySwap * (1 << 30)))
	}
	for i := 1; i < len(create)-1; i++ {
		switch create[i] {
		case "--name":
			create[i+1] = "myce-" + d.state.Identity + "-" + name
		case "--cidfile":
			create[i+1] = filepath.Join(d.dir, d.state.Identity+"-"+name+".cid")
		case "--network-alias":
			create[i+1] = name
		case "--memory":
			create[i+1] = strconv.FormatInt(limit, 10)
		case "--memory-swap":
			create[i+1] = strconv.FormatInt(swap, 10)
		case "--env":
			if strings.HasPrefix(create[i+1], "MYCE_MEMORY_LIMIT_BYTES=") {
				create[i+1] = "MYCE_MEMORY_LIMIT_BYTES=" + strconv.FormatInt(limit, 10)
			}
		}
	}
	if limit > 0 && !slices.Contains(create, "--memory") {
		create = append(create, "--memory", strconv.FormatInt(limit, 10), "--env", "MYCE_MEMORY_LIMIT_BYTES="+strconv.FormatInt(limit, 10))
	}
	if swap > 0 && !slices.Contains(create, "--memory-swap") {
		create = append(create, "--memory-swap", strconv.FormatInt(swap, 10))
	}
	for k, v := range env {
		create = append(create, "--env", k+"="+v)
	}
	create = append(create, args...)
	if d.state.Network != "" {
		nets, e := d.networks()
		if e != nil {
			return "", e
		}
		if len(nets) != 1 || nets[0].Name != d.state.Network || nets[0].Driver != "bridge" || !nets[0].Internal {
			return "", errors.New("Docker containment violation: private network must be an internal bridge")
		}
	}
	out, e := d.operation("create", create...)
	if e != nil {
		return "", e
	}
	id := strings.TrimSpace(string(out))
	if id == "" || strings.ContainsAny(id, " \n\t") {
		d.mu.Lock()
		d.state.Pending = true
		d.state.Error = "invalid Docker create receipt"
		_ = d.saveLocked()
		d.mu.Unlock()
		return "", errors.New("invalid Docker create receipt")
	}
	d.mu.Lock()
	if name == "main" {
		d.state.Container = id
		if d.state.Containers == nil {
			d.state.MemoryLimitBytes = limit
			d.state.MemorySwapBytes = swap
		}
	}
	if d.state.Containers != nil {
		d.state.Containers[id] = dockerMember{Name: name, Memory: limit, Swap: swap}
	}
	d.state.PendingContainer = id
	e = d.saveLocked()
	d.mu.Unlock()
	if e != nil {
		return "", e
	}
	objects, e := d.containers()
	if e != nil {
		return "", e
	}
	found := false
	for _, c := range objects {
		if c.ID != id {
			continue
		}
		found = true
		if why := d.containerViolation(c); why != "" {
			return "", errors.New(why)
		}
		if limit > 0 && (c.HostConfig.Memory != limit || (swap > 0 && c.HostConfig.MemorySwap != swap)) {
			return "", errors.New("Docker memory ceiling not enforced")
		}
	}
	if !found {
		return "", errors.New("created Docker container missing from inspection")
	}
	_, e = d.operation("start", "start", id)
	if e != nil {
		return "", e
	}
	d.mu.Lock()
	if m, ok := d.state.Containers[id]; ok {
		m.Started = true
		d.state.Containers[id] = m
	}
	e = d.saveLocked()
	d.mu.Unlock()
	return id, e
}
func (d *dockerAttempt) waitReady(id, name string, r DockerReadiness) error {
	duration, _ := time.ParseDuration(r.Timeout)
	deadline := d.clock.Now().Add(duration)
	wake := time.NewTicker(100 * time.Millisecond)
	defer wake.Stop()
	client := &http.Client{Timeout: d.timeout, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	for {
		objects, e := d.containers()
		if e != nil {
			return e
		}
		if e = d.observeGroup(objects); e != nil {
			return e
		}
		d.mu.Lock()
		stopped := d.state.Stopped
		failure := d.state.Failure
		d.mu.Unlock()
		if stopped {
			return fmt.Errorf("Docker sidecar %s readiness stopped: %s", name, failure)
		}
		if !d.clock.Now().Before(deadline) {
			return fmt.Errorf("Docker sidecar %s HTTP readiness timeout", name)
		}
		for _, c := range objects {
			if c.ID != id || !*c.State.Running {
				continue
			}
			ip := c.NetworkSettings.Networks[d.state.Network].IPAddress
			if net.ParseIP(ip) == nil {
				continue
			}
			resp, err := client.Get("http://" + net.JoinHostPort(ip, strconv.Itoa(r.Port)) + r.Path)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 && d.clock.Now().Before(deadline) {
					return nil
				}
			}
		}
		if !d.clock.Now().Before(deadline) {
			return fmt.Errorf("Docker sidecar %s HTTP readiness timeout", name)
		}
		select {
		case <-wake.C:
		case <-d.halt:
			return errors.New("Docker readiness interrupted")
		}
	}
}

func validDockerSwap(value *float64, memory float64) bool {
	if value == nil {
		return true
	}
	_, e := memoryBytes(*value)
	return e == nil && *value >= memory && memory > 0
}
