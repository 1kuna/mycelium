package myce

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// A small allow-list keeps daemon selection and container lifecycle worker-owned.
func dockerRunArgs(command []string) ([]string, error) {
	if len(command) < 3 || command[0] != "docker" {
		return nil, errors.New("run.docker requires direct docker run argv")
	}
	i := 1
	if command[i] == "container" {
		i++
	}
	if i >= len(command) || command[i] != "run" {
		return nil, errors.New("run.docker requires docker run or docker container run")
	}
	i++
	for i < len(command) && strings.HasPrefix(command[i], "-") {
		flag, value, equals := strings.Cut(command[i], "=")
		switch flag {
		case "--init":
			if equals {
				return nil, errors.New("Docker --init takes no value")
			}
			i++
			continue
		case "--env", "-e", "--volume", "-v", "--device", "--gpus", "--entrypoint", "--workdir", "-w", "--user", "-u":
			if !equals {
				i++
				if i >= len(command) {
					return nil, fmt.Errorf("Docker %s requires a value", flag)
				}
				value = command[i]
			}
			if value == "" || strings.Contains(value, "docker.sock") {
				return nil, fmt.Errorf("unsupported Docker %s value", flag)
			}
			if flag == "--volume" || flag == "-v" {
				parts := strings.Split(value, ":")
				if len(parts) < 2 || len(parts) > 3 || !filepath.IsAbs(parts[0]) || !filepath.IsAbs(parts[1]) {
					return nil, errors.New("managed Docker volumes require absolute bind paths")
				}
				source, target := filepath.Clean(parts[0]), filepath.Clean(parts[1])
				if slices.Contains([]string{"/", "/var", "/var/run", "/run"}, source) || target == "/" || target == "/myce" || target == "/myce/checkpoint" {
					return nil, errors.New("Docker bind may not expose the daemon or override checkpoint custody")
				}
			}
			if flag == "--env" || flag == "-e" {
				name, _, _ := strings.Cut(value, "=")
				if strings.HasPrefix(name, "MYCE_") || dockerDeviceEnvironment(name) || !strings.Contains(value, "=") {
					return nil, errors.New("Docker env must use explicit values and cannot override worker identity, credentials or device selectors")
				}
			}
		default:
			return nil, fmt.Errorf("unsupported managed Docker flag %s (labels, name, cidfile, daemon and lifecycle are worker-owned)", flag)
		}
		i++
	}
	if i >= len(command) || command[i] == "" {
		return nil, errors.New("Docker image is required")
	}
	image := command[i]
	digest := strings.Split(image, "@sha256:")
	if len(digest) != 2 || len(digest[1]) != 64 {
		return nil, errors.New("managed Docker image must be pinned by sha256 digest")
	}
	if _, err := hex.DecodeString(digest[1]); err != nil {
		return nil, errors.New("invalid Docker image digest")
	}
	start := 2
	if command[1] == "container" {
		start = 3
	}
	return command[start:], nil
}

// dockerExecution is durable custody, separate from the worker's heartbeat state.
type dockerExecution struct {
	CleanupCause *RuntimeCause `json:"cleanup_cause,omitempty"`

	LogsNone           bool                    `json:"logs_none,omitempty"`
	Owner              *Result                 `json:"owner,omitempty"` // Saved job/attempt/machine/session binding; legacy custody uses opaque identity.
	EvidenceSaved      bool                    `json:"evidence_saved,omitempty"`
	NetworkID          string                  `json:"network_id,omitempty"`
	NetworkRemoved     bool                    `json:"network_removed,omitempty"`
	Network            string                  `json:"network,omitempty"`
	Containers         map[string]dockerMember `json:"containers,omitempty"`
	PendingContainer   string                  `json:"pending_container,omitempty"`
	PendingName        string                  `json:"pending_name,omitempty"`
	MainFinished       bool                    `json:"main_finished,omitempty"`
	InfrastructureStop bool                    `json:"infrastructure_stop,omitempty"`
	AmbiguousKill      bool                    `json:"ambiguous_kill,omitempty"`
	Failure            string                  `json:"failure,omitempty"`
	MemoryLimitBytes   int64                   `json:"memory_limit_bytes,omitempty"`
	MemorySwapBytes    int64                   `json:"memory_swap_bytes,omitempty"`
	MemoryFailure      string                  `json:"memory_failure,omitempty"`
	OOMKilled          bool                    `json:"oom_killed,omitempty"`
	Identity           string                  `json:"identity"`
	Endpoint           string                  `json:"endpoint"`
	Container          string                  `json:"container,omitempty"`
	DispatchedAt       *time.Time              `json:"dispatched_at,omitempty"`
	Operation          string                  `json:"operation,omitempty"`
	Pending            bool                    `json:"pending,omitempty"`
	Stopped            bool                    `json:"stopped,omitempty"`
	Forced             bool                    `json:"forced,omitempty"`
	StopAt             *time.Time              `json:"stop_at,omitempty"`
	OperatorConfirmed  bool                    `json:"operator_confirmed,omitempty"`
	Released           bool                    `json:"released,omitempty"`
	Error              string                  `json:"error,omitempty"`
	ExitCode           int                     `json:"exit_code"`
}
type dockerAttempt struct {
	command     func(context.Context, ...string) ([]byte, error) // Injectable daemon boundary.
	persist     func(dockerExecution) error                      // Optional custody store for fault injection.
	admit       func() error
	dirty       bool
	closed      chan struct{}
	halt        chan struct{}
	haltOnce    sync.Once
	mu          sync.Mutex
	state       dockerExecution
	dir, binary string

	ctx       context.Context
	inventory func([]string)
	workdir   string
	env       []string
	clock     Clock
	timeout   time.Duration
}

func (d *dockerAttempt) saveLocked() error {
	err := runtimeErrorFor("persist", d.saveState(d.state))
	d.dirty = err != nil
	return err
}

func (d *dockerAttempt) saveState(state dockerExecution) error {
	if d.dir == "" {
		return nil
	}
	var err error
	if d.persist != nil {
		err = d.persist(state)
	} else {
		err = saveWorkerJSON(d.dir, state.Identity+".docker.json", state)
	}
	return err
}
func (d *dockerAttempt) status() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state.Released {
		return "released"
	}
	if d.state.Error != "" {
		return "cleanup unconfirmed: " + d.state.Operation + "; " + d.state.Error
	}
	if d.state.Pending {
		return d.state.Operation + " pending"
	}
	if d.state.Stopped {
		return "stopping"
	}
	if d.state.Operation == "start" {
		return "running"
	}
	return "creating"
}
func (d *dockerAttempt) stop(now time.Time, grace time.Duration, kill bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.state.Stopped {
		d.state.InfrastructureStop = true
	}
	d.stopLocked(now, grace, kill)
}

// Cleanup after a launch error is not itself evidence of infrastructure loss.
func (d *dockerAttempt) stopLocked(now time.Time, grace time.Duration, kill bool) {
	changed := !d.state.Stopped
	d.state.Stopped = true
	deadline := now.Add(grace)
	if d.state.StopAt == nil || deadline.Before(*d.state.StopAt) {
		d.state.StopAt = &deadline
		changed = true
	}
	if kill && !d.state.Forced {
		d.state.Forced = true
		changed = true
	}
	if !changed {
		return
	}
	if err := d.saveLocked(); err != nil {
		d.state.Error = "persist Docker stop intent failed"
	}
}
func (d *dockerAttempt) call(args ...string) ([]byte, error) {
	return d.callWithTimeout(d.timeout, args...)
}
func (d *dockerAttempt) callWithTimeout(timeout time.Duration, args ...string) (out []byte, err error) {
	operation := "reconcile"
	if len(args) > 0 {
		operation = args[0]
		if operation == "network" && len(args) > 1 {
			operation += "." + args[1]
		}
	}
	defer func() { err = runtimeErrorFor(operation, err) }()
	parent := d.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if d.command != nil {
		return d.command(ctx, args...)
	}
	cmd := exec.CommandContext(ctx, d.binary, append([]string{"--host", d.state.Endpoint}, args...)...)
	cmd.Env = d.env
	cmd.Dir = d.workdir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = timeout
	out, err = cmd.Output()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return out, err
}
func (d *dockerAttempt) operation(kind string, args ...string) ([]byte, error) {
	if d.admit != nil {
		if err := d.admit(); err != nil {
			d.stop(d.clock.Now(), 0, true)
			return nil, err
		}
	}
	d.mu.Lock()
	if d.state.Stopped {
		d.mu.Unlock()
		return nil, errors.New("Docker stop recorded before dispatch")
	}
	d.state.Operation = kind
	d.state.Pending = true
	if kind == "create" {
		d.state.PendingName = ""
		d.state.PendingContainer = ""
		for i, arg := range args {
			if arg == "--name" && i+1 < len(args) {
				d.state.PendingName = args[i+1]
			}
		}
	}
	at := d.clock.Now().UTC()
	d.state.DispatchedAt = &at
	err := d.saveLocked()
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	out, err := d.call(args...)
	d.mu.Lock()
	defer d.mu.Unlock()
	if err == nil {
		d.state.Pending = false
		d.state.Error = ""
	} else if refusal := new(exec.ExitError); errors.As(err, &refusal) && strings.HasPrefix(strings.TrimSpace(string(refusal.Stderr)), "Error response from daemon:") {
		d.state.Pending = false
		d.state.Error = "Docker " + kind + " refused"
	} else {
		d.state.Error = "Docker " + kind + " disposition unknown"
	}
	if saveErr := d.saveLocked(); saveErr != nil {
		return nil, saveErr
	}
	return out, err
}

type dockerInspection struct {
	HostConfig struct {
		RestartPolicy   *dockerRestartPolicy
		Memory          int64
		MemorySwap      int64
		NetworkMode     string
		PortBindings    map[string]json.RawMessage
		PublishAllPorts bool
	} `json:"HostConfig"`
	NetworkSettings struct {
		Networks map[string]struct{ IPAddress string }
	} `json:"NetworkSettings"`
	Mounts []struct{ Source, Destination string } `json:"Mounts"`
	ID     string                                 `json:"Id"`
	Name   string                                 `json:"Name"`
	Config struct{ Labels map[string]string }     `json:"Config"`
	State  struct {
		Pid        int
		StartedAt  time.Time
		Paused     bool
		Restarting bool
		Running    *bool
		Status     string
		ExitCode   int
		OOMKilled  bool
	} `json:"State"`
}

type dockerRestartPolicy struct {
	Name              string
	MaximumRetryCount int
}

func dockerLive(c dockerInspection) bool {
	return c.State.Running != nil && *c.State.Running || c.State.Paused || c.State.Restarting || c.State.Status == "paused" || c.State.Status == "restarting" || c.State.Status == "removing"
}

// One inventory implementation is shared by privileged observation and cleanup.
func inspectOwnedDocker(ctx context.Context, endpoint, identity string, call func(context.Context, string, ...string) ([]byte, error)) ([]dockerInspection, error) {
	out, err := call(ctx, endpoint, "ps", "--all", "--quiet", "--no-trunc", "--filter", dockerAttemptFilter(identity))
	if err != nil {
		return nil, err
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return []dockerInspection{}, nil
	}
	out, inspectErr := call(ctx, endpoint, append([]string{"inspect"}, ids...)...)
	var cs []dockerInspection
	if decodeErr := json.Unmarshal(out, &cs); decodeErr != nil {
		return nil, runtimeErrorFor("inspect", errors.Join(inspectErr, decodeErr))
	}
	var valid []dockerInspection
	incomplete := inspectErr != nil || len(cs) != len(ids)
	seen := map[string]bool{}
	for _, c := range cs {
		if c.ID == "" || !slices.Contains(ids, c.ID) || seen[c.ID] || c.Config.Labels["myce.attempt"] == "" || identity != "" && c.Config.Labels["myce.attempt"] != identity {
			incomplete = true
			continue
		}
		seen[c.ID] = true
		valid = append(valid, c)
		if c.State.Running == nil || !slices.Contains([]string{"created", "running", "paused", "restarting", "removing", "exited", "dead"}, c.State.Status) {
			incomplete = true
		}
	}
	if incomplete {
		return valid, runtimeErrorFor("inspect", errors.Join(inspectErr, errors.New("incomplete Docker inventory")))
	}
	cs = valid
	return cs, nil
}

// Every object is inspected before signaling: names and cidfiles are not ownership proof.
func (d *dockerAttempt) containers() ([]dockerInspection, error) {
	containers, err := inspectOwnedDocker(d.ctx, d.state.Endpoint, d.state.Identity, func(_ context.Context, _ string, args ...string) ([]byte, error) {
		b, err := d.call(args...)
		if len(args) > 0 && args[0] == "ps" && err == nil && d.inventory != nil {
			d.inventory(strings.Fields(string(b)))
		}
		return b, err
	})
	if err != nil && d.clock != nil {
		// Outcome observation was lost. Retain custody until inventory recovers,
		// then clean this attempt rather than interpreting recovery exits as work.
		d.stop(d.clock.Now(), 0, true)
	}
	return containers, err
}
func (d *dockerAttempt) reconcile() (done bool, err error) {
	defer func() { err = runtimeErrorFor("reconcile", err) }()
	if d.state.OperatorConfirmed {
		return false, errors.New("legacy Docker assertion has unknown operation disposition")
	}
	// Deadline KILL must remain possible when the spool is unwritable.
	// Preserve pending-start acceptance before signaling any container.
	d.mu.Lock()
	killDue := d.state.Forced || (d.state.Stopped && d.state.StopAt != nil && !d.clock.Now().Before(*d.state.StopAt))
	if d.dirty {
		if err := d.saveLocked(); err != nil && !killDue {
			d.mu.Unlock()
			return false, err
		}
	}
	d.mu.Unlock()
	containers, err := d.containers()
	if err != nil {
		return false, err
	}
	for _, c := range containers {
		if c.HostConfig.RestartPolicy == nil {
			return false, errors.New("Docker restart policy unknown")
		}
		if c.HostConfig.RestartPolicy.Name != "no" {
			if _, err := d.call("update", "--restart=no", c.ID); err != nil {
				return false, err
			}
			return false, nil // Verify effective policy in the next inventory.
		}
	}
	// A positively identified network settles its accepted creation; absence cannot.
	d.mu.Lock()
	networkPending := d.state.Pending && d.state.Operation == "network-create"
	d.mu.Unlock()
	if networkPending {
		ns, e := d.networks()
		if e != nil {
			return false, e
		}
		for _, n := range ns {
			if n.Name == d.state.Network && n.Internal && n.Driver == "bridge" {
				d.mu.Lock()
				d.state.Pending = false
				d.state.NetworkID = n.ID
				d.state.Error = ""
				e = d.saveLocked()
				d.mu.Unlock()
				if e != nil {
					return false, e
				}
				break
			}
		}
	}
	d.mu.Lock()
	stopped, forced, pending := d.state.Stopped, d.state.Forced, d.state.Pending
	// Created objects settle create acceptance; an empty scan never does.
	if pending && d.state.Operation == "create" {
		for _, c := range containers {
			if d.state.PendingName != "" && c.Name == "/"+d.state.PendingName {
				d.state.Pending, pending = false, false
				d.state.Error = ""
				if err := d.saveLocked(); err != nil {
					d.mu.Unlock()
					return false, err
				}
				break
			}
		}
	}
	// One worker-owned start request, no restart policy: running is positive acceptance evidence.
	if pending && d.state.Operation == "start" {
		for _, c := range containers {
			if (c.ID == d.state.PendingContainer || d.state.PendingContainer == "" && c.ID == d.state.Container) && (dockerLive(c) || d.state.DispatchedAt != nil && !c.State.StartedAt.IsZero() && !c.State.StartedAt.Before(*d.state.DispatchedAt)) {
				d.state.Pending = false
				d.state.Stopped = true
				d.state.InfrastructureStop = true
				d.state.Forced = true
				d.state.Error = ""
				if err := d.saveLocked(); err != nil && !killDue {
					d.mu.Unlock()
					return false, err
				}
				pending = false
				stopped = true
				forced = true
				break
			}
		}
	}
	if (pending || (stopped && d.state.StopAt != nil && !d.clock.Now().Before(*d.state.StopAt))) && !forced {
		d.state.Forced = true
		forced = true
		if err := d.saveLocked(); err != nil && !killDue {
			d.mu.Unlock()
			return false, err
		}
	}
	d.mu.Unlock()
	if err := d.observeGroup(containers); err != nil && !killDue {
		return false, err
	}
	d.mu.Lock()
	stopped, forced = d.state.Stopped, d.state.Forced
	d.mu.Unlock()
	running := false
	for _, c := range containers {
		limit, swap := d.containerLimit(c.ID)
		if limit > 0 && (c.HostConfig.Memory != limit || (swap > 0 && c.HostConfig.MemorySwap != swap)) {
			d.mu.Lock()
			d.state.MemoryFailure = "Docker memory ceiling not enforced"
			err := d.saveLocked()
			d.mu.Unlock()
			if err != nil && !killDue {
				return false, err
			}
			if dockerLive(c) {
				if _, err = d.signalContainer(c.ID, true); err != nil {
					return false, err
				}
				running = true
				continue
			}
		}
		if dockerLive(c) {
			if c.State.Paused || c.State.Status == "paused" {
				if _, err := d.call("unpause", c.ID); err != nil {
					return false, err
				}
			}
			running = true
			if stopped || pending {
				if _, err = d.signalContainer(c.ID, forced || pending); err != nil {
					return false, err
				}
			}
		}
	}
	if running || pending {
		return false, nil
	}
	if err := d.retainEvidence(containers); err != nil {
		return false, err
	}
	if err := d.removeNetworks(containers); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// A final successful inventory after a definite start response is the release proof.
	if len(containers) > 0 {
		if !d.state.MainFinished {
			for _, c := range containers {
				if c.ID == d.state.Container {
					d.state.ExitCode = c.State.ExitCode
				}
			}
		}
		for _, c := range containers {
			d.state.OOMKilled = d.state.OOMKilled || c.State.OOMKilled
		}
	}
	d.state.Released = true
	d.state.Error = ""
	if err := d.saveLocked(); err != nil {
		d.state.Released = false
		return false, err
	}
	return true, nil
}

// TERM intent is permanent once durable; every delivery error waits for the
// deadline. KILL is idempotent and is retried while inspection proves running.
func (d *dockerAttempt) signalContainer(id string, kill bool) ([]byte, error) {
	if d.dir == "" {
		return d.stopOrphan(id, kill)
	}
	if kill {
		return d.call("kill", "--signal=KILL", id)
	}
	d.mu.Lock()
	m, known := d.state.Containers[id]
	if m.TermDispatched {
		d.mu.Unlock()
		return nil, nil
	}
	if !known {
		m.Memory = d.state.MemoryLimitBytes
		m.Swap = d.state.MemorySwapBytes
	}
	if id == d.state.Container && m.Name == "" {
		m.Name = "main"
	}
	m.TermDispatched = true
	m.TermOutcome = "unknown-after-restart"
	// Save a candidate without changing live state: a failed write leaves
	// no intent for a later dirty flush to consume, and sends no TERM.
	next := d.state
	next.Containers = maps.Clone(d.state.Containers)
	if next.Containers == nil {
		next.Containers = map[string]dockerMember{}
	}
	next.Containers[id] = m
	if err := d.saveState(next); err != nil {
		d.mu.Unlock()
		return nil, err
	}
	d.state = next
	d.dirty = false
	d.mu.Unlock()
	out, err := d.call("kill", "--signal=TERM", id)
	d.mu.Lock()
	m = d.state.Containers[id]
	m.TermOutcome = "sent"
	if err != nil {
		m.TermOutcome = "error"
	}
	d.state.Containers[id] = m
	saveErr := d.saveLocked()
	d.mu.Unlock()
	if saveErr != nil {
		return out, saveErr
	}
	return out, err
}

const dockerStderrTailBytes = 64 * 1024

// Docker --tail bounds lines; this writer additionally bounds bytes, including
// one arbitrarily long line. stdout is discarded, never folded into stderr.
type dockerTail struct{ data []byte }

func (b *dockerTail) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) >= dockerStderrTailBytes {
		b.data = append(b.data[:0], p[len(p)-dockerStderrTailBytes:]...)
	} else {
		if extra := len(b.data) + len(p) - dockerStderrTailBytes; extra > 0 {
			b.data = append(b.data[:0], b.data[extra:]...)
		}
		b.data = append(b.data, p...)
	}
	return n, nil
}

// Evidence stays in the private spool, independent of disposable Docker custody.
// It is not included in Result, queue logs, status, or public diagnostics.
func (d *dockerAttempt) retainEvidence(containers []dockerInspection) (err error) {
	defer func() { err = runtimeErrorFor("retain_evidence", err) }()
	if d.dir == "" {
		// Orphans have no owning attempt/spool. The host cleanup report records
		// the evidence gap; an in-memory observation is not retained evidence.
		d.mu.Lock()
		d.state.EvidenceSaved = false
		d.mu.Unlock()
		return nil
	}
	for _, c := range containers {
		d.mu.Lock()
		m := d.state.Containers[c.ID]
		logsNone := d.state.LogsNone
		d.mu.Unlock()
		if m.ExitCode != nil {
			continue
		}
		tail := &dockerTail{}
		if !logsNone {
			ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
			cmd := exec.CommandContext(ctx, d.binary, "--host", d.state.Endpoint, "logs", "--tail", "256", c.ID)
			cmd.Env = d.env
			cmd.Dir = d.workdir
			cmd.Stdout = io.Discard
			cmd.Stderr = tail
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			cmd.Cancel = func() error { return signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL) }
			cmd.WaitDelay = d.timeout
			err := cmd.Run()
			cancel()
			if err != nil {
				return runtimeErrorFor("logs", err)
			}
		}
		d.mu.Lock()
		if d.state.Containers == nil {
			d.state.Containers = map[string]dockerMember{}
		}
		m = d.state.Containers[c.ID]
		if c.ID == d.state.Container && m.Name == "" {
			m.Name = "main"
		}
		code := c.State.ExitCode
		if m.TermDispatched && m.TermOutcome == "" {
			m.TermOutcome = "unknown-after-restart"
		}
		m.ExitCode = &code
		m.StderrTail = string(tail.data)
		d.state.Containers[c.ID] = m
		err := d.saveLocked()
		d.mu.Unlock()
		if err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := saveWorkerJSON(d.dir, d.state.Identity+".docker-evidence.json", d.state.Containers); err != nil {
		return err
	}
	d.state.EvidenceSaved = true
	return d.saveLocked()
}

func (w Worker) startDocker(s *workerStore, j Job, checkpoint string, release func()) (*process, error) {
	if w.DockerEndpoint == "" {
		return nil, errors.New("Docker runtime disabled; configure --docker-host to run image jobs")
	}
	if err := w.checkHostMemoryBudget(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB); err != nil {
		return nil, err
	}
	if j.Spec.Run.Workdir != "" {
		st, err := os.Stat(j.Spec.Run.Workdir)
		if err != nil {
			return nil, err
		}
		if !st.IsDir() {
			return nil, errors.New("Docker client workdir is not a directory")
		}
	}
	args, err := dockerRunArgs(j.Spec.Run.Command)
	if err != nil {
		return nil, err
	}
	if w.Machine.Device != "" {
		if _, err := WithModelDevice(Model{}, w.Machine.GPU, w.Machine.Device); err != nil {
			return nil, err
		}
	}
	binary, err := exec.LookPath("docker")
	if err != nil {
		return nil, err
	}
	ceilingBytes, err := w.ceilingBytes(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB)
	if err != nil {
		return nil, err
	}
	identity := ID()
	endpoint := w.DockerEndpoint
	if os.Getenv("DOCKER_CONTEXT") != "" {
		return nil, errors.New("managed Docker requires --docker-host, not DOCKER_CONTEXT")
	}
	d := &dockerAttempt{admit: func() error { return w.checkHostMemoryBudget(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) }, closed: make(chan struct{}), halt: make(chan struct{}), dir: s.dir, workdir: j.Spec.Run.Workdir, binary: binary, clock: w.Clock, timeout: w.dockerCallTimeout, state: dockerExecution{Identity: identity, Endpoint: endpoint, MemoryLimitBytes: ceilingBytes, MemorySwapBytes: 0}}
	d.state.Owner = &Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	d.state.LogsNone = j.Spec.Logs == "none"
	if j.Spec.Run.DockerNetwork == "private" {
		d.state.Network = "myce-" + identity
		d.state.Containers = map[string]dockerMember{}
	}
	if d.timeout <= 0 {
		d.timeout = 2 * time.Second
	}
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if k != "MYCE_TOKEN" && k != "DOCKER_CONTEXT" {
			d.env = append(d.env, v)
		}
	}
	for k, v := range j.Spec.Run.Env {
		if k != "MYCE_TOKEN" && !strings.HasPrefix(k, "DOCKER_") {
			d.env = append(d.env, k+"="+v)
		}
	}
	s.state.Job = &j
	s.state.Identity = identity
	s.state.Log = identity + ".log"
	if d.state.LogsNone {
		s.state.Log = ""
	}
	s.state.Result = nil
	s.state.OperatorStopped = false
	s.state.PID = 0
	s.state.Offset = 0
	s.state.Pending = 0
	output, err := openCommandOutput(s.dir, s.state.Log, j.Spec.Logs, os.O_EXCL)
	if err != nil {
		return nil, err
	}
	if err = d.saveLocked(); err == nil {
		err = s.save()
	}
	if err != nil {
		output.Close()
		return nil, err
	}
	p := &process{machine: w.Machine.ID, job: j, done: make(chan workerFinish, 1), docker: d}
	if w.control != nil {
		w.control.set(p)
	}
	label := "myce.attempt=" + identity
	create := []string{"create", "--restart=no", "--pull=never", "--init", "--name", "myce-" + identity, "--label", label, "--label", "myce.worker=" + w.Machine.ID, "--cidfile", filepath.Join(s.dir, identity+".cid"), "--env", "MYCE_ATTEMPT_ID=" + identity, "--env", "MYCE_DOCKER_LABEL=" + label, "--env", "MYCE_JOB_ID=" + j.ID, "--env", "MYCE_ATTEMPT=" + strconv.Itoa(j.Attempt), "--env", "MYCE_CHECKPOINT_DIR=/myce/checkpoint", "--volume", checkpoint + ":/myce/checkpoint"}
	if d.state.LogsNone {
		create = append(create, "--log-driver", "none")
	}
	if d.state.Network != "" {
		create = append(create, "--network", d.state.Network, "--network-alias", "main", "--env", "MYCE_DOCKER_NETWORK="+d.state.Network)
	} else {
		create = append(create, "--network", "none")
	}
	if ceilingBytes > 0 {
		create = append(create, "--memory", strconv.FormatInt(ceilingBytes, 10), "--env", "MYCE_MEMORY_LIMIT_BYTES="+strconv.FormatInt(ceilingBytes, 10))
	}
	for k, v := range placementEnvironment(w.Machine) {
		create = append(create, "--env", k+"="+v)
	}
	if w.Machine.Device != "" {
		bound, _ := WithModelDevice(Model{}, w.Machine.GPU, w.Machine.Device)
		if w.Machine.GPU == "cuda" && w.DeviceBinding != "" {
			bound.DeviceEnv["CUDA_VISIBLE_DEVICES"] = w.DeviceBinding
		}
		for k, v := range bound.DeviceEnv {
			create = append(create, "--env", k+"="+v)
		}
	}
	for k, v := range j.Spec.Run.Env {
		if !strings.HasPrefix(k, "MYCE_") && !strings.HasPrefix(k, "DOCKER_") {
			create = append(create, "--env", k+"="+v)
		}
	}
	create = append(create, args...)
	go d.run(p, j, s.state.Session, output, release, func() error {
		if e := w.authorizeStart(j, j.Session); e != nil {
			return e
		}
		if d.state.Network != "" {
			out, e := d.operation("network-create", "network", "create", "--driver", "bridge", "--internal", "--label", label, "--label", "myce.worker="+w.Machine.ID, d.state.Network)
			if e != nil {
				return e
			}
			id := strings.TrimSpace(string(out))
			d.mu.Lock()
			if id == "" || strings.ContainsAny(id, " \n\t") {
				d.state.Pending = true
				d.state.Error = "invalid Docker network create receipt"
				e = errors.New(d.state.Error)
			} else {
				d.state.NetworkID = id
			}
			saveErr := d.saveLocked()
			d.mu.Unlock()
			if saveErr != nil {
				return saveErr
			}
			if e != nil {
				return e
			}
		}
		for _, sidecar := range j.Spec.Run.DockerSidecars {
			sideargs, _ := dockerRunArgs(sidecar.Command)
			base := create[:len(create)-len(args)]
			id, e := d.launchContainer(base, sideargs, sidecar.Name, sidecar.MemoryGB, sidecar.MemorySwap, sidecar.Env)
			if e != nil {
				return e
			}
			if sidecar.Readiness != nil {
				if e = d.waitReady(id, sidecar.Name, *sidecar.Readiness); e != nil {
					return e
				}
			}
		}
		_, e := d.launchContainer(create[:len(create)-len(args)], args, "main", j.Spec.Run.DockerMemoryGB, j.Spec.Run.DockerMemorySwap, nil)
		return e
	})
	return p, nil
}
func (d *dockerAttempt) run(p *process, j Job, session string, output *os.File, release func(), launch func() error) {
	defer close(d.closed)
	if release != nil {
		defer release()
	}
	result := Result{Machine: p.machine, Session: session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	e := launch()
	if e != nil {
		d.mu.Lock()
		if !d.state.Stopped {
			d.stopLocked(d.clock.Now(), 0, true)
		}
		d.mu.Unlock()
	}
	wake := time.NewTicker(100 * time.Millisecond)
	defer wake.Stop()
	for {
		// Release proof is already durable. Re-observing the intentionally
		// disconnected containers here would fabricate a containment failure.
		d.mu.Lock()
		released := d.state.Released
		d.mu.Unlock()
		if released {
			break
		}
		done, cleanupErr := d.reconcile()
		if cleanupErr != nil {
			d.mu.Lock()
			d.state.CleanupCause = runtimeDiagnostic(cleanupErr)
			d.state.Error = "Docker cleanup unconfirmed; " + d.state.CleanupCause.String()
			_ = d.saveLocked()
			d.mu.Unlock()
		}
		if done {
			break
		}
		select {
		case <-wake.C:
		case <-d.halt:
			output.Close()
			return
		}
	}
	p.mu.Lock()
	result.PoliteStop = p.polite
	p.mu.Unlock()
	d.mu.Lock()
	result = dockerOutcome(d.state, result, e)
	container := d.state.Container
	logsNone := d.state.LogsNone
	d.mu.Unlock()
	if container != "" && !logsNone {
		ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
		cmd := exec.CommandContext(ctx, d.binary, "--host", d.state.Endpoint, "logs", container)
		cmd.Env = d.env
		cmd.Stdout = output
		cmd.Stderr = output
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error { return signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL) }
		cmd.WaitDelay = d.timeout
		if logErr := cmd.Run(); logErr != nil {
			result.Success = false
			result.CleanupCause = runtimeDiagnostic(runtimeErrorFor("logs", logErr))
			if result.Error == "" {
				result.Error = "Docker log collection failed"
			}
		}
		cancel()
	}
	syncErr := syncCommandOutput(output)
	closeErr := output.Close()
	if syncErr != nil || closeErr != nil {
		result.Success = false
		result.CleanupCause = runtimeDiagnostic(runtimeErrorFor("persist", errors.Join(syncErr, closeErr)))
		if result.Error == "" {
			result.Error = "persist Docker output failed"
		}
	}
	err := saveWorkerJSON(d.dir, d.state.Identity+".result.json", result)
	p.done <- workerFinish{result: result, err: err}
}

// A bare zero exit code, or terminal state first observed during recovery,
// cannot authorize main success. Eligibility is frozen before cleanup resumes.
func (state dockerExecution) completedMain() bool {
	return state.MainFinished && state.Container != "" && state.Stopped && state.StopAt != nil && !state.Pending && !state.OperatorConfirmed
}

func dockerOutcome(state dockerExecution, owner Result, e error) Result {
	result := owner
	result.CleanupCause = safeRuntimeCause(state.CleanupCause)
	infrastructure := state.InfrastructureStop || errors.Is(e, errWorkerStartAuthorization)
	if e != nil {
		infrastructure = infrastructure || e.Error() == "worker restarted; Docker cleanup required" || errors.Is(e, context.DeadlineExceeded) || errors.Is(e, context.Canceled)
		result.ExecutionCause = runtimeDiagnostic(runtimeErrorFor("launch", e))
		result.Error = "Docker launch failed: " + result.ExecutionCause.String()
		if result.ExecutionCause.Class == "admission_changed" {
			result.Error = "Docker launch failed: host allowance changed since queue admission"
		}
		if result.ExecutionCause.Operation == "readiness" && result.ExecutionCause.Class == "timeout" {
			result.Error = "Docker readiness timeout"
		}
		if strings.HasPrefix(e.Error(), "Docker memory ceiling not enforced") {
			result.Error = "Docker memory ceiling not enforced"
		}
	}
	result.Forced = state.Forced && !state.MainFinished
	result.Success = e == nil && state.ExitCode == 0
	if state.MainFinished && state.ExitCode != 0 {
		result.Error = fmt.Sprintf("Docker main exited with status %d", state.ExitCode)
		code := state.ExitCode
		result.ExecutionCause = &RuntimeCause{Operation: "main", Class: "command_exit", ExitCode: &code}
	}
	if state.Failure != "" {
		result.Error = state.Failure
		result.ExecutionCause = &RuntimeCause{Operation: "group", Class: "stage_failed"}
	}
	if state.MemoryFailure != "" {
		result.Error = state.MemoryFailure
		result.ExecutionCause = &RuntimeCause{Operation: "main", Class: "resource_limit"}
	}
	if state.OOMKilled {
		result.ExecutionCause = &RuntimeCause{Operation: "main", Class: "oom"}
		result.Error = "Docker container OOM-killed without a declared memory ceiling"
		if state.MemoryLimitBytes > 0 {
			result.Error = fmt.Sprintf("exceeded its %g GiB memory ceiling", float64(state.MemoryLimitBytes)/(1<<30))
		}
	}
	result = classifyAttemptOutcome(result, attemptCause{
		Infrastructure: infrastructure,
		StageFailure:   state.Failure != "" || state.MemoryFailure != "" || state.MainFinished && state.ExitCode != 0 || e != nil && !infrastructure,
		OutOfMemory:    state.OOMKilled,
		AmbiguousKill:  state.MemoryFailure == "" && (state.AmbiguousKill || state.Failure == "" && state.ExitCode == 137),
	})
	if result.Interrupted {
		result.Success = false
	}
	return result
}

func (d *dockerAttempt) close() { d.haltOnce.Do(func() { close(d.halt) }); <-d.closed }

func dockerRecoveryError(state dockerExecution) error {
	if state.completedMain() {
		return nil
	}
	return errors.New("worker restarted; Docker cleanup required")
}

func (w Worker) restoreDocker(s *workerStore) (*process, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker.json"))
	if err != nil {
		return nil, err
	}
	var state dockerExecution
	if err = json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Identity != s.state.Identity || state.Endpoint == "" || (state.Owner != nil && !s.matchesResult(*state.Owner, w.Machine.ID)) {
		return nil, errors.New("invalid Docker custody record")
	}
	if state.OperatorConfirmed {
		state.Released = false
		state.Pending = true
	}
	if state.Released {
		if s.state.Result == nil {
			// Missing final output/result remains an interruption, but durable
			// stage/OOM evidence cannot be erased by this recovery boundary.
			state.InfrastructureStop = true
			r := dockerOutcome(state, Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}}, nil)
			r.Success = false
			if r.Interrupted {
				r.Error = "worker restarted after Docker release"
			}
			s.state.Result = &r
		}
		return nil, nil
	}
	if w.DockerEndpoint == "" {
		return nil, errors.New("Docker runtime disabled; restore --docker-host to recover saved Docker execution; custody remains pending")
	}
	binary, err := exec.LookPath("docker")
	if err != nil {
		binary = "docker"
	} // Retry a missing CLI without declaring release.
	state.LogsNone = state.LogsNone || s.state.Job.Spec.Logs == "none"
	d := &dockerAttempt{closed: make(chan struct{}), halt: make(chan struct{}), state: state, dir: s.dir, binary: binary, clock: w.Clock, timeout: w.dockerCallTimeout}
	if d.timeout <= 0 {
		d.timeout = 2 * time.Second
	}
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if k != "MYCE_TOKEN" && k != "DOCKER_CONTEXT" {
			d.env = append(d.env, v)
		}
	}
	// A saved stop keeps its original deadline and dispatch latches. An
	// unfinished launch/running attempt still requires immediate forced cleanup.
	recoveryErr := dockerRecoveryError(state)
	if !state.Stopped {
		d.stop(w.Clock.Now(), 0, true)
	}
	policy := ""
	if state.LogsNone {
		policy = "none"
	}
	output, err := openCommandOutput(s.dir, s.state.Log, policy, os.O_APPEND)
	if err != nil {
		return nil, err
	}
	p := &process{job: *s.state.Job, machine: w.Machine.ID, done: make(chan workerFinish, 1), docker: d}
	go d.run(p, *s.state.Job, s.state.Session, output, nil, func() error { return recoveryErr })
	return p, nil
}

// ReconcileDockerAttempt never replaces unknown disposition with an assertion.
func ReconcileDockerAttempt(ctx context.Context, dir, identity string) error {
	if os.Geteuid() == 0 {
		return ErrHostIdentity
	}
	if dir == "" || identity == "" || filepath.Base(identity) != identity {
		return errors.New("resolve-docker requires exact worker state and attempt identity")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := (Worker{StateDir: dir}).openStore()
	if err != nil {
		return err
	}
	defer store.close()
	if store.state.Identity != identity || store.state.Job == nil || !store.state.Job.Spec.Run.Docker {
		return errors.New("Docker attempt does not match current worker custody")
	}
	b, err := os.ReadFile(filepath.Join(dir, identity+".docker.json"))
	if err != nil {
		return err
	}
	var state dockerExecution
	if json.Unmarshal(b, &state) != nil || state.Identity != identity || state.Endpoint == "" {
		return errors.New("invalid Docker custody record")
	}
	d := &dockerAttempt{ctx: ctx, dir: dir, binary: "docker", clock: RealClock{}, timeout: 2 * time.Second, state: state, env: dockerClientEnvironment()}
	d.stop(d.clock.Now(), StopGrace, false)
	released, err := d.reconcile()
	if err != nil {
		return err
	}
	if !released {
		return errors.New("Docker operation disposition or release remains unconfirmed")
	}
	return nil
}
func defaultDockerEndpoint(value string) string {
	if value == "" {
		return "unix:///var/run/docker.sock"
	}
	return value
}
func dockerClientEnvironment() []string {
	var env []string
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if k != "MYCE_TOKEN" && k != "DOCKER_CONTEXT" {
			env = append(env, v)
		}
	}
	return env
}

func dockerDeviceEnvironment(name string) bool {
	return slices.Contains([]string{"CUDA_VISIBLE_DEVICES", "ONEAPI_DEVICE_SELECTOR", "ZE_AFFINITY_MASK"}, name)
}

func dockerAttemptFilter(identity string) string {
	if identity == "" {
		return "label=myce.attempt"
	}
	return "label=myce.attempt=" + identity
}
