package myce

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var dockerGroupName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.-]*[$]?$`)

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
		case "--env", "-e", "--volume", "-v", "--device", "--gpus", "--entrypoint", "--workdir", "-w", "--user", "-u", "--group-add":
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
			if flag == "--group-add" && !dockerGroupName.MatchString(value) {
				if _, err := strconv.ParseUint(value, 10, 31); err != nil {
					return nil, errors.New("Docker --group-add requires a GID from 0 to 2147483647 or group name")
				}
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
	FileMounts       []dockerFileMount   `json:"file_mounts,omitempty"`
	Accelerator      bool                `json:"accelerator,omitempty"`
	UserCancel       bool                `json:"user_cancel,omitempty"`
	StopGrace        time.Duration       `json:"accelerator_stop_grace,omitempty"`
	PriorityStop     *PriorityPreemption `json:"priority_stop,omitempty"`
	CleanupCause     *RuntimeCause       `json:"cleanup_cause,omitempty"`
	ObservationRetry string              `json:"observation_retry,omitempty"`

	LogsNone             bool                    `json:"logs_none,omitempty"`
	Owner                *Result                 `json:"owner,omitempty"` // Saved job/attempt/machine/session binding; legacy custody uses opaque identity.
	EvidenceSaved        bool                    `json:"evidence_saved,omitempty"`
	NetworkID            string                  `json:"network_id,omitempty"`
	NetworkRemoved       bool                    `json:"network_removed,omitempty"`
	Network              string                  `json:"network,omitempty"`
	Containers           map[string]dockerMember `json:"containers,omitempty"`
	PendingContainer     string                  `json:"pending_container,omitempty"`
	PendingName          string                  `json:"pending_name,omitempty"`
	MainFinished         bool                    `json:"main_finished,omitempty"`
	Infrastructure       []infrastructureFact    `json:"infrastructure_facts,omitempty"`
	ObservationLost      bool                    `json:"observation_lost,omitempty"`
	InfrastructureStop   bool                    `json:"infrastructure_stop,omitempty"`
	AmbiguousKill        bool                    `json:"ambiguous_kill,omitempty"`
	Failure              string                  `json:"failure,omitempty"`
	CgroupParent         string                  `json:"cgroup_parent,omitempty"`
	EnvelopeReleased     bool                    `json:"envelope_released,omitempty"`
	EnvelopeEvidenceLost bool                    `json:"envelope_evidence_lost,omitempty"`
	MemoryPeak           *AttemptMemoryPeak      `json:"memory_peak,omitempty"`
	MemoryLimitBytes     int64                   `json:"memory_limit_bytes,omitempty"`
	MemorySwapBytes      int64                   `json:"memory_swap_bytes,omitempty"`
	MemoryFailure        string                  `json:"memory_failure,omitempty"`
	OOMKilled            bool                    `json:"oom_killed,omitempty"`
	Identity             string                  `json:"identity"`
	Endpoint             string                  `json:"endpoint"`
	Container            string                  `json:"container,omitempty"`
	DispatchedAt         *time.Time              `json:"dispatched_at,omitempty"`
	Operation            string                  `json:"operation,omitempty"`
	Pending              bool                    `json:"pending,omitempty"`
	Stopped              bool                    `json:"stopped,omitempty"`
	Forced               bool                    `json:"forced,omitempty"`
	StopAt               *time.Time              `json:"stop_at,omitempty"`
	OperatorConfirmed    bool                    `json:"operator_confirmed,omitempty"`
	Released             bool                    `json:"released,omitempty"`
	Error                string                  `json:"error,omitempty"`
	ExitCode             int                     `json:"exit_code"`
}
type dockerAttempt struct {
	memory          *memoryAttempt
	storageObserver *storageObserver
	memoryWorker    *Worker
	command         func(context.Context, ...string) ([]byte, error) // Injectable daemon boundary.
	persist         func(dockerExecution) error                      // Optional custody store for fault injection.
	admit           func() error
	dirty           bool
	closed          chan struct{}
	halt            chan struct{}
	haltOnce        sync.Once
	mu              sync.Mutex
	state           dockerExecution
	dir, binary     string

	ctx       context.Context
	inventory func([]string)
	workdir   string
	env       []string
	clock     Clock
	timeout   time.Duration
}

// Starting a container includes runtime/mount setup, unlike an observation RPC.
// The former 2s budget cancelled real starts on a loaded host (#149). Allow
// 30s for that work; a hung start costs at most 28s more before exact cleanup.
const dockerStartTimeout = 30 * time.Second

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
	changed := !d.state.Stopped || d.dirty
	if d.state.Accelerator && !d.state.UserCancel {
		if kill {
			grace = d.state.StopGrace
		}
		grace = min(acceleratorStopGrace(grace), acceleratorStopGrace(d.state.StopGrace))
		deadline := now.Add(grace)
		if d.state.Stopped && d.state.StopAt != nil && d.state.StopAt.Before(deadline) {
			deadline = *d.state.StopAt
		}
		grace = max(0, deadline.Sub(now))
		kill = grace == 0
		if d.state.Forced != kill {
			d.state.Forced = kill
			changed = true
		}
	}
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
func (d *dockerAttempt) recordInfrastructure(fact infrastructureFact, cause error) error {
	if fact.Kind == launchRefused || fact.Kind == launchFailed || fact.Kind == environmentFailed {
		fact.Predicate = launchFailurePredicate(fact.Kind, cause)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	prior := d.state.Infrastructure
	d.state.Infrastructure = append(append([]infrastructureFact(nil), prior...), fact)
	if err := d.saveLocked(); err != nil {
		d.state.Infrastructure = prior
		return errors.Join(cause, err)
	}
	return cause
}

func (d *dockerAttempt) recordObservationLoss() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state.MainFinished || d.state.Failure != "" || d.state.MemoryFailure != "" {
		return nil
	}
	prior := d.state.ObservationLost
	d.state.ObservationLost = true
	if err := d.saveLocked(); err != nil {
		d.state.ObservationLost = prior
		return err
	}
	return nil
}

func (d *dockerAttempt) call(args ...string) ([]byte, error) {
	observation := len(args) > 0 && (args[0] == "ps" || args[0] == "inspect" || args[0] == "network" && len(args) > 1 && (args[1] == "ls" || args[1] == "inspect"))
	if !observation {
		return d.callWithTimeout(d.timeout, args...)
	}
	// A read timeout says nothing about the payload. Retry only the read,
	// leaving exact custody and all stop/launch intent untouched. The capped
	// backoff limits daemon load to one retry per 5s during a prolonged outage;
	// its cost is at most 5s extra observation/cleanup latency after recovery.
	clock := d.clock
	if clock == nil {
		clock = RealClock{}
	}
	var since time.Time
	backoff := 100 * time.Millisecond
	warned := false
	for failures := 0; ; {
		out, err := d.callWithTimeout(d.timeout, args...)
		if !errors.Is(err, context.DeadlineExceeded) || d.ctx != nil && d.ctx.Err() != nil {
			if failures > 0 {
				d.mu.Lock()
				d.state.ObservationRetry = ""
				_ = d.saveLocked()
				d.mu.Unlock()
			}
			return out, err
		}
		failures++
		if failures == 1 {
			since = clock.Now()
		}
		elapsed := clock.Now().Sub(since).Round(time.Second)
		d.mu.Lock()
		d.state.CleanupCause = runtimeDiagnostic(err)
		d.state.ObservationRetry = fmt.Sprintf("Docker observation retry; custody retained; unavailable %s; %d timed-out reads; %s", elapsed, failures, d.state.CleanupCause.String())
		// Heartbeats must keep their recognized launch/running transition. A
		// read timeout is a diagnostic, not a cleanup/admission refusal (#149).
		if d.state.Stopped {
			d.state.Error = "Docker cleanup unconfirmed; " + d.state.CleanupCause.String()
		}
		_ = d.saveLocked()
		if elapsed >= 2*time.Minute && !warned {
			log.Printf("attempt %s: %s", d.state.Identity, d.state.ObservationRetry)
			warned = true
		}
		d.mu.Unlock()
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-d.halt:
			timer.Stop()
			return nil, runtimeErrorFor(args[0], context.Canceled)
		}
		backoff = min(2*backoff, 5*time.Second)
	}
}
func (d *dockerAttempt) callWithTimeout(timeout time.Duration, args ...string) (out []byte, err error) {
	operation := "reconcile"
	if len(args) > 0 {
		operation = args[0]
		if operation == "rm" {
			operation = "reconcile"
		}
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
			return nil, d.recordInfrastructure(infrastructureFact{Kind: launchRefused, At: time.Now().UTC()}, err)
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
	timeout := d.timeout
	if kind == "start" {
		timeout = max(timeout, dockerStartTimeout)
	}
	out, err := d.callWithTimeout(timeout, args...)
	if err != nil {
		err = d.recordInfrastructure(infrastructureFact{Kind: launchFailed, At: time.Now().UTC()}, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err == nil {
		d.state.Pending = false
		d.state.Error = ""
	} else if refusal := new(exec.ExitError); errors.As(err, &refusal) && strings.HasPrefix(strings.TrimSpace(string(refusal.Stderr)), "Error response from daemon:") {
		if kind == "create" && len(d.state.FileMounts) > 0 {
			// Daemon paths and refusal details stay in the worker-local log.
			log.Printf("attempt %s: Docker file create refused: %s", d.state.Identity, strings.TrimSpace(string(refusal.Stderr)))
		}
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
		CgroupParent    string
		NetworkMode     string
		PortBindings    map[string]json.RawMessage
		PublishAllPorts bool
	} `json:"HostConfig"`
	NetworkSettings struct {
		Networks map[string]struct{ IPAddress string }
	} `json:"NetworkSettings"`
	Mounts []dockerFileMount                  `json:"Mounts"`
	ID     string                             `json:"Id"`
	Name   string                             `json:"Name"`
	Config struct{ Labels map[string]string } `json:"Config"`
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
		if saveErr := d.recordObservationLoss(); saveErr != nil {
			return nil, errors.Join(err, saveErr)
		}
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
	if handled, err := d.settleFailedStart(containers); handled || err != nil {
		return false, err
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
				// An earlier inactive snapshot was provisional if rm raced this
				// start. Capture the actual terminal evidence after stopping it.
				m := d.state.Containers[c.ID]
				m.ExitCode, m.StderrTail = nil, ""
				if d.state.Containers != nil {
					d.state.Containers[c.ID] = m
				}
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
		d.stopLocked(d.clock.Now(), 0, true)
		forced = d.state.Forced
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
			if c.ID != d.state.Container {
				// A helper killed for unsafe containment must also stop main.
				d.stopLocked(d.clock.Now(), 0, true)
			}
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
	if err := d.releaseEnvelope(); err != nil {
		return false, err
	}
	d.state.Released = true
	d.state.Error = ""
	if err := d.saveLocked(); err != nil {
		d.state.Released = false
		return false, err
	}
	return true, nil
}

// A lost start reply must not require a start that Docker has already cancelled
// ever to become visible. Start addresses an existing immutable container ID:
// removing that exact, inspected inactive object prevents a delayed start. The
// existing pending-start intent survives a crash between rm and its receipt.
// Unlike a pending create, an exact absent ID cannot materialize later.
func (d *dockerAttempt) settleFailedStart(containers []dockerInspection) (bool, error) {
	d.mu.Lock()
	pending := d.state.Pending && d.state.Stopped
	op, id, dispatched := d.state.Operation, d.state.PendingContainer, d.state.DispatchedAt
	if id == "" {
		id = d.state.Container
	}
	d.mu.Unlock()
	if !pending || id == "" || op != "start" {
		return false, nil
	}
	for _, c := range containers {
		if c.ID != id {
			continue
		}
		// Preserve the existing positive-start and bounded signal protocol.
		if dockerLive(c) || dispatched != nil && !c.State.StartedAt.IsZero() && !c.State.StartedAt.Before(*dispatched) {
			return false, nil
		}
		if err := d.observeGroup(containers); err != nil {
			return true, err
		}
		d.mu.Lock()
		m := d.state.Containers[id]
		m.ExitCode, m.StderrTail = nil, "" // Refresh after a refused prior rm.
		if d.state.Containers != nil {
			d.state.Containers[id] = m
		}
		d.mu.Unlock()
		if err := d.retainEvidence([]dockerInspection{c}); err != nil {
			return true, err
		}
		// No force: a start racing inspection makes rm refuse, retaining custody.
		_, err := d.call("rm", id)
		return true, err // Re-inventory before settling removal.
	}
	// Do not treat a missing label match as proof: inspect the exact ID too.
	_, err := d.call("inspect", id)
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return true, errors.New("pending Docker start absence unconfirmed")
	}
	message := strings.TrimSpace(string(exit.Stderr))
	if !strings.EqualFold(message, "Error: No such object: "+id) && !strings.EqualFold(message, "Error response from daemon: No such container: "+id) {
		return true, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state.Pending = false
	d.state.Error = ""
	if d.state.Containers == nil {
		d.state.Containers = map[string]dockerMember{}
	}
	m := d.state.Containers[id]
	m.Removed = true
	d.state.Containers[id] = m
	return true, d.saveLocked()
}

// TERM intent is permanent once durable; every delivery error waits for the
// deadline. KILL is idempotent and is retried while inspection proves running.
func (d *dockerAttempt) signalContainer(id string, kill bool) ([]byte, error) {
	d.mu.Lock()
	if d.dir == "" {
		d.state.Accelerator = true
	}
	if kill {
		if d.state.Accelerator && !d.state.UserCancel {
			d.stopLocked(d.clock.Now(), 0, true)
			kill = d.state.Forced
		}
	}
	d.mu.Unlock()
	if d.dir == "" {
		return d.stopOrphan(id, kill)
	}
	if kill {
		out, err := d.call("kill", "--signal=KILL", id)
		if err != nil {
			return out, err
		}
		// A storage outage cannot suppress deadline KILLs to other members.
		// Failed persistence rolls the record back and leaves custody dirty;
		// classification then has no delivery evidence for this signal.
		_ = d.recordSignal(id, "KILL", time.Now().UTC())
		return out, nil
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
	at := time.Now().UTC()
	d.mu.Lock()
	m = d.state.Containers[id]
	m.TermOutcome = "sent"
	if err == nil {
		m.Signals = append(m.Signals, deliveredSignal{Signal: "TERM", At: at})
	}
	if err != nil {
		m.TermOutcome = "error"
	}
	d.state.Containers[id] = m
	saveErr := d.saveLocked()
	d.mu.Unlock()
	if saveErr != nil {
		d.mu.Lock()
		m = d.state.Containers[id]
		if err == nil {
			m.Signals = m.Signals[:len(m.Signals)-1]
		}
		d.state.Containers[id] = m
		d.mu.Unlock()
		return out, saveErr
	}
	return out, err
}

func (d *dockerAttempt) recordSignal(id, name string, at time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	m := d.state.Containers[id]
	prior := m.memberOutcome
	m.Signals = append(append([]deliveredSignal(nil), m.Signals...), deliveredSignal{Signal: name, At: at})
	if d.state.Containers == nil {
		d.state.Containers = map[string]dockerMember{}
	}
	d.state.Containers[id] = m
	if err := d.saveLocked(); err != nil {
		m.memberOutcome = prior
		d.state.Containers[id] = m
		return err
	}
	return nil
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

func (w Worker) startDocker(s *workerStore, j Job, checkpoint string, release func()) (proc *process, resultErr error) {
	if dockerNeedsEnvelope(j.Spec) && !w.dockerEnvelopeSupported() {
		return nil, errors.New(dockerEnvelopeUnavailable)
	}
	if err := w.verifyDevice(); err != nil {
		return nil, s.startFailure(j, launchRefused, err)
	}
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
	observer := w.observeAttemptStorage(j, s.dir, checkpoint, identity, workerStorageMetadata(s))
	defer func() {
		if proc == nil {
			observer.finish()
		}
	}()
	endpoint := w.DockerEndpoint
	if os.Getenv("DOCKER_CONTEXT") != "" {
		return nil, errors.New("managed Docker requires --docker-host, not DOCKER_CONTEXT")
	}
	launch := HostLaunch{Machine: w.Machine.ID, Session: s.state.Session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(j.Attempt)}
	d := &dockerAttempt{admit: func() error {
		if err := w.verifyDevice(); err != nil {
			return err
		}
		if err := w.checkHostMemoryBudget(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB); err != nil {
			return err
		}
		return w.authorizeLaunch(launch)
	}, storageObserver: observer, closed: make(chan struct{}), halt: make(chan struct{}), dir: s.dir, workdir: j.Spec.Run.Workdir, binary: binary, clock: w.Clock, timeout: w.dockerCallTimeout, state: dockerExecution{Identity: identity, Endpoint: endpoint, MemoryLimitBytes: ceilingBytes, MemorySwapBytes: 0}}
	launched := false
	defer func() {
		if !launched && d.memory != nil {
			resultErr = errors.Join(resultErr, d.releaseEnvelope())
		}
	}()
	if dockerNeedsEnvelope(j.Spec) {
		parent, e := w.memoryCeilings.create("attempt-"+identity, ceilingBytes, 0)
		if e != nil {
			return nil, e
		}
		if _, e = parent.children(); e != nil {
			_ = parent.cleanup(w.Clock)
			return nil, e
		}
		if e = parent.setOOMGroup(true); e != nil {
			_ = parent.cleanup(w.Clock)
			return nil, e
		}
		relative, e := filepath.Rel("/sys/fs/cgroup", parent.path)
		if e != nil || strings.HasPrefix(relative, "..") {
			_ = parent.cleanup(w.Clock)
			return nil, errors.New(dockerEnvelopeUnavailable)
		}
		d.state.CgroupParent = "/" + relative
		d.memory = parent
		d.memoryWorker = &w
	}
	d.state.Accelerator = !j.Spec.Needs.cpuOnly()
	d.state.StopGrace = acceleratorStopGrace(w.AcceleratorStopGrace)
	d.state.Owner = &Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	d.command = w.dockerCommand
	d.state.LogsNone = j.Spec.Logs == "none"
	if dockerFileJob(j.Spec) {
		d.state.FileMounts, err = dockerFileMounts(j, s.dir, checkpoint)
		if err != nil {
			return nil, err
		}
	}
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
		if k != "MYCE_TOKEN" && !strings.HasPrefix(k, "DOCKER_") && !dockerDeviceEnvironment(k) {
			d.env = append(d.env, k+"="+v)
		}
	}
	// Persist custody before publishing its identity in the attempt record.
	// No Docker operation can be dispatched until both writes succeed.
	if err = d.saveLocked(); err != nil {
		return nil, err
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
	err = s.save()
	if err != nil {
		output.Close()
		return nil, err
	}
	p := &process{machine: w.Machine.ID, job: j, done: make(chan workerFinish, 1), docker: d, savePriorityStop: s.priorityStopRecorder()}
	if w.control != nil {
		w.control.set(p)
	}
	label := "myce.attempt=" + identity
	create := []string{"create", "--restart=no", "--pull=never", "--init", "--name", "myce-" + identity, "--label", label, "--label", "myce.worker=" + w.Machine.ID, "--cidfile", filepath.Join(s.dir, identity+".cid"), "--env", "MYCE_ATTEMPT_ID=" + identity, "--env", "MYCE_DOCKER_LABEL=" + label, "--env", "MYCE_JOB_ID=" + j.ID, "--env", "MYCE_ATTEMPT=" + strconv.Itoa(j.Attempt), "--env", "MYCE_CHECKPOINT_DIR=/myce/checkpoint"}
	progressFile := applicationProgressFile(s.dir, j)
	create = append(create, "--env", "MYCE_PROGRESS_FILE=/myce/progress/"+filepath.Base(progressFile))
	if !dockerFileJob(j.Spec) {
		create = append(create, "--volume", checkpoint+":/myce/checkpoint", "--volume", filepath.Dir(progressFile)+":/myce/progress")
	}
	if dockerFileJob(j.Spec) {
		// --mount refuses a missing source instead of creating a root-owned
		// directory on an incompatible/remote daemon. Never bind cache blobs.
		for _, mount := range d.state.FileMounts {
			value := "type=bind,src=" + mount.Source + ",dst=" + mount.Destination
			if !mount.RW {
				value += ",readonly"
			}
			create = append(create, "--mount", value)
		}
		create = append(create, "--user", strconv.Itoa(os.Geteuid())+":"+strconv.Itoa(os.Getegid()), "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--workdir", "/myce/work", "--env", "MYCE_INPUT_DIR=/myce/inputs", "--env", "MYCE_OUTPUT_DIR=/myce/work", "--env", "MYCE_SCRATCH_DIR=/myce/work")
	}
	if d.state.CgroupParent != "" {
		create = append(create, "--cgroup-parent", d.state.CgroupParent, "--memory-swap", strconv.FormatInt(ceilingBytes, 10))
	}
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
	bound, e := w.deviceEnvironment()
	if e != nil {
		return nil, e
	}
	for k, v := range bound {
		create = append(create, "--env", k+"="+v)
	}
	for k, v := range j.Spec.Run.Env {
		if !dockerFileJob(j.Spec) && !strings.HasPrefix(k, "MYCE_") && !strings.HasPrefix(k, "DOCKER_") && !dockerDeviceEnvironment(k) {
			create = append(create, "--env", k+"="+v)
		}
	}
	create = append(create, args...)
	launched = true
	go d.run(p, j, s.state.Session, output, release, func() error {
		if e := w.authorizeStart(j, j.Session); e != nil {
			return d.recordInfrastructure(infrastructureFact{Kind: launchRefused, At: time.Now().UTC()}, e)
		}
		if dockerFileJob(j.Spec) {
			// Refuse automatic anonymous volumes before create can allocate them.
			if e := d.checkFileImages(j); e != nil {
				return e
			}
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
	finishMemory := func() *AttemptMemoryPeak { return nil }
	if d.memoryWorker != nil {
		finishMemory = d.memoryWorker.observeAttemptMemory(d.envelope(), j, nil)
	}
	defer finishMemory()
	stopEvents := d.observeDockerEvents()
	defer stopEvents()
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
	sample := finishMemory()
	d.mu.Lock()
	if !d.state.EnvelopeEvidenceLost {
		d.state.MemoryPeak = sampledMemoryPeak(d.state.MemoryPeak, sample)
	}
	result = dockerOutcome(d.state, result, e)
	container := d.state.Container
	logsNone := d.state.LogsNone
	mainRemoved := d.state.Containers[container].Removed
	d.mu.Unlock()
	if container != "" && !logsNone && !mainRemoved {
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
	stopEvents()
	result.StoragePeak = d.storageObserver.finish()
	err := saveWorkerJSON(d.dir, d.state.Identity+".result.json", result)
	p.done <- workerFinish{result: result, err: err}
}

// A completed main resumes cleanup directly. dockerOutcome also preserves
// terminal observations made while recovery settles an unfinished attempt.
func (state dockerExecution) completedMain() bool {
	return state.MainFinished && state.Container != "" && state.Stopped && state.StopAt != nil && !state.Pending && !state.OperatorConfirmed
}

func dockerOutcome(state dockerExecution, owner Result, e error) Result {
	result := owner
	result.MemoryPeak = state.MemoryPeak
	// Recovery is a cause only when no terminal main outcome was observed.
	if state.MainFinished && errors.Is(e, errDockerRecovery) {
		e = nil
	}
	result.CleanupCause = safeRuntimeCause(state.CleanupCause)
	facts := state.Containers[state.Container].memberOutcome
	facts.ObservationLost = state.ObservationLost
	if e != nil {
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
		if errors.Is(e, errDockerFileImageVolumes) {
			result.Error = errDockerFileImageVolumes.Error()
		}
	}
	result.Forced = state.Forced
	result.Success = e == nil && state.ExitCode == 0
	if state.ExitCode != 0 {
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
	// A helper failure supplies that member's facts, never the main's signals.
	if state.Failure != "" {
		facts = memberOutcome{}
		for _, m := range state.Containers {
			if m.Name != "main" && m.Exit != nil && !m.deliveredBeforeExit() {
				facts = m.memberOutcome
				break
			}
		}
		result.Success = false
	}
	if state.MemoryFailure != "" {
		result.Success = false
		facts = memberOutcome{}
	}
	// A launch/readiness failure already ended the attempt. Signals used only
	// to clean it up cannot replace that outcome with a helper interruption.
	if !state.MainFinished && facts.Exit == nil && state.MemoryFailure == "" && e == nil {
		for _, m := range state.Containers {
			if m.Exit != nil && m.deliveredBeforeExit() {
				lost := facts.ObservationLost
				facts = m.memberOutcome
				facts.ObservationLost = facts.ObservationLost || lost
				break
			}
		}
	}

	facts.ObservationLost = facts.ObservationLost || state.ObservationLost
	facts.Infrastructure = append(append([]infrastructureFact(nil), facts.Infrastructure...), state.Infrastructure...)
	// Legacy custody without a timestamp is insufficient signal evidence.
	if facts.Exit == nil && state.ExitCode != 0 && state.MemoryFailure == "" {
		facts.Exit = &memberExit{Status: state.ExitCode}
	}
	result = classifyAttemptOutcome(result, attemptCause{PriorityStop: state.PriorityStop, memberOutcome: facts, OutOfMemory: state.OOMKilled})
	if state.EnvelopeEvidenceLost {
		result.MemoryPeak = nil
		if !state.OOMKilled {
			result.Interrupted, result.Success = true, false
			result.Error = "Docker shared envelope ended with worker stop; memory peak and OOM cause unavailable"
		}
	}
	if result.Interrupted {
		result.Success = false
	}
	return result
}

// Let the independent reconciler deliver TERM to every member and enforce its
// durable deadline before shutting observation down. Unknown release stays held.
func (d *dockerAttempt) waitShutdown(clock Clock) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		d.mu.Lock()
		until, released := d.state.StopAt, d.state.Released || !d.state.Accelerator || d.state.UserCancel
		d.mu.Unlock()
		if released || until == nil || !clock.Now().Before(until.Add(5*time.Second)) {
			return
		}
		<-tick.C
	}
}

func (d *dockerAttempt) close() { d.haltOnce.Do(func() { close(d.halt) }); <-d.closed }

var errDockerRecovery = errors.New("worker restarted; Docker cleanup required")

func dockerRecoveryError(state dockerExecution) error {
	if state.completedMain() {
		return nil
	}
	return errDockerRecovery
}

func (w Worker) restoreDocker(s *workerStore) (*process, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker.json"))
	if errors.Is(err, os.ErrNotExist) {
		// A settled pre-launch refusal (including legacy generic identities)
		// needs no Docker recovery. Missing custody otherwise proves loss of
		// observation, never release: keep this attempt's exclusion pending.
		if s.state.Result == nil || s.state.Result.ReleasePending {
			if s.state.Result == nil {
				facts := memberOutcome{ObservationLost: true}
				if err := saveWorkerJSON(s.dir, s.state.Identity+".observation-loss.json", facts); err != nil {
					return nil, err
				}
				r := classifyAttemptOutcome(Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, ReleasePending: true, Error: "worker restarted; owned Docker state missing; release unconfirmed"}, attemptCause{memberOutcome: facts, PriorityStop: s.state.PriorityStop})
				s.state.Result = &r
				if err := s.save(); err != nil {
					return nil, err
				}
				log.Printf("attempt %s/%d: owned Docker state missing; interrupted, release unconfirmed", s.state.Job.ID, s.state.Job.Attempt)
			}
		}
		return nil, nil
	}
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
			stageFailure := !r.Success && !r.Interrupted
			r.Success = false
			facts := memberOutcome{ObservationLost: !stageFailure}
			if err := saveWorkerJSON(s.dir, state.Identity+".observation-loss.json", facts); err != nil {
				return nil, err
			}
			r = classifyAttemptOutcome(r, attemptCause{PriorityStop: state.PriorityStop, memberOutcome: facts})
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
	state.Accelerator = !s.state.Job.Spec.Needs.cpuOnly()
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
	// unfinished launch/running attempt still requires bounded runtime cleanup.
	recoveryErr := dockerRecoveryError(state)
	if recoveryErr != nil {
		if err := d.recordObservationLoss(); err != nil {
			return nil, err
		}
	}
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
	p := &process{job: *s.state.Job, machine: w.Machine.ID, done: make(chan workerFinish, 1), docker: d, savePriorityStop: s.priorityStopRecorder()}
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
	state.Accelerator = !store.state.Job.Spec.Needs.cpuOnly()
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
	return slices.Contains([]string{"CUDA_VISIBLE_DEVICES", "ONEAPI_DEVICE_SELECTOR", "ZE_AFFINITY_MASK", "ZE_ENABLE_PCI_ID_DEVICE_ORDER"}, name)
}

func dockerAttemptFilter(identity string) string {
	if identity == "" {
		return "label=myce.attempt"
	}
	return "label=myce.attempt=" + identity
}
