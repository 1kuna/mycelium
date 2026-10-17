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
	HostRevision      HostRevision            `json:"host_revision"`
	LogsNone          bool                    `json:"logs_none,omitempty"`
	Owner             *Result                 `json:"owner,omitempty"` // Saved job/attempt/machine/session binding; legacy custody uses opaque identity.
	EvidenceSaved     bool                    `json:"evidence_saved,omitempty"`
	NetworkID         string                  `json:"network_id,omitempty"`
	NetworkRemoved    bool                    `json:"network_removed,omitempty"`
	Network           string                  `json:"network,omitempty"`
	Containers        map[string]dockerMember `json:"containers,omitempty"`
	PendingContainer  string                  `json:"pending_container,omitempty"`
	MainFinished      bool                    `json:"main_finished,omitempty"`
	Failure           string                  `json:"failure,omitempty"`
	MemoryLimitBytes  int64                   `json:"memory_limit_bytes,omitempty"`
	MemorySwapBytes   int64                   `json:"memory_swap_bytes,omitempty"`
	MemoryFailure     string                  `json:"memory_failure,omitempty"`
	OOMKilled         bool                    `json:"oom_killed,omitempty"`
	Identity          string                  `json:"identity"`
	Endpoint          string                  `json:"endpoint"`
	Container         string                  `json:"container,omitempty"`
	Operation         string                  `json:"operation,omitempty"`
	Pending           bool                    `json:"pending,omitempty"`
	Stopped           bool                    `json:"stopped,omitempty"`
	Forced            bool                    `json:"forced,omitempty"`
	StopAt            *time.Time              `json:"stop_at,omitempty"`
	OperatorConfirmed bool                    `json:"operator_confirmed,omitempty"`
	Released          bool                    `json:"released,omitempty"`
	Error             string                  `json:"error,omitempty"`
	ExitCode          int                     `json:"exit_code"`
}
type dockerAttempt struct {
	persist      func(dockerExecution) error // Optional custody store for fault injection.
	admit        func() error
	dirty        bool
	closed       chan struct{}
	halt         chan struct{}
	haltOnce     sync.Once
	mu           sync.Mutex
	state        dockerExecution
	dir, binary  string
	hostDir      string
	hostRevision HostRevision
	ctx          context.Context
	inventory    func([]string)
	workdir      string
	env          []string
	clock        Clock
	timeout      time.Duration
}

func (d *dockerAttempt) saveLocked() error {
	err := d.saveState(d.state)
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
func (d *dockerAttempt) callWithTimeout(timeout time.Duration, args ...string) ([]byte, error) {
	parent := d.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, d.binary, append([]string{"--host", d.state.Endpoint}, args...)...)
	cmd.Env = d.env
	cmd.Dir = d.workdir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = timeout
	out, err := cmd.Output()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return out, err
}
func (d *dockerAttempt) operation(kind string, args ...string) ([]byte, error) {
	if d.hostRevision.Revision == 0 {
		if err := bindHostRevision(d.hostDir, &d.hostRevision); err != nil {
			return nil, err
		}
	}
	unlock, err := hostLaunchGate(d.hostDir, d.hostRevision)
	if err != nil {
		d.stop(d.clock.Now(), 0, true)
		return nil, err
	}
	defer unlock()
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
	err = d.saveLocked()
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := CheckHostRevision(d.hostDir, d.hostRevision); err != nil {
		d.stop(d.clock.Now(), 0, true)
		return nil, err
	}
	out, err := d.call(args...)
	d.mu.Lock()
	defer d.mu.Unlock()
	if err == nil {
		d.state.Pending = false
		d.state.Error = ""
	} else if refusal, ok := err.(*exec.ExitError); ok && strings.HasPrefix(strings.TrimSpace(string(refusal.Stderr)), "Error response from daemon:") {
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
	Config struct{ Labels map[string]string }     `json:"Config"`
	State  struct {
		Running   *bool
		Status    string
		ExitCode  int
		OOMKilled bool
	} `json:"State"`
}

// Every object is inspected before signaling: names and cidfiles are not ownership proof.
func (d *dockerAttempt) containers() ([]dockerInspection, error) {
	out, err := d.call("ps", "--all", "--quiet", "--no-trunc", "--filter", dockerAttemptFilter(d.state.Identity))
	if err != nil {
		return nil, err
	}
	ids := strings.Fields(string(out))
	if d.inventory != nil {
		d.inventory(ids)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	out, err = d.call(append([]string{"inspect"}, ids...)...)
	if err != nil {
		return nil, fmt.Errorf("Docker inspection unconfirmed for containers %s: %w", strings.Join(ids, ","), err)
	}
	var containers []dockerInspection
	if err = json.Unmarshal(out, &containers); err != nil {
		return nil, err
	}
	if len(containers) != len(ids) {
		return nil, errors.New("incomplete Docker inventory")
	}
	seen := map[string]bool{}
	for _, c := range containers {
		if c.ID == "" || !slices.Contains(ids, c.ID) || seen[c.ID] || c.State.Running == nil || !slices.Contains([]string{"created", "running", "paused", "restarting", "removing", "exited", "dead"}, c.State.Status) {
			return nil, errors.New("incomplete Docker inspection")
		}
		if c.State.Status == "paused" || c.State.Status == "restarting" || c.State.Status == "removing" {
			return nil, errors.New("Docker execution not settled")
		}
		seen[c.ID] = true
		if c.Config.Labels["myce.attempt"] == "" || (d.state.Identity != "" && c.Config.Labels["myce.attempt"] != d.state.Identity) {
			return nil, errors.New("Docker ownership mismatch")
		}
	}
	return containers, nil
}
func (d *dockerAttempt) reconcile() (bool, error) {
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
	d.mu.Lock()
	stopped, forced, pending := d.state.Stopped, d.state.Forced, d.state.Pending
	// One worker-owned start request, no restart policy: running is positive acceptance evidence.
	if pending && d.state.Operation == "start" {
		for _, c := range containers {
			if c.ID == d.state.PendingContainer && *c.State.Running || (d.state.PendingContainer == "" && c.ID == d.state.Container && *c.State.Running) {
				d.state.Pending = false
				d.state.Stopped = true
				d.state.Forced = true
				d.state.Error = ""
				if err = d.saveLocked(); err != nil && !killDue {
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
		if err = d.saveLocked(); err != nil && !killDue {
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
			err = d.saveLocked()
			d.mu.Unlock()
			if err != nil && !killDue {
				return false, err
			}
			if *c.State.Running {
				if _, err = d.signalContainer(c.ID, true); err != nil {
					return false, err
				}
				running = true
				continue
			}
		}
		if *c.State.Running {
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
func (d *dockerAttempt) retainEvidence(containers []dockerInspection) error {
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
				return errors.New("Docker private stderr collection failed")
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
	unlock, err := hostLaunchGate(w.HostStateDir, w.hostRevision)
	if err != nil {
		return nil, err
	}
	defer unlock()
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
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" {
		endpoint = "unix:///var/run/docker.sock"
	}
	if os.Getenv("DOCKER_CONTEXT") != "" {
		return nil, errors.New("managed Docker requires DOCKER_HOST, not DOCKER_CONTEXT")
	}
	d := &dockerAttempt{admit: func() error { return w.checkHostMemoryBudget(j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB) }, closed: make(chan struct{}), halt: make(chan struct{}), dir: s.dir, hostDir: w.HostStateDir, hostRevision: w.hostRevision, workdir: j.Spec.Run.Workdir, binary: binary, clock: w.Clock, timeout: w.dockerCallTimeout, state: dockerExecution{HostRevision: w.hostRevision, Identity: identity, Endpoint: endpoint, MemoryLimitBytes: ceilingBytes, MemorySwapBytes: 0}}
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
	create := []string{"create", "--pull=never", "--init", "--name", "myce-" + identity, "--label", label, "--label", "myce.worker=" + w.Machine.ID, "--cidfile", filepath.Join(s.dir, identity+".cid"), "--env", "MYCE_ATTEMPT_ID=" + identity, "--env", "MYCE_DOCKER_LABEL=" + label, "--env", "MYCE_JOB_ID=" + j.ID, "--env", "MYCE_ATTEMPT=" + strconv.Itoa(j.Attempt), "--env", "MYCE_CHECKPOINT_DIR=/myce/checkpoint", "--volume", checkpoint + ":/myce/checkpoint"}
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
		alreadyStopping := d.state.Stopped
		d.mu.Unlock()
		if !alreadyStopping {
			d.stop(d.clock.Now(), 0, true)
		}
	}
	wake := time.NewTicker(100 * time.Millisecond)
	defer wake.Stop()
	for {
		done, cleanupErr := d.reconcile()
		if cleanupErr != nil {
			d.mu.Lock()
			d.state.Error = "Docker cleanup unconfirmed"
			if cleanupErr.Error() == "Docker ownership mismatch" {
				d.state.Error += "; ownership mismatch (missing attempt label)"
			}
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
			result.Error = "Docker log collection failed"
		}
		cancel()
	}
	syncErr := syncCommandOutput(output)
	closeErr := output.Close()
	if syncErr != nil || closeErr != nil {
		result.Success = false
		result.Error = "persist Docker output failed"
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
	if e != nil {
		result.Interrupted = e.Error() == "worker restarted; Docker cleanup required" || errors.Is(e, context.DeadlineExceeded)
		result.Error = "Docker launch failed: " + e.Error()
		if strings.HasPrefix(e.Error(), "Docker memory ceiling not enforced") {
			result.Error = "Docker memory ceiling not enforced"
		}
	}
	if e == nil {
		result.Interrupted = (state.Stopped && !state.MainFinished)
	}
	result.Forced = state.Forced && !state.MainFinished
	result.Success = !result.Interrupted && e == nil && state.ExitCode == 0 && state.MemoryFailure == "" && state.Failure == "" && !state.OOMKilled
	if state.Failure != "" {
		result.Error = state.Failure
		result.Interrupted = false
		result.Forced = false
	}
	if state.MemoryFailure != "" {
		result.Interrupted = false
		result.Forced = false
		result.Error = state.MemoryFailure
	}
	if state.OOMKilled {
		result.Error = "Docker container OOM-killed without a declared memory ceiling"
		if state.MemoryLimitBytes > 0 {
			result.Error = fmt.Sprintf("exceeded its %g GiB memory ceiling", float64(state.MemoryLimitBytes)/(1<<30))
		}
		result.Interrupted = false
		result.Forced = false
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
	if state.Released {
		if s.state.Result == nil {
			r := Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: s.state.Job.ID, Attempt: s.state.Job.Attempt}, Interrupted: true, Forced: state.Forced, Error: "worker restarted after Docker release"}
			s.state.Result = &r
		}
		return nil, nil
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

// ConfirmDockerGone records an operator's external fence/release proof while the
// worker is offline. It never asks an unresponsive daemon to prove its own fence.
func ConfirmDockerGone(hostDir, dir, identity string, confirmed bool) error {
	if dir == "" || identity == "" || filepath.Base(identity) != identity || !confirmed {
		return errors.New("resolve-docker requires --state-dir, --attempt-id and --confirm-gone after fencing delayed starts and verifying release")
	}
	var err error
	if hostDir == "" {
		hostDir, err = HostStateDirectory()
		if err != nil {
			return err
		}
	}
	if err = os.MkdirAll(hostDir, 0700); err != nil {
		return err
	}
	unlock, err := hostOperationLock(hostDir, "reconcile.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	s, err := (Worker{StateDir: dir}).openStore()
	if err != nil {
		return err
	}
	defer s.close()
	if s.state.Identity != identity || s.state.Job == nil || !s.state.Job.Spec.Run.Docker {
		return errors.New("Docker attempt does not match current worker custody")
	}
	data, err := os.ReadFile(filepath.Join(dir, identity+".docker.json"))
	if err != nil {
		return err
	}
	var state dockerExecution
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Identity != identity || state.Endpoint == "" {
		return errors.New("invalid Docker custody record")
	}
	state.Released = true
	state.Pending = false
	state.Stopped = true
	state.Forced = true
	state.OperatorConfirmed = true
	state.Error = "operator confirmed execution gone and delayed starts fenced"
	return saveWorkerJSON(dir, identity+".docker.json", state)
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
