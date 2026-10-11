package myce

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	Identity          string     `json:"identity"`
	Endpoint          string     `json:"endpoint"`
	Container         string     `json:"container,omitempty"`
	Operation         string     `json:"operation,omitempty"`
	Pending           bool       `json:"pending,omitempty"`
	Stopped           bool       `json:"stopped,omitempty"`
	Forced            bool       `json:"forced,omitempty"`
	StopAt            *time.Time `json:"stop_at,omitempty"`
	OperatorConfirmed bool       `json:"operator_confirmed,omitempty"`
	Released          bool       `json:"released,omitempty"`
	Error             string     `json:"error,omitempty"`
	ExitCode          int        `json:"exit_code"`
}
type dockerAttempt struct {
	dirty       bool
	closed      chan struct{}
	halt        chan struct{}
	haltOnce    sync.Once
	mu          sync.Mutex
	state       dockerExecution
	dir, binary string
	workdir     string
	env         []string
	clock       Clock
	timeout     time.Duration
}

func (d *dockerAttempt) saveLocked() error {
	err := saveWorkerJSON(d.dir, d.state.Identity+".docker.json", d.state)
	d.dirty = err != nil
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
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, d.binary, append([]string{"--host", d.state.Endpoint}, args...)...)
	cmd.Env = d.env
	cmd.Dir = d.workdir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return signalWorkerGroup(cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = d.timeout
	out, err := cmd.Output()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return out, err
}
func (d *dockerAttempt) operation(kind string, args ...string) ([]byte, error) {
	d.mu.Lock()
	if d.state.Stopped {
		d.mu.Unlock()
		return nil, errors.New("Docker stop recorded before dispatch")
	}
	d.state.Operation = kind
	d.state.Pending = true
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
	ID     string                             `json:"Id"`
	Config struct{ Labels map[string]string } `json:"Config"`
	State  struct {
		Running  *bool
		Status   string
		ExitCode int
	} `json:"State"`
}

// Every object is inspected before signaling: names and cidfiles are not ownership proof.
func (d *dockerAttempt) containers() ([]dockerInspection, error) {
	out, err := d.call("ps", "--all", "--quiet", "--no-trunc", "--filter", "label=myce.attempt="+d.state.Identity)
	if err != nil {
		return nil, err
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return nil, nil
	}
	out, err = d.call(append([]string{"inspect"}, ids...)...)
	if err != nil {
		return nil, err
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
		if c.Config.Labels["myce.attempt"] != d.state.Identity {
			return nil, errors.New("Docker ownership mismatch")
		}
	}
	return containers, nil
}
func (d *dockerAttempt) reconcile() (bool, error) {
	d.mu.Lock()
	if d.dirty {
		if err := d.saveLocked(); err != nil {
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
			if c.ID == d.state.Container && *c.State.Running {
				d.state.Pending = false
				d.state.Stopped = true
				d.state.Forced = true
				d.state.Error = ""
				if err = d.saveLocked(); err != nil {
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
		if err = d.saveLocked(); err != nil {
			d.mu.Unlock()
			return false, err
		}
	}
	d.mu.Unlock()
	running := false
	for _, c := range containers {
		if *c.State.Running {
			running = true
			if stopped || pending {
				signal := "--signal=TERM"
				if forced || pending {
					signal = "--signal=KILL"
				}
				if _, err = d.call("kill", signal, c.ID); err != nil {
					return false, err
				}
			}
		}
	}
	if running || pending {
		return false, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// A final successful inventory after a definite start response is the release proof.
	if len(containers) > 0 {
		d.state.ExitCode = containers[0].State.ExitCode
	}
	d.state.Released = true
	d.state.Error = ""
	if err := d.saveLocked(); err != nil {
		d.state.Released = false
		return false, err
	}
	return true, nil
}
func (w Worker) startDocker(s *workerStore, j Job, checkpoint string, release func()) (*process, error) {
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
	identity := ID()
	endpoint := os.Getenv("DOCKER_HOST")
	if endpoint == "" {
		endpoint = "unix:///var/run/docker.sock"
	}
	if os.Getenv("DOCKER_CONTEXT") != "" {
		return nil, errors.New("managed Docker requires DOCKER_HOST, not DOCKER_CONTEXT")
	}
	d := &dockerAttempt{closed: make(chan struct{}), halt: make(chan struct{}), dir: s.dir, workdir: j.Spec.Run.Workdir, binary: binary, clock: w.Clock, timeout: w.dockerCallTimeout, state: dockerExecution{Identity: identity, Endpoint: endpoint}}
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
	s.state.Result = nil
	s.state.PID = 0
	s.state.Offset = 0
	s.state.Pending = 0
	output, err := os.OpenFile(filepath.Join(s.dir, s.state.Log), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
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
		out, e := d.operation("create", create...)
		if e != nil {
			return e
		}
		id := strings.TrimSpace(string(out))
		if id == "" || strings.ContainsAny(id, " \n\t") {
			d.mu.Lock()
			d.state.Pending = true
			d.state.Error = "invalid Docker create receipt"
			_ = d.saveLocked()
			d.mu.Unlock()
			return errors.New("invalid Docker create receipt")
		}
		d.mu.Lock()
		d.state.Container = id
		e = d.saveLocked()
		d.mu.Unlock()
		if e != nil {
			return e
		}
		_, e = d.operation("start", "start", id)
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
	d.mu.Lock()
	result.Interrupted = d.state.Stopped
	result.Forced = d.state.Forced
	result.Success = e == nil && d.state.ExitCode == 0
	container := d.state.Container
	d.mu.Unlock()
	if e != nil {
		result.Error = "Docker launch failed"
	}
	if container != "" {
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
	syncErr := output.Sync()
	closeErr := output.Close()
	if syncErr != nil || closeErr != nil {
		result.Success = false
		result.Error = "persist Docker output failed"
	}
	err := saveWorkerJSON(d.dir, d.state.Identity+".result.json", result)
	p.done <- workerFinish{result: result, err: err}
}
func (d *dockerAttempt) close() { d.haltOnce.Do(func() { close(d.halt) }); <-d.closed }

func (w Worker) restoreDocker(s *workerStore) (*process, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker.json"))
	if err != nil {
		return nil, err
	}
	var state dockerExecution
	if err = json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Identity != s.state.Identity || state.Endpoint == "" {
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
	d.stop(w.Clock.Now(), 0, true)
	output, err := os.OpenFile(filepath.Join(s.dir, s.state.Log), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	p := &process{job: *s.state.Job, machine: w.Machine.ID, done: make(chan workerFinish, 1), docker: d}
	go d.run(p, *s.state.Job, s.state.Session, output, nil, func() error { return errors.New("worker restarted; Docker cleanup required") })
	return p, nil
}

// ConfirmDockerGone records an operator's external fence/release proof while the
// worker is offline. It never asks an unresponsive daemon to prove its own fence.
func ConfirmDockerGone(dir, identity string, confirmed bool) error {
	if dir == "" || identity == "" || filepath.Base(identity) != identity || !confirmed {
		return errors.New("resolve-docker requires --state-dir, --attempt-id and --confirm-gone after fencing delayed starts and verifying release")
	}
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
