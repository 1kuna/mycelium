package processadapter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Adapter struct {
	cfg       Config
	mu        sync.Mutex
	processes map[int]ProcessHandle
}

type ProcessRegistry interface {
	Add(ctx context.Context, ref domain.ProcessRef) error
	Remove(ctx context.Context, ref domain.ProcessRef) error
}

type ProcessHandle interface {
	PID() int
	Signal(os.Signal) error
	Kill() error
	Wait() error
}

type ProcessRunner interface {
	Start(ctx context.Context, binary string, args []string) (ProcessHandle, error)
}

type Config struct {
	Name            string
	BinaryPath      string
	Args            []string
	HealthPath      string
	PollInterval    time.Duration
	StopGracePeriod time.Duration
	HTTPClient      *http.Client
	Clock           ports.Clock
	ProcessRegistry ProcessRegistry
	ProcessRunner   ProcessRunner
}

func New(cfg Config) *Adapter {
	if cfg.Name == "" {
		cfg.Name = "process"
	}
	if cfg.HealthPath == "" {
		cfg.HealthPath = "/health"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.StopGracePeriod == 0 {
		cfg.StopGracePeriod = 2 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.System{}
	}
	if cfg.ProcessRunner == nil {
		cfg.ProcessRunner = execProcessRunner{}
	}
	return &Adapter{cfg: cfg, processes: map[int]ProcessHandle{}}
}

func (a *Adapter) Name() string {
	return a.cfg.Name
}

func (a *Adapter) Launch(ctx context.Context, preset domain.Preset, addr string) (ports.Handle, error) {
	if a.cfg.BinaryPath == "" {
		return ports.Handle{}, fmt.Errorf("%s backend binary path is required", a.cfg.Name)
	}
	args := append([]string(nil), a.cfg.Args...)
	args = append(args, preset.LaunchArgs...)
	rendered, err := renderArgs(args, preset, addr)
	if err != nil {
		return ports.Handle{}, err
	}
	if err := ctx.Err(); err != nil {
		return ports.Handle{}, err
	}
	process, err := a.cfg.ProcessRunner.Start(ctx, a.cfg.BinaryPath, rendered)
	if err != nil {
		return ports.Handle{}, err
	}
	select {
	case <-ctx.Done():
		_ = process.Kill()
		_ = process.Wait()
		return ports.Handle{}, ctx.Err()
	default:
	}
	a.mu.Lock()
	a.processes[process.PID()] = process
	a.mu.Unlock()
	ref := processRef(process.PID(), processGroupID(process.PID()), a.cfg.Name, a.cfg.BinaryPath, rendered, a.cfg.Clock.Now())
	if a.cfg.ProcessRegistry != nil {
		if err := a.cfg.ProcessRegistry.Add(ctx, ref); err != nil {
			_ = process.Kill()
			_ = process.Wait()
			a.mu.Lock()
			delete(a.processes, process.PID())
			a.mu.Unlock()
			return ports.Handle{}, err
		}
	}
	return handleFromProcessRef(ref, addr), nil
}

func (a *Adapter) WaitReady(ctx context.Context, addr string) error {
	if addr == "" {
		return fmt.Errorf("ready address is required")
	}
	url := "http://" + strings.TrimRight(addr, "/") + a.cfg.HealthPath
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := a.cfg.HTTPClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
		}
		timer := a.cfg.Clock.NewTimer(a.cfg.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C():
		}
	}
}

func (a *Adapter) Stop(ctx context.Context, handle ports.Handle) error {
	if handle.PID == 0 {
		return nil
	}
	a.mu.Lock()
	process := a.processes[handle.PID]
	a.mu.Unlock()
	if process == nil {
		stopped, err := a.stopStoredProcess(ctx, handle)
		if stopped {
			err = errors.Join(err, a.removeProcessRef(context.Background(), handle))
		}
		return err
	}
	stopped, err := a.stopProcess(ctx, process)
	if stopped {
		a.mu.Lock()
		delete(a.processes, handle.PID)
		a.mu.Unlock()
		err = errors.Join(err, a.removeProcessRef(context.Background(), handle))
	}
	return err
}

func (a *Adapter) stopProcess(ctx context.Context, process ProcessHandle) (bool, error) {
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	if err := process.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		if killErr := process.Kill(); killErr != nil {
			if errors.Is(killErr, os.ErrProcessDone) || errors.Is(killErr, syscall.ESRCH) {
				return true, nil
			}
			return false, killErr
		}
		select {
		case <-ctx.Done():
			waitErr := <-done
			if waitErr != nil {
				return true, waitErr
			}
			return true, ctx.Err()
		case waitErr := <-done:
			return true, waitErr
		}
	}
	timer := a.cfg.Clock.NewTimer(a.cfg.StopGracePeriod)
	select {
	case <-ctx.Done():
		timer.Stop()
		if err := process.Kill(); err != nil {
			return false, err
		}
		waitErr := <-done
		if waitErr != nil {
			return true, waitErr
		}
		return true, ctx.Err()
	case <-timer.C():
		if err := process.Kill(); err != nil {
			return false, err
		}
		select {
		case <-ctx.Done():
			waitErr := <-done
			if waitErr != nil {
				return true, waitErr
			}
			return true, ctx.Err()
		case waitErr := <-done:
			return true, waitErr
		}
	case waitErr := <-done:
		timer.Stop()
		return true, waitErr
	}
}

func (a *Adapter) removeProcessRef(ctx context.Context, handle ports.Handle) error {
	if a.cfg.ProcessRegistry == nil || handle.PID == 0 {
		return nil
	}
	return a.cfg.ProcessRegistry.Remove(ctx, domain.ProcessRef{PID: handle.PID, PGID: handle.PGID, Kind: handle.Kind, Ref: handle.Ref, Binary: handle.Binary, Args: append([]string(nil), handle.Args...), StartedAt: handle.StartedAt})
}

func processRef(pid, pgid int, kind, binary string, args []string, startedAt time.Time) domain.ProcessRef {
	return domain.ProcessRef{PID: pid, PGID: pgid, Kind: kind, Ref: fmt.Sprintf("%d", pid), Binary: binary, Args: append([]string(nil), args...), StartedAt: startedAt.UTC()}
}

func handleFromProcessRef(ref domain.ProcessRef, addr string) ports.Handle {
	return ports.Handle{PID: ref.PID, PGID: ref.PGID, Addr: addr, Kind: ref.Kind, Ref: ref.Ref, Binary: ref.Binary, Args: append([]string(nil), ref.Args...), StartedAt: ref.StartedAt}
}

func (a *Adapter) stopStoredProcess(ctx context.Context, handle ports.Handle) (bool, error) {
	process, err := os.FindProcess(handle.PID)
	if err != nil {
		return false, err
	}
	if err := verifyProcessIdentity(handle); err != nil {
		return false, err
	}
	if err := signalHandle(handle, process, syscall.SIGTERM); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		if killErr := killHandle(handle, process); killErr != nil {
			if errors.Is(killErr, os.ErrProcessDone) || errors.Is(killErr, syscall.ESRCH) {
				return true, nil
			}
			return false, killErr
		}
		return a.waitForExternalExit(ctx, handle, process)
	}
	timer := a.cfg.Clock.NewTimer(a.cfg.StopGracePeriod)
	for {
		if exited, err := processExited(process); exited || err != nil {
			timer.Stop()
			return exited, err
		}
		poll := a.cfg.Clock.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			poll.Stop()
			timer.Stop()
			if err := killHandle(handle, process); err != nil {
				return false, err
			}
			stopped, waitErr := a.waitForExternalExit(context.Background(), handle, process)
			if waitErr != nil {
				return stopped, waitErr
			}
			return stopped, ctx.Err()
		case <-timer.C():
			poll.Stop()
			if err := killHandle(handle, process); err != nil {
				return false, err
			}
			return a.waitForExternalExit(context.Background(), handle, process)
		case <-poll.C():
		}
	}
}

func (a *Adapter) waitForExternalExit(ctx context.Context, handle ports.Handle, process *os.Process) (bool, error) {
	timer := a.cfg.Clock.NewTimer(a.cfg.StopGracePeriod)
	for {
		if exited, err := processExited(process); exited || err != nil {
			timer.Stop()
			return exited, err
		}
		poll := a.cfg.Clock.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			poll.Stop()
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C():
			poll.Stop()
			return false, fmt.Errorf("process %d did not exit after signal", handle.PID)
		case <-poll.C():
		}
	}
}

func renderArgs(args []string, preset domain.Preset, addr string) ([]string, error) {
	host, port, ok := strings.Cut(addr, ":")
	if !ok || host == "" || port == "" {
		return nil, fmt.Errorf("backend addr must be host:port, got %q", addr)
	}
	out := make([]string, 0, len(args))
	replacer := strings.NewReplacer(
		"{model}", preset.ModelRef,
		"{preset}", preset.ID,
		"{host}", host,
		"{port}", port,
		"{addr}", addr,
	)
	for _, arg := range args {
		out = append(out, replacer.Replace(arg))
	}
	return out, nil
}

type execProcessRunner struct{}

func (execProcessRunner) Start(_ context.Context, binary string, args []string) (ProcessHandle, error) {
	cmd := exec.Command(binary, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return execProcess{cmd: cmd}, nil
}

type execProcess struct {
	cmd *exec.Cmd
}

func (p execProcess) PID() int {
	return p.cmd.Process.Pid
}

func (p execProcess) Signal(sig os.Signal) error {
	return p.cmd.Process.Signal(sig)
}

func (p execProcess) Kill() error {
	return p.cmd.Process.Kill()
}

func (p execProcess) Wait() error {
	return p.cmd.Wait()
}

type osProcessHandle struct {
	process *os.Process
}

func (p osProcessHandle) PID() int {
	return p.process.Pid
}

func (p osProcessHandle) Signal(sig os.Signal) error {
	return p.process.Signal(sig)
}

func (p osProcessHandle) Kill() error {
	return p.process.Kill()
}

func (p osProcessHandle) Wait() error {
	_, err := p.process.Wait()
	return err
}

func processGroupID(pid int) int {
	if pid <= 0 {
		return 0
	}
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		return 0
	}
	return pgid
}

func verifyProcessIdentity(handle ports.Handle) error {
	if handle.PID <= 0 {
		return fmt.Errorf("process pid is required")
	}
	if handle.PGID == 0 {
		return nil
	}
	pgid, err := syscall.Getpgid(handle.PID)
	if err != nil {
		return err
	}
	if pgid != handle.PGID {
		return fmt.Errorf("process %d pgid changed from %d to %d", handle.PID, handle.PGID, pgid)
	}
	return nil
}

func signalHandle(handle ports.Handle, process *os.Process, sig syscall.Signal) error {
	if safeProcessGroup(handle.PGID) {
		return syscall.Kill(-handle.PGID, sig)
	}
	return process.Signal(sig)
}

func killHandle(handle ports.Handle, process *os.Process) error {
	if safeProcessGroup(handle.PGID) {
		return syscall.Kill(-handle.PGID, syscall.SIGKILL)
	}
	return process.Kill()
}

func safeProcessGroup(pgid int) bool {
	return pgid > 1 && pgid != syscall.Getpgrp()
}

func processExited(process *os.Process) (bool, error) {
	if err := process.Signal(syscall.Signal(0)); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

var _ ports.BackendAdapter = (*Adapter)(nil)
