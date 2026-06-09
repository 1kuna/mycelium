package llamacpp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"mycelium/internal/backends/processidentity"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type ProcessRegistry interface {
	Add(ctx context.Context, ref domain.ProcessRef) error
	Remove(ctx context.Context, ref domain.ProcessRef) error
}

type Adapter struct {
	cfg       Config
	mu        sync.Mutex
	processes map[int]ProcessHandle
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
	BinaryPath      string
	Args            []string
	LaunchProfiles  map[string][]string
	HealthPath      string
	PollInterval    time.Duration
	StopGracePeriod time.Duration
	HTTPClient      *http.Client
	Clock           ports.Clock
	ProcessRegistry ProcessRegistry
	ProcessRunner   ProcessRunner
}

func DefaultConfig() Config {
	return Config{
		BinaryPath: "llama-server",
		Args:       []string{"--host", "{host}", "--port", "{port}", "-m", "{model}", "-c", "{ctx}", "--parallel", "1"},
		LaunchProfiles: map[string][]string{
			"llamacpp-cuda":  nil,
			"llamacpp-metal": nil,
		},
		HealthPath:      "/health",
		PollInterval:    250 * time.Millisecond,
		StopGracePeriod: 2 * time.Second,
		HTTPClient:      http.DefaultClient,
		Clock:           clock.System{},
		ProcessRunner:   execProcessRunner{},
	}
}

func NewAdapter(cfg Config) *Adapter {
	def := DefaultConfig()
	if cfg.BinaryPath == "" {
		cfg.BinaryPath = def.BinaryPath
	}
	if cfg.Args == nil {
		cfg.Args = def.Args
	}
	if cfg.LaunchProfiles == nil {
		cfg.LaunchProfiles = def.LaunchProfiles
	}
	if cfg.HealthPath == "" {
		cfg.HealthPath = def.HealthPath
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = def.PollInterval
	}
	if cfg.StopGracePeriod == 0 {
		cfg.StopGracePeriod = def.StopGracePeriod
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = def.HTTPClient
	}
	if cfg.Clock == nil {
		cfg.Clock = def.Clock
	}
	if cfg.ProcessRunner == nil {
		cfg.ProcessRunner = def.ProcessRunner
	}
	return &Adapter{cfg: cfg, processes: map[int]ProcessHandle{}}
}

func (a *Adapter) Name() string {
	return "llamacpp"
}

func (a *Adapter) Launch(ctx context.Context, p domain.Preset, addr string) (ports.Handle, error) {
	return a.launchAt(ctx, p, addr)
}

func (a *Adapter) LaunchDynamic(ctx context.Context, p domain.Preset, addr string) (ports.Handle, error) {
	concrete, err := reserveDynamicAddr(addr)
	if err != nil {
		return ports.Handle{}, err
	}
	return a.launchAt(ctx, p, concrete)
}

func (a *Adapter) launchAt(ctx context.Context, p domain.Preset, addr string) (ports.Handle, error) {
	args, err := a.renderLaunchArgs(p, addr)
	if err != nil {
		return ports.Handle{}, err
	}
	if err := ctx.Err(); err != nil {
		return ports.Handle{}, err
	}
	process, err := a.cfg.ProcessRunner.Start(ctx, a.cfg.BinaryPath, args)
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
	ref := processRef(process.PID(), processGroupID(process.PID()), "llamacpp", a.cfg.BinaryPath, args, a.cfg.Clock.Now())
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

func reserveDynamicAddr(addr string) (string, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if port != "0" {
		return addr, nil
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	concrete := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return concrete, nil
}

func (a *Adapter) renderLaunchArgs(p domain.Preset, addr string) ([]string, error) {
	args := append([]string(nil), a.cfg.Args...)
	if p.LaunchProfile != "" {
		profileArgs, ok := a.cfg.LaunchProfiles[p.LaunchProfile]
		if !ok {
			return nil, fmt.Errorf("unknown llama.cpp launch profile %q", p.LaunchProfile)
		}
		args = append(args, profileArgs...)
	}
	args = append(args, p.LaunchArgs...)
	return renderArgs(args, p, addr), nil
}

func (a *Adapter) WaitReady(ctx context.Context, addr string) error {
	url := healthURL(addr, a.cfg.HealthPath)
	for {
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

func (a *Adapter) Stop(ctx context.Context, h ports.Handle) error {
	if h.PID == 0 {
		return nil
	}
	a.mu.Lock()
	process := a.processes[h.PID]
	a.mu.Unlock()
	if process == nil {
		stopped, err := a.stopStoredProcess(ctx, h)
		if stopped {
			err = errors.Join(err, a.removeProcessRef(context.Background(), h))
		}
		return err
	}
	stopped, err := a.stopProcess(ctx, process)
	if stopped {
		a.mu.Lock()
		delete(a.processes, h.PID)
		a.mu.Unlock()
		err = errors.Join(err, a.removeProcessRef(context.Background(), h))
	}
	return err
}

func (a *Adapter) stopProcess(ctx context.Context, process ProcessHandle) (bool, error) {
	done := make(chan error, 1)
	go func() {
		done <- process.Wait()
	}()
	if err := process.Signal(os.Interrupt); err != nil {
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
			if waitErr != nil && !expectedStopSignalExit(waitErr) {
				return true, waitErr
			}
			return true, ctx.Err()
		case waitErr := <-done:
			return true, cleanStopWaitError(waitErr)
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
		if waitErr != nil && !expectedStopSignalExit(waitErr) {
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
			if waitErr != nil && !expectedStopSignalExit(waitErr) {
				return true, waitErr
			}
			return true, ctx.Err()
		case waitErr := <-done:
			return true, cleanStopWaitError(waitErr)
		}
	case waitErr := <-done:
		timer.Stop()
		return true, cleanStopWaitError(waitErr)
	}
}

func cleanStopWaitError(err error) error {
	if expectedStopSignalExit(err) {
		return nil
	}
	return err
}

func expectedStopSignalExit(err error) bool {
	if err == nil {
		return false
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		return false
	}
	if status.Exited() {
		switch status.ExitStatus() {
		case 128 + int(syscall.SIGTERM), 128 + int(syscall.SIGINT), 128 + int(syscall.SIGKILL):
			return true
		default:
			return false
		}
	}
	if !status.Signaled() {
		return false
	}
	switch status.Signal() {
	case syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL:
		return true
	default:
		return false
	}
}

func (a *Adapter) removeProcessRef(ctx context.Context, h ports.Handle) error {
	if a.cfg.ProcessRegistry == nil {
		return nil
	}
	return a.cfg.ProcessRegistry.Remove(ctx, domain.ProcessRef{PID: h.PID, PGID: h.PGID, Kind: h.Kind, Ref: h.Ref, Binary: h.Binary, Args: append([]string(nil), h.Args...), StartedAt: h.StartedAt})
}

func (a *Adapter) stopStoredProcess(ctx context.Context, handle ports.Handle) (bool, error) {
	process, err := os.FindProcess(handle.PID)
	if err != nil {
		return false, err
	}
	if exited, err := processidentity.Exited(handle.PID); exited || err != nil {
		if stopped, classErr, ok := classifyPermissionStopError(handle, err); ok {
			return stopped, classErr
		}
		return exited, err
	}
	if err := verifyProcessIdentity(handle); err != nil {
		if processidentity.IsExited(err) {
			return true, nil
		}
		return false, err
	}
	if err := signalHandle(handle, process, syscall.SIGINT); err != nil {
		if stopped, classErr, ok := classifyPermissionStopError(handle, err); ok {
			return stopped, classErr
		}
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return true, nil
		}
		if killErr := killHandle(handle, process); killErr != nil {
			if stopped, classErr, ok := classifyPermissionStopError(handle, killErr); ok {
				return stopped, classErr
			}
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
				if stopped, classErr, ok := classifyPermissionStopError(handle, err); ok {
					return stopped, classErr
				}
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
				if stopped, classErr, ok := classifyPermissionStopError(handle, err); ok {
					return stopped, classErr
				}
				return false, err
			}
			return a.waitForExternalExit(context.Background(), handle, process)
		case <-poll.C():
		}
	}
}

func classifyPermissionError(handle ports.Handle, err error) (bool, error) {
	stopped, classErr, _ := classifyPermissionStopError(handle, err)
	return stopped, classErr
}

func classifyPermissionStopError(handle ports.Handle, err error) (bool, error, bool) {
	return processidentity.ClassifyPermissionError(handle, err)
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

func processRef(pid, pgid int, kind, binary string, args []string, startedAt time.Time) domain.ProcessRef {
	return domain.ProcessRef{PID: pid, PGID: pgid, Kind: kind, Ref: fmt.Sprintf("%d", pid), Binary: binary, Args: append([]string(nil), args...), StartedAt: startedAt.UTC()}
}

func handleFromProcessRef(ref domain.ProcessRef, addr string) ports.Handle {
	return ports.Handle{PID: ref.PID, PGID: ref.PGID, Addr: addr, Kind: ref.Kind, Ref: ref.Ref, Binary: ref.Binary, Args: append([]string(nil), ref.Args...), StartedAt: ref.StartedAt}
}

func signalPID(pid int) error {
	if pid <= 0 {
		return nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := proc.Signal(os.Interrupt); err != nil {
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	return nil
}

func renderArgs(args []string, p domain.Preset, addr string) []string {
	host, port := splitAddr(addr)
	replacer := strings.NewReplacer(
		"{addr}", addr,
		"{host}", host,
		"{port}", port,
		"{model}", p.ModelRef,
		"{preset}", p.ID,
		"{ctx}", fmt.Sprintf("%d", p.ContextLength),
	)
	out := make([]string, len(args))
	for i, arg := range args {
		out[i] = replacer.Replace(arg)
	}
	return out
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
	return processidentity.Verify(handle)
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

func splitAddr(addr string) (host, port string) {
	lastColon := strings.LastIndex(addr, ":")
	if lastColon < 0 {
		return addr, ""
	}
	return addr[:lastColon], addr[lastColon+1:]
}

func healthURL(addr, path string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/") + path
	}
	return "http://" + addr + path
}

var _ ports.BackendAdapter = (*Adapter)(nil)
var _ ports.DynamicBackendAdapter = (*Adapter)(nil)
