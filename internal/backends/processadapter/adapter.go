package processadapter

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type Adapter struct {
	cfg       Config
	mu        sync.Mutex
	processes map[int]*exec.Cmd
}

type ProcessRegistry interface {
	Add(ctx context.Context, ref domain.ProcessRef) error
	Remove(ctx context.Context, ref domain.ProcessRef) error
}

type Config struct {
	Name            string
	BinaryPath      string
	Args            []string
	HealthPath      string
	PollInterval    time.Duration
	HTTPClient      *http.Client
	Clock           ports.Clock
	ProcessRegistry ProcessRegistry
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
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.System{}
	}
	return &Adapter{cfg: cfg, processes: map[int]*exec.Cmd{}}
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
	cmd := exec.Command(a.cfg.BinaryPath, rendered...)
	if err := cmd.Start(); err != nil {
		return ports.Handle{}, err
	}
	select {
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return ports.Handle{}, ctx.Err()
	default:
	}
	a.mu.Lock()
	a.processes[cmd.Process.Pid] = cmd
	a.mu.Unlock()
	ref := domain.ProcessRef{PID: cmd.Process.Pid, Kind: "process", Ref: fmt.Sprintf("%d", cmd.Process.Pid)}
	if a.cfg.ProcessRegistry != nil {
		if err := a.cfg.ProcessRegistry.Add(ctx, ref); err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			a.mu.Lock()
			delete(a.processes, cmd.Process.Pid)
			a.mu.Unlock()
			return ports.Handle{}, err
		}
	}
	return ports.Handle{PID: cmd.Process.Pid, Addr: addr, Kind: ref.Kind, Ref: ref.Ref}, nil
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
	defer a.removeProcessRef(ctx, handle)
	a.mu.Lock()
	cmd := a.processes[handle.PID]
	delete(a.processes, handle.PID)
	a.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := cmd.Process.Kill(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (a *Adapter) removeProcessRef(ctx context.Context, handle ports.Handle) {
	if a.cfg.ProcessRegistry == nil || handle.PID == 0 {
		return
	}
	_ = a.cfg.ProcessRegistry.Remove(ctx, domain.ProcessRef{PID: handle.PID, Kind: handle.Kind, Ref: handle.Ref})
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

var _ ports.BackendAdapter = (*Adapter)(nil)
