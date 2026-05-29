package llamacpp

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

type ProcessRegistry interface {
	Add(ctx context.Context, ref domain.ProcessRef) error
	Remove(ctx context.Context, ref domain.ProcessRef) error
}

type Adapter struct {
	cfg       Config
	mu        sync.Mutex
	processes map[int]*exec.Cmd
}

type Config struct {
	BinaryPath      string
	Args            []string
	LaunchProfiles  map[string][]string
	HealthPath      string
	PollInterval    time.Duration
	HTTPClient      *http.Client
	Clock           ports.Clock
	ProcessRegistry ProcessRegistry
}

func DefaultConfig() Config {
	return Config{
		BinaryPath: "llama-server",
		Args:       []string{"--host", "{host}", "--port", "{port}", "-m", "{model}", "-c", "{ctx}"},
		LaunchProfiles: map[string][]string{
			"llamacpp-cuda":  nil,
			"llamacpp-metal": nil,
		},
		HealthPath:   "/health",
		PollInterval: 250 * time.Millisecond,
		HTTPClient:   http.DefaultClient,
		Clock:        clock.System{},
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
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = def.HTTPClient
	}
	if cfg.Clock == nil {
		cfg.Clock = def.Clock
	}
	return &Adapter{cfg: cfg, processes: map[int]*exec.Cmd{}}
}

func (a *Adapter) Name() string {
	return "llamacpp"
}

func (a *Adapter) Launch(ctx context.Context, p domain.Preset, addr string) (ports.Handle, error) {
	args, err := a.renderLaunchArgs(p, addr)
	if err != nil {
		return ports.Handle{}, err
	}
	if err := ctx.Err(); err != nil {
		return ports.Handle{}, err
	}
	cmd := exec.Command(a.cfg.BinaryPath, args...)
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
	a.mu.Lock()
	cmd := a.processes[h.PID]
	delete(a.processes, h.PID)
	a.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		if err := signalPID(h.PID); err != nil {
			return err
		}
		a.removeProcessRef(ctx, h)
		return nil
	}

	_ = cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-done
		a.removeProcessRef(context.Background(), h)
		return ctx.Err()
	case <-done:
		a.removeProcessRef(ctx, h)
		return nil
	}
}

func (a *Adapter) removeProcessRef(ctx context.Context, h ports.Handle) {
	if a.cfg.ProcessRegistry == nil {
		return
	}
	_ = a.cfg.ProcessRegistry.Remove(ctx, domain.ProcessRef{PID: h.PID, Kind: h.Kind, Ref: h.Ref})
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
