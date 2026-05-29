package llamacpp

import (
	"context"
	"fmt"
	"net/http"
	"os"
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

type Config struct {
	BinaryPath   string
	Args         []string
	HealthPath   string
	PollInterval time.Duration
	HTTPClient   *http.Client
	Clock        ports.Clock
}

func DefaultConfig() Config {
	return Config{
		BinaryPath:   "llama-server",
		Args:         []string{"--host", "{host}", "--port", "{port}", "-m", "{model}", "-c", "{ctx}"},
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
	args := renderArgs(a.cfg.Args, p, addr)
	cmd := exec.CommandContext(ctx, a.cfg.BinaryPath, args...)
	if err := cmd.Start(); err != nil {
		return ports.Handle{}, err
	}
	a.mu.Lock()
	a.processes[cmd.Process.Pid] = cmd
	a.mu.Unlock()
	return ports.Handle{PID: cmd.Process.Pid, Addr: addr, Kind: "process", Ref: fmt.Sprintf("%d", cmd.Process.Pid)}, nil
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
		return ctx.Err()
	case <-done:
		return nil
	}
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
