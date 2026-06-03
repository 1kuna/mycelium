package mlx

import (
	"net/http"
	"time"

	"mycelium/internal/backends/processadapter"
	"mycelium/internal/ports"
)

type Config struct {
	BinaryPath      string
	Args            []string
	ProcessRegistry processadapter.ProcessRegistry
	ProcessRunner   processadapter.ProcessRunner
	HTTPClient      *http.Client
	Clock           ports.Clock
	PollInterval    time.Duration
	StopGracePeriod time.Duration
}

func NewAdapter(binaryPath string) *processadapter.Adapter {
	return NewAdapterWithConfig(Config{BinaryPath: binaryPath})
}

func NewAdapterWithConfig(cfg Config) *processadapter.Adapter {
	args := append([]string{"--model", "{model}", "--host", "{host}", "--port", "{port}"}, cfg.Args...)
	return processadapter.New(processadapter.Config{
		Name:            "mlx",
		BinaryPath:      cfg.BinaryPath,
		Args:            args,
		HealthPath:      "/health",
		PollInterval:    cfg.PollInterval,
		StopGracePeriod: cfg.StopGracePeriod,
		HTTPClient:      cfg.HTTPClient,
		Clock:           cfg.Clock,
		ProcessRegistry: cfg.ProcessRegistry,
		ProcessRunner:   cfg.ProcessRunner,
	})
}
