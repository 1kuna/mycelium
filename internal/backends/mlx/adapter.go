package mlx

import (
	"time"

	"mycelium/internal/backends/processadapter"
)

type Config struct {
	BinaryPath      string
	ProcessRegistry processadapter.ProcessRegistry
}

func NewAdapter(binaryPath string) *processadapter.Adapter {
	return NewAdapterWithConfig(Config{BinaryPath: binaryPath})
}

func NewAdapterWithConfig(cfg Config) *processadapter.Adapter {
	return processadapter.New(processadapter.Config{
		Name:            "mlx",
		BinaryPath:      cfg.BinaryPath,
		Args:            []string{"server", "--model", "{model}", "--host", "{host}", "--port", "{port}"},
		HealthPath:      "/health",
		PollInterval:    250 * time.Millisecond,
		ProcessRegistry: cfg.ProcessRegistry,
	})
}
