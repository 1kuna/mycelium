package mlx

import (
	"time"

	"mycelium/internal/backends/processadapter"
)

func NewAdapter(binaryPath string) *processadapter.Adapter {
	return processadapter.New(processadapter.Config{
		Name:         "mlx",
		BinaryPath:   binaryPath,
		Args:         []string{"server", "--model", "{model}", "--host", "{host}", "--port", "{port}"},
		HealthPath:   "/health",
		PollInterval: 250 * time.Millisecond,
	})
}
