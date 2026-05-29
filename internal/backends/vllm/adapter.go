package vllm

import (
	"time"

	"mycelium/internal/backends/processadapter"
)

func NewAdapter(binaryPath string) *processadapter.Adapter {
	return processadapter.New(processadapter.Config{
		Name:         "vllm",
		BinaryPath:   binaryPath,
		Args:         []string{"serve", "{model}", "--host", "{host}", "--port", "{port}"},
		HealthPath:   "/health",
		PollInterval: 250 * time.Millisecond,
	})
}
