package myce

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

// MemorySource reports available GPU memory in GiB. Sources must return a finite,
// nonnegative value, or an error; callers should never substitute a stale budget
// for a failed live query.
type MemorySource func(context.Context) (float64, error)

// NewMemorySource creates a memory source for cuda, xpu, metal, or fake.
// CUDA queries nvidia-smi on every call, sums device free memory, optionally caps
// it at a positive budget, then subtracts reserve (clamped at zero).
// XPU and Metal use an explicitly configured positive --memory-gb budget minus
// --reserve-memory-gb. This is an operator-selected conservative allowance, not
// live free-memory telemetry: it cannot observe allocations by other processes.
// In particular, host free RAM is not a reliable GPU budget for shared memory.
// Fake uses the configured budget minus reserve and needs no GPU or command.
func NewMemorySource(platform string, budget, reserve float64) (MemorySource, error) {
	return newMemorySourceWithCommand(platform, budget, reserve, func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, name, args...).Output()
	})
}

// Command injection is per source, so tests need neither GPU hardware nor a
// process-wide command override that could race with other workers.
func newMemorySourceWithCommand(platform string, budget, reserve float64, command func(context.Context, string, ...string) ([]byte, error)) (MemorySource, error) {
	if !validMemoryValue(budget) || !validMemoryValue(reserve) {
		return nil, fmt.Errorf("memory budget and reserve must be finite and nonnegative")
	}
	switch platform {
	case "cuda":
		if command == nil {
			return nil, fmt.Errorf("CUDA memory source requires a command runner")
		}
	case "xpu", "metal":
		if budget <= 0 {
			return nil, fmt.Errorf("%s memory source requires an explicit positive --memory-gb budget; no live GPU memory telemetry is available", platform)
		}
	case "fake":
	default:
		return nil, fmt.Errorf("unknown GPU memory platform %q", platform)
	}
	if (platform != "cuda" || budget > 0) && reserve > budget {
		return nil, fmt.Errorf("--reserve-memory-gb exceeds the configured --memory-gb budget")
	}
	return func(ctx context.Context) (float64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		available := budget
		if platform == "cuda" {
			out, err := command(ctx, "nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits")
			if err != nil {
				return 0, fmt.Errorf("query CUDA free memory with nvidia-smi: %w", err)
			}
			available, err = parseCUDAFreeMemory(out)
			if err != nil {
				return 0, err
			}
			if budget > 0 {
				available = min(available, budget)
			}
		}
		return max(0, available-reserve), nil
	}, nil
}

func validMemoryValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

func parseCUDAFreeMemory(output []byte) (float64, error) {
	data := strings.TrimSpace(string(output))
	if data == "" {
		return 0, fmt.Errorf("nvidia-smi returned no GPU free-memory data")
	}
	var total float64
	for i, line := range strings.Split(data, "\n") {
		mib, err := strconv.ParseFloat(strings.TrimSpace(line), 64)
		if err != nil || !validMemoryValue(mib) {
			return 0, fmt.Errorf("nvidia-smi returned invalid GPU free memory on row %d", i+1)
		}
		// nvidia-smi memory values are MiB, despite its unqualified unit name.
		total += mib / 1024
		if !validMemoryValue(total) {
			return 0, fmt.Errorf("nvidia-smi GPU free-memory sum is not finite")
		}
	}
	return total, nil
}
