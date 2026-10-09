package myce

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// MemorySource reports available GPU memory in GiB. Sources must return a finite,
// nonnegative value, or an error; callers should never substitute a stale budget
// for a failed live query.
type MemorySource func(context.Context) (float64, error)

// NewMemorySource uses automatic unified-memory detection.
func NewMemorySource(platform string, budget, reserve float64) (MemorySource, error) {
	return NewMemorySourceWithMode(platform, budget, reserve, "auto")
}

// NewMemorySourceWithMode supports auto, unified, dedicated, and fixed.
// Live sources are capped by budget (if positive), then reserve is subtracted.
// Fixed is an explicit operator allowance, never an implicit probe fallback.
func NewMemorySourceWithMode(platform string, budget, reserve float64, mode string) (MemorySource, error) {
	return newMemorySource(platform, budget, reserve, mode, runtime.GOOS, os.ReadFile,
		func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, name, args...).Output()
		})
}

func newMemorySourceWithCommand(platform string, budget, reserve float64, command func(context.Context, string, ...string) ([]byte, error)) (MemorySource, error) {
	return newMemorySource(platform, budget, reserve, "auto", runtime.GOOS, os.ReadFile, command)
}

func newMemorySource(platform string, budget, reserve float64, mode, goos string, readFile func(string) ([]byte, error), command func(context.Context, string, ...string) ([]byte, error)) (MemorySource, error) {
	if !validMemoryValue(budget) || !validMemoryValue(reserve) {
		return nil, fmt.Errorf("memory budget and reserve must be finite and nonnegative")
	}
	if mode != "auto" && mode != "unified" && mode != "dedicated" && mode != "fixed" {
		return nil, fmt.Errorf("unknown memory mode %q", mode)
	}
	if platform != "cuda" && platform != "metal" && platform != "xpu" && platform != "fake" {
		return nil, fmt.Errorf("unknown GPU memory platform %q", platform)
	}
	if platform == "metal" && mode == "dedicated" {
		return nil, fmt.Errorf("metal always uses unified memory; use auto, unified or fixed")
	}
	fixed := mode == "fixed" || platform == "fake" || (platform == "xpu" && mode != "unified")
	if fixed && budget <= 0 && platform != "fake" {
		return nil, fmt.Errorf("fixed memory source requires a positive --memory-gb budget")
	}
	if (fixed || budget > 0) && reserve > budget {
		return nil, fmt.Errorf("--reserve-memory-gb exceeds the configured --memory-gb budget")
	}
	if !fixed && command == nil {
		return nil, fmt.Errorf("live memory source requires a command runner")
	}
	return func(ctx context.Context) (float64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		available := budget
		if !fixed {
			probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			unified := mode == "unified" || platform == "metal"
			if !unified {
				out, err := command(probeCtx, "nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits")
				if err != nil {
					return 0, fmt.Errorf("query CUDA free memory with nvidia-smi: %w", err)
				}
				available, err = parseCUDAFreeMemory(out)
				if errors.Is(err, errUnifiedMemory) && mode == "auto" {
					unified = true
				} else if err != nil {
					return 0, err
				}
			}
			if unified {
				var err error
				available, err = hostAvailableMemory(probeCtx, goos, readFile, command)
				if err != nil {
					return 0, fmt.Errorf("unified memory unavailable: %w; refusing work (explicit --memory-mode fixed --memory-gb can configure an operator budget)", err)
				}
			}
			if budget > 0 {
				available = min(available, budget)
			}
		}
		return max(0, available-reserve), nil
	}, nil
}

var errUnifiedMemory = errors.New("nvidia-smi memory fields unsupported (unified memory)")

func hostAvailableMemory(ctx context.Context, goos string, readFile func(string) ([]byte, error), command func(context.Context, string, ...string) ([]byte, error)) (float64, error) {
	switch goos {
	case "linux":
		if readFile == nil {
			return 0, errors.New("/proc/meminfo reader unavailable")
		}
		out, err := readFile("/proc/meminfo")
		if err != nil {
			return 0, fmt.Errorf("read /proc/meminfo: %w", err)
		}
		return parseMemAvailable(out)
	case "darwin":
		out, err := command(ctx, "vm_stat")
		if err != nil {
			return 0, fmt.Errorf("read vm_stat: %w", err)
		}
		return parseVMStat(out)
	default:
		return 0, fmt.Errorf("no OS memory source for %s", goos)
	}
}

func parseMemAvailable(out []byte) (float64, error) {
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "MemAvailable:" {
			continue
		}
		if len(fields) != 3 || fields[2] != "kB" {
			break
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			break
		}
		return float64(kb) / (1024 * 1024), nil
	}
	return 0, errors.New("/proc/meminfo has no valid MemAvailable in kB")
}

// Free + inactive pages are a conservative reclaimable allowance. Active,
// wired and compressed pages (including resident GPU allocations) are excluded.
// Speculative pages are not added: they can already be included in free pages.
func parseVMStat(out []byte) (float64, error) {
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) == 0 {
		return 0, errors.New("empty vm_stat")
	}
	const prefix = "Mach Virtual Memory Statistics: (page size of "
	header := strings.TrimPrefix(lines[0], prefix)
	if header == lines[0] || !strings.HasSuffix(header, " bytes)") {
		return 0, errors.New("vm_stat has no page size")
	}
	pageSize, err := strconv.ParseUint(strings.TrimSuffix(header, " bytes)"), 10, 64)
	if err != nil || pageSize == 0 || pageSize > 1024*1024 {
		return 0, errors.New("vm_stat has invalid page size")
	}
	pages := map[string]uint64{}
	for _, line := range lines[1:] {
		key, value, ok := strings.Cut(line, ":")
		if !ok || (key != "Pages free" && key != "Pages inactive") {
			continue
		}
		if _, exists := pages[key]; exists {
			return 0, errors.New("vm_stat has duplicate page counts")
		}
		count, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(value), "."), 10, 64)
		if err != nil {
			return 0, errors.New("vm_stat has invalid page counts")
		}
		pages[key] = count
	}
	if len(pages) != 2 {
		return 0, errors.New("vm_stat missing free or inactive pages")
	}
	return (float64(pages["Pages free"]) + float64(pages["Pages inactive"])) * float64(pageSize) / (1024 * 1024 * 1024), nil
}

func validMemoryValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

func parseCUDAFreeMemory(output []byte) (float64, error) {
	data := strings.TrimSpace(string(output))
	if data == "" {
		return 0, fmt.Errorf("nvidia-smi returned no GPU free-memory data")
	}
	// A plain command has no device-selection contract, and v2 does not split
	// models across GPUs. Summing devices would admit work that fits none of them.
	if strings.Contains(data, "\n") {
		return 0, fmt.Errorf("CUDA inventory requires exactly one GPU; multi-GPU device selection is unsupported")
	}
	switch strings.ToLower(data) {
	case "n/a", "[n/a]", "not supported", "[not supported]":
		return 0, errUnifiedMemory
	}
	mib, err := strconv.ParseFloat(data, 64)
	if err != nil || !validMemoryValue(mib) {
		return 0, fmt.Errorf("nvidia-smi returned invalid GPU free memory")
	}
	return mib / 1024, nil
}
