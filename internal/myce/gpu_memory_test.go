package myce

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestMemorySourceCUDA(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "command context")
	calls := 0
	source, err := newMemorySourceWithCommand("cuda", 0, 1, func(got context.Context, name string, args ...string) ([]byte, error) {
		if got != ctx || name != "nvidia-smi" || !reflect.DeepEqual(args, []string{"--query-gpu=memory.free", "--format=csv,noheader,nounits"}) {
			t.Fatalf("unexpected command: context=%v name=%q args=%v", got, name, args)
		}
		calls++
		if calls == 1 {
			return []byte(" 8192\r\n4096\r\n"), nil
		}
		return []byte("512\n"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []float64{11, 0} {
		got, err := source(ctx)
		if err != nil || got != want {
			t.Fatalf("memory = %v, %v; want %v", got, err, want)
		}
	}
	if calls != 2 {
		t.Fatalf("source must query on each call: %d calls", calls)
	}
}

func TestMemorySourceCUDABudgetCap(t *testing.T) {
	source, err := newMemorySourceWithCommand("cuda", 8, 2, func(context.Context, string, ...string) ([]byte, error) {
		return []byte("16384\n"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source(context.Background()); err != nil || got != 6 {
		t.Fatalf("memory = %v, %v; want 6", got, err)
	}
}

func TestMemorySourceCUDARejectsBadOutput(t *testing.T) {
	for _, output := range []string{"", " \n", "N/A\n", "[Not Supported]", "1 MiB", "-1", "NaN", "Inf", "-Inf", "1e309", "1024\n\n2048", "1024\nwrong", "1024,2048"} {
		t.Run(output, func(t *testing.T) {
			source, err := newMemorySourceWithCommand("cuda", 16, 0, func(context.Context, string, ...string) ([]byte, error) {
				return []byte(output), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := source(context.Background()); err == nil || got != 0 {
				t.Fatalf("bad output returned %v, %v", got, err)
			}
		})
	}
	if _, err := parseCUDAFreeMemory([]byte("0\n")); err != nil {
		t.Fatalf("zero free memory is valid: %v", err)
	}
	if _, err := parseCUDAFreeMemory([]byte(strings.Repeat("1.7976931348623157e308\n", 2048))); err == nil {
		t.Fatal("accepted an overflowing GPU free-memory sum")
	}
}

func TestMemorySourceCUDACommandFailure(t *testing.T) {
	want := errors.New("command unavailable")
	source, err := newMemorySourceWithCommand("cuda", 16, 0, func(context.Context, string, ...string) ([]byte, error) {
		return []byte("8192"), want
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source(context.Background()); !errors.Is(err, want) || got != 0 {
		t.Fatalf("command failure returned %v, %v", got, err)
	}
}

func TestMemorySourceConfiguredBudget(t *testing.T) {
	for _, platform := range []string{"fake", "xpu", "metal"} {
		t.Run(platform, func(t *testing.T) {
			source, err := newMemorySourceWithCommand(platform, 12.5, 2.25, func(context.Context, string, ...string) ([]byte, error) {
				t.Fatal("configured memory must not run a command")
				return nil, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := source(context.Background()); err != nil || got != 10.25 {
				t.Fatalf("memory = %v, %v; want 10.25", got, err)
			}
		})
	}
	for _, platform := range []string{"fake", "xpu", "metal"} {
		source, err := NewMemorySource(platform, 2, 2)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := source(context.Background()); err != nil || got != 0 {
			t.Fatalf("fully reserved %s memory = %v, %v", platform, got, err)
		}
	}
	source, err := NewMemorySource("fake", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source(context.Background()); err != nil || got != 0 {
		t.Fatalf("zero fake memory = %v, %v", got, err)
	}
}

func TestMemorySourceInvalidConfiguration(t *testing.T) {
	for _, platform := range []string{"cuda", "fake", "xpu", "metal"} {
		for _, invalid := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			if _, err := NewMemorySource(platform, invalid, 0); err == nil {
				t.Fatalf("%s accepted budget %v", platform, invalid)
			}
			if _, err := NewMemorySource(platform, 8, invalid); err == nil {
				t.Fatalf("%s accepted reserve %v", platform, invalid)
			}
		}
		if _, err := NewMemorySource(platform, 8, 9); err == nil {
			t.Fatalf("%s accepted reserve larger than budget", platform)
		}
	}
	for _, platform := range []string{"xpu", "metal"} {
		if _, err := NewMemorySource(platform, 0, 0); err == nil {
			t.Fatalf("%s accepted missing explicit positive budget", platform)
		}
	}
	if _, err := NewMemorySource("unknown", 8, 0); err == nil {
		t.Fatal("accepted unknown platform")
	}
	if _, err := newMemorySourceWithCommand("cuda", 0, 0, nil); err == nil {
		t.Fatal("accepted nil CUDA command runner")
	}
}

func TestMemorySourceCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, platform := range []string{"cuda", "fake", "xpu", "metal"} {
		source, err := newMemorySourceWithCommand(platform, 8, 0, func(context.Context, string, ...string) ([]byte, error) {
			t.Fatal("canceled source must not run a command")
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("%s cancellation = %v", platform, err)
		}
	}
}
