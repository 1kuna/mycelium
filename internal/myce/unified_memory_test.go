package myce

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnifiedCUDAAutoDetectionAndRecheck(t *testing.T) {
	for _, output := range []string{"[N/A]\n", "N/A", "Not Supported", "[Not Supported]"} {
		t.Run(output, func(t *testing.T) {
			reads, queries := 0, 0
			source, err := newMemorySource("cuda", 0, 16, "auto", "linux", func(path string) ([]byte, error) {
				if path != "/proc/meminfo" {
					t.Fatalf("path %s", path)
				}
				reads++
				if reads == 1 {
					return []byte("MemTotal: 134217728 kB\nMemAvailable: 100663296 kB\n"), nil
				}
				return []byte("MemAvailable: 8388608 kB\n"), nil
			}, func(context.Context, string, ...string) ([]byte, error) { queries++; return []byte(output), nil })
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []float64{80, 0} {
				got, err := source(context.Background())
				if err != nil || got != want {
					t.Fatalf("got %v, %v; want %v", got, err, want)
				}
			}
			if reads != 2 || queries != 2 {
				t.Fatalf("probes reads=%d queries=%d", reads, queries)
			}
		})
	}
}

func TestUnifiedMacPageSizesAndCap(t *testing.T) {
	for _, pageSize := range []string{"4096", "16384"} {
		t.Run(pageSize, func(t *testing.T) {
			calls := 0
			source, err := newMemorySource("metal", 2, 1, "auto", "darwin", nil, func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "vm_stat" || len(args) != 0 {
					t.Fatalf("command %s %v", name, args)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("unbounded vm_stat")
				}
				calls++
				free := "1048576"
				if calls == 2 {
					free = "0"
				}
				return []byte("Mach Virtual Memory Statistics: (page size of " + pageSize + " bytes)\nPages free: " + free + ".\nPages inactive: 0.\nPages active: 99999999.\nPages wired down: 99999999.\nPages occupied by compressor: 99999999.\n"), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []float64{1, 0} {
				got, err := source(context.Background())
				if err != nil || got != want {
					t.Fatalf("got %v, %v; want %v", got, err, want)
				}
			}
		})
	}
	got, err := parseVMStat([]byte("Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages free: 65536.\nPages inactive: 131072.\nPages speculative: 99999999.\n"))
	if err != nil || got != 3 {
		t.Fatalf("reclaimable memory %v, %v", got, err)
	}
}

func TestUnifiedFailureAndOverrides(t *testing.T) {
	failed := func(string) ([]byte, error) { return nil, errors.New("meminfo denied") }
	command := func(context.Context, string, ...string) ([]byte, error) { return []byte("[N/A]"), nil }
	source, err := newMemorySource("cuda", 32, 16, "auto", "linux", failed, command)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source(context.Background()); got != 0 || err == nil || !strings.Contains(err.Error(), "meminfo denied") || !strings.Contains(err.Error(), "refusing work") {
		t.Fatalf("failure = %v, %v", got, err)
	}
	source, err = newMemorySource("cuda", 32, 16, "fixed", "linux", failed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source(context.Background()); got != 16 || err != nil {
		t.Fatalf("fixed = %v, %v", got, err)
	}
	source, err = newMemorySource("cuda", 0, 1, "unified", "linux", func(string) ([]byte, error) { return []byte("MemAvailable: 4194304 kB"), nil }, func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("override must bypass GPU query")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source(context.Background()); got != 3 || err != nil {
		t.Fatalf("unified = %v, %v", got, err)
	}
	source, err = newMemorySource("cuda", 32, 0, "dedicated", "linux", failed, command)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source(context.Background()); got != 0 || !errors.Is(err, errUnifiedMemory) {
		t.Fatalf("dedicated = %v, %v", got, err)
	}
	for _, mode := range []string{"wrong", "fixed"} {
		if _, err := NewMemorySourceWithMode("cuda", 0, 0, mode); err == nil {
			t.Fatalf("accepted %s", mode)
		}
	}
	if _, err := NewMemorySourceWithMode("metal", 0, 0, "dedicated"); err == nil {
		t.Fatal("accepted dedicated metal")
	}
	for _, goos := range []string{"linux", "darwin", "unknown"} {
		source, err := newMemorySource("metal", 0, 0, "auto", goos, failed, func(context.Context, string, ...string) ([]byte, error) {
			return nil, errors.New("vm_stat unavailable")
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := source(context.Background()); got != 0 || err == nil {
			t.Fatalf("%s = %v, %v", goos, got, err)
		}
	}
}

func TestOSMemoryRejectsMissingOrMalformedData(t *testing.T) {
	for _, input := range []string{"", "MemTotal: 134217728 kB", "MemAvailable: N/A kB", "MemAvailable: -1 kB", "MemAvailable: 123 MB", "MemAvailable: 1.5 kB", "MemAvailable: 18446744073709551616 kB"} {
		if _, err := parseMemAvailable([]byte(input)); err == nil {
			t.Fatalf("accepted meminfo %q", input)
		}
	}
	header := "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
	for _, input := range []string{"", "Pages free: 1.", header, header + "Pages free: 1.", header + "Pages free: -1.\nPages inactive: 1.", header + "Pages free: 1.\nPages inactive: N/A", header + "Pages free: 1.\nPages free: 2.\nPages inactive: 1.", strings.Replace(header, "16384", "0", 1) + "Pages free: 1.\nPages inactive: 1."} {
		if _, err := parseVMStat([]byte(input)); err == nil {
			t.Fatalf("accepted vm_stat %q", input)
		}
	}
	if got, err := parseMemAvailable([]byte("MemAvailable: 0 kB")); got != 0 || err != nil {
		t.Fatalf("zero = %v, %v", got, err)
	}
}

func TestUnifiedProbeFailureStatusAndRecovery(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(server.Close)
	var recovered atomic.Bool
	source, err := newMemorySource("cuda", 0, 16, "auto", "linux", func(string) ([]byte, error) {
		if !recovered.Load() {
			return nil, errors.New("meminfo denied")
		}
		return []byte("MemAvailable: 100663296 kB"), nil
	}, func(context.Context, string, ...string) ([]byte, error) { return []byte("[N/A]"), nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := Worker{StateDir: t.TempDir(), Client: Client{URL: server.URL, Token: "test"}, Machine: machine("unified", "fake"), MemorySource: source, MemoryMeasured: true, Interval: time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = q.Cancel(j.ID)
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("worker did not exit")
		}
	})
	await(t, func() bool {
		s, err := q.Snapshot()
		return err == nil && len(s.Machines) == 1 && strings.Contains(s.Machines[0].MemoryError, "meminfo denied") && s.Machines[0].FreeMemoryGB == 0
	})
	requireState(t, q, j.ID, "queued")
	if job(t, q, j.ID).Attempt != 0 {
		t.Fatal("failed telemetry admitted work")
	}
	// The public status page displays the diagnostic (escaped by html/template).
	response, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || !strings.Contains(string(data), "meminfo denied") {
		t.Fatalf("status page hid failure: %v", err)
	}
	recovered.Store(true)
	await(t, func() bool {
		s, err := q.Snapshot()
		return err == nil && len(s.Machines) == 1 && s.Machines[0].MemoryError == "" && s.Machines[0].FreeMemoryGB == 80
	})
	await(t, func() bool { return job(t, q, j.ID).State == "succeeded" })
}

func TestMemoryFailureBlocksModelAdmission(t *testing.T) {
	q, _, m := modelFixture(t)
	m.MemoryError = "unified memory unavailable"
	check(t, q, m, nil)
	c := addModelCall(t, q, "production")
	if c.Machine != "" || len(q.s.ModelSlots) != 0 {
		t.Fatal("failed telemetry admitted model load")
	}
	m.MemoryError = ""
	check(t, q, m, nil)
	if len(q.s.ModelSlots) != 1 {
		t.Fatal("recovered telemetry did not admit model load")
	}
}

func TestUnifiedOSRecheckedBeforeCommandAndModelLoad(t *testing.T) {
	reads := 0
	source, err := newMemorySource("cuda", 0, 16, "auto", "linux", func(string) ([]byte, error) {
		reads++
		if reads == 1 {
			return []byte("MemAvailable: 100663296 kB"), nil
		}
		return []byte("MemAvailable: 16777216 kB"), nil
	}, func(context.Context, string, ...string) ([]byte, error) { return []byte("[N/A]"), nil })
	if err != nil {
		t.Fatal(err)
	}
	if got, err := source(context.Background()); err != nil || got != 80 {
		t.Fatalf("initial inventory %v, %v", got, err)
	}
	q, _ := fixture(t)
	j := submit(t, q, spec("production"))
	j.Attempt = 1
	w := Worker{StateDir: t.TempDir(), Machine: machine("unified", "fake"), MemorySource: source, MemoryMeasured: true}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	if _, err := w.start(store, *j); !errors.Is(err, errWorkerMemory) {
		t.Fatalf("command started after OS memory dropped: %v", err)
	}
	if store.state.Job != nil {
		t.Fatal("refused command entered spool")
	}
	engine := &lifecycleEngine{started: make(chan struct{})}
	rt, model, ctx := runtimeLoadFixture(t, engine)
	rt.memorySource, rt.memoryMeasured = source, true
	rt.load(ctx, model)
	select {
	case <-engine.started:
		t.Fatal("model engine started after OS memory dropped")
	default:
	}
	if model.state != "failed" || !strings.Contains(model.err, "insufficient free GPU memory") {
		t.Fatalf("model failure hidden: %+v", model)
	}
	if reads != 3 {
		t.Fatalf("OS memory not rechecked at both starts: %d", reads)
	}
}
