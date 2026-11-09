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

func TestUnifiedProbeFailureStatusAndRecovery(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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
		w := Worker{EnvironmentLimit: 1 << 30, StateDir: newTestStateRoot(t), Client: Client{URL: server.URL, Token: "test"}, Machine: machine("unified", "fake"), MemorySource: source, MemoryMeasured: true, Interval: time.Millisecond}
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
		// Public status keeps a fixed diagnostic and omits raw probe output.
		req, _ := http.NewRequest("GET", server.URL, nil)
		req.Header.Set("Authorization", "Bearer test")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || !strings.Contains(string(data), "memory probe failed") || strings.Contains(string(data), "meminfo denied") {
			t.Fatalf("status page hid failure: %v", err)
		}
		recovered.Store(true)
		await(t, func() bool {
			s, err := q.Snapshot()
			return err == nil && len(s.Machines) == 1 && s.Machines[0].MemoryError == "" && s.Machines[0].FreeMemoryGB == 80
		})
		await(t, func() bool { return job(t, q, j.ID).State == "succeeded" })

	})

	t.Run("MemoryFailureBlocksModelAdmission", func(t *testing.T) {
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

	})
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
	q, clock := fixture(t)
	j := submit(t, q, spec("production"))
	j.Attempt = 1
	w := Worker{Clock: clock, StateDir: newTestStateRoot(t), Machine: machine("unified", "fake"), MemorySource: source, MemoryMeasured: true}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	await(t, func() bool {
		_, err = w.start(store, *j)
		return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
	})
	if !errors.Is(err, errWorkerMemory) {
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
