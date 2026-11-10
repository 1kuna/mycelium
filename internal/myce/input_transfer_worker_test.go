package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The production queue and real exec gate must see one preparation attempt,
// with no payload or loss/retry charge while its immutable dependency recovers.
func TestInputTransferProductionPreparationRetainsAttempt(t *testing.T) {
	for _, mode := range []string{"headers", "resume", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			s := submitVerificationInput(t, q)
			s.Priority, s.Resumable = "production", false
			s.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1}
			s.Run.Command = []string{"sh", "-ec", `test "$(cat input)" = 'small verified input'; printf once >> launched`}
			j := submit(t, q, s)
			m := cpuMachine("input-worker", "", "cpu", 8, 64)
			assigned := check(t, q, m, nil).Job
			if assigned == nil {
				t.Fatal("production input not assigned", job(t, q, j.ID))
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var gets atomic.Int32
			c := Client{URL: server.URL, Token: "test", Jitter: func() float64 { return 0 }, Pause: func(ctx context.Context, _ time.Duration) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			ref := s.Files["input"]
			c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/files/blobs/") {
					n := gets.Add(1)
					if n == 1 {
						if mode == "headers" {
							close(entered)
							select {
							case <-release:
							case <-r.Context().Done():
								return nil, r.Context().Err()
							}
						} else {
							return &http.Response{StatusCode: 200, Header: http.Header{"Etag": {fmt.Sprintf("%q", ref.SHA256)}}, ContentLength: ref.SizeBytes, Body: io.NopCloser(io.MultiReader(strings.NewReader("small "), stagingTimeoutReader{}))}, nil
						}
					} else if r.Header.Get("Range") != "bytes=6-" {
						t.Error("did not resume saved dependency prefix", r.Header)
					}
				}
				return http.DefaultTransport.RoundTrip(r)
			})}
			w := Worker{StateDir: newTestStateRoot(t), Clock: clock, Machine: m, Client: c, FileCacheRoot: t.TempDir(), FileCacheLimit: 1 << 20, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }, MemorySource: func(context.Context) (float64, error) { return 64, nil }}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session, store.state.Job = assigned.Session, assigned
			store.extensionFiles.timeout = testWaitTimeout
			if err := w.initFileTransfers(ctx, store); err != nil {
				t.Fatal(err)
			}
			type startResult struct {
				p   *process
				err error
			}
			done := make(chan startResult, 1)
			go func() { p, err := w.startContext(ctx, store, *assigned); done <- startResult{p, err} }()
			select {
			case <-entered:
			case got := <-done:
				t.Fatalf("preparation ended before test boundary: %v", got.err)
			case <-time.After(testWaitTimeout):
				t.Fatal("staging did not enter")
			}
			assertUnstarted := func() {
				got := job(t, q, j.ID)
				if got.Attempt != assigned.Attempt || got.State != "running" || got.StartedAt != nil || got.Losses != 0 || got.InfrastructureRetries != 0 {
					t.Fatalf("slow staging spent an attempt: %+v", got)
				}
				for _, copy := range got.Copies {
					if copy.LossCharged {
						t.Fatal("slow staging charged custody")
					}
				}
				if _, err := os.Stat(filepath.Join(store.dir, "job-files", j.ID, fmt.Sprint(assigned.Attempt), "launched")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("payload launched before staging", err)
				}
			}
			assertUnstarted()
			if mode != "headers" && !store.inputTransferRecovering.Load() {
				t.Fatal("missing recovering observation")
			}
			if mode == "cancel" {
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
				cancel()
			} else {
				unblock()
			}
			var started startResult
			select {
			case started = <-done:
			case <-time.After(testWaitTimeout):
				t.Fatal("staging did not settle")
			}
			if mode == "cancel" {
				if started.p != nil || !errors.Is(started.err, context.Canceled) {
					t.Fatal("owner cancellation launched or lost reason", started.err)
				}
				store.extensionFiles.mu.Lock()
				pending := store.extensionFiles.pending
				store.extensionFiles.mu.Unlock()
				if pending != nil {
					select {
					case <-pending.done:
					case <-time.After(testWaitTimeout):
						t.Fatal("cancelled transfer retained operation")
					}
				}
				if err := q.Complete(result(assigned, false)); err != nil {
					t.Fatal(err)
				}
			} else {
				if started.err != nil || started.p == nil {
					t.Fatal("staging did not launch verified input", started.err)
				}
				select {
				case finished := <-started.p.done:
					if finished.err != nil || !finished.result.Success {
						t.Fatalf("payload failed: %+v", finished)
					}
					if err := q.Complete(finished.result); err != nil {
						t.Fatal(err)
					}
				case <-time.After(testWaitTimeout):
					started.p.stop(clock.Now(), 0, true)
					t.Fatal("payload did not finish")
				}
				data, err := os.ReadFile(filepath.Join(store.dir, "job-files", j.ID, fmt.Sprint(assigned.Attempt), "launched"))
				if err != nil || string(data) != "once" {
					t.Fatal("payload did not execute exactly once", string(data), err)
				}
			}
			got := job(t, q, j.ID)
			want := "succeeded"
			if mode == "cancel" {
				want = "cancelled"
			}
			if got.State != want || got.Attempt != assigned.Attempt || got.Losses != 0 || got.InfrastructureRetries != 0 || store.inputTransferRecovering.Load() {
				t.Fatalf("recovery spent production retry or retained state: %+v", got)
			}
			for _, copy := range got.Copies {
				if copy.LossCharged {
					t.Fatal("recovery charged custody")
				}
			}
		})
	}
}

func TestInputTransferRecoveringStatusDoesNotHideRefusal(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("input-worker", "", "cpu", 4, 16)
	m.LastSeen, m.FileTransferError = clock.Now(), inputTransferRecoveringReason
	if err := q.update(func() error { q.touchMachine(m.ID); q.s.Machines[m.ID] = &m; return nil }); err != nil {
		t.Fatal(err)
	}
	status, err := q.publicSnapshot()
	if err != nil || len(status.Machines) != 1 || status.Machines[0].FileTransferError != inputTransferRecoveringReason {
		t.Fatal("lost public recovering reason", status.Machines, err)
	}
	for _, event := range q.s.Health.Events {
		if event.Kind == "staging_cache_refused" {
			t.Fatal("pending recovery reported as refusal", event)
		}
	}
	for _, order := range [][]string{{inputTransferRecoveringReason, "private transport failure"}, {"private transport failure", inputTransferRecoveringReason}} {
		combined := ""
		for _, next := range order {
			combined = preferTransferError(combined, next)
		}
		if err := q.update(func() error { q.touchMachine(m.ID); m.FileTransferError = combined; return nil }); err != nil {
			t.Fatal(err)
		}
		requireIncident(t, q, "staging_cache_refused", "cache/"+m.ID, 1, true)
		status, err = q.publicSnapshot()
		if err != nil || len(status.Machines) != 1 || status.Machines[0].FileTransferError != "file transfer pending; only published progress is available elsewhere" {
			t.Fatal("refusal leaked private text or was hidden by aggregation order", status.Machines, err)
		}
	}
}
