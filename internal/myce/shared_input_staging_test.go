package myce

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedInputStagingArchiveProgressAndCancellation(t *testing.T) {
	for _, format := range []string{"tar", "tar.gz"} {
		for _, stopped := range []bool{false, true} {
			t.Run(format+map[bool]string{false: "/progress", true: "/cancel"}[stopped], func(t *testing.T) {
				var archive bytes.Buffer
				tw := tar.NewWriter(&archive)
				payload := bytes.Repeat([]byte("fixture"), 64<<10)
				if err := tw.WriteHeader(&tar.Header{Name: "data", Mode: 0600, Size: int64(len(payload))}); err != nil {
					t.Fatal(err)
				}
				if _, err := tw.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := tw.Close(); err != nil {
					t.Fatal(err)
				}
				var compressed bytes.Buffer
				if format == "tar.gz" {
					z := gzip.NewWriter(&compressed)
					if _, err := z.Write(archive.Bytes()); err != nil {
						t.Fatal(err)
					}
					if err := z.Close(); err != nil {
						t.Fatal(err)
					}
					archive = compressed
				}
				source := filepath.Join(t.TempDir(), "archive")
				if err := os.WriteFile(source, archive.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
				clock := &workerFakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
				cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
				if err := cache.cache.init(); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				started := clock.Now()
				published := false
				err := cache.stageInput(ctx, func(ctx context.Context, stage string) error {
					progress := ctx.Value(ioProgressKey{}).(func())
					ctx = context.WithValue(ctx, ioProgressKey{}, func() {
						clock.Advance(20 * time.Second)
						progress()
						if stopped {
							cancel()
						}
					})
					dest := filepath.Join(stage, "data")
					if err := unpackSharedArchive(ctx, source, dest, format, int64(len(payload))); err != nil {
						return err
					}
					_, err := identifyFileContext(ctx, filepath.Join(dest, "data"))
					return err
				}, func(string) error { published = true; return nil })
				<-onlySharedStaging(t, cache).done
				if stopped {
					if !errors.Is(err, context.Canceled) || published {
						t.Fatal("archive ignored cancellation", err)
					}
				} else if err != nil || !published || clock.Now().Sub(started) <= checkpointAvailabilityStallBudget {
					t.Fatal("active archive extraction/hashing did not renew progress", err, clock.Now().Sub(started))
				}
			})
		}
	}
}

// #364: stop must reach the actor while a NAS syscall has not returned. Use
// the real queue/actor/cache locks, but block only the source-I/O fault seam.
func TestWorkerStagingCancellationAndPreemption(t *testing.T) {
	for _, mode := range []string{"cancel", "production_preemption", "normal", "queue_restart"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			preemptionPolicy(t, q)
			w := recoveryWorker(t)
			w.Clock = clock
			w.Machine.Capabilities = append(w.Machine.Capabilities, "shared_inputs_v1")
			w.control = &processControl{clock: clock}
			var handler atomic.Value
			handler.Store(API{Queue: q, Token: "test"}.Handler())
			w.Client = Client{URL: "http://worker-test.invalid", Token: "test", HTTP: &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				recorder := httptest.NewRecorder()
				handler.Load().(http.Handler).ServeHTTP(recorder, r)
				return recorder.Result(), nil
			})}}
			nas := t.TempDir()
			ref := sharedFixtureFile(t, nas, "input", "shared fixture")
			input := SharedInput{NAS: "input", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
			cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
			w.CheckpointRoot, w.sharedInputs = nas, cache
			root, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer root.close()
			if err := w.initInventory(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(root.dir, "payload-started")
			s := spec("experiment")
			s.Resumable = false
			s.SharedInputs = map[string]SharedInput{"data": input}
			s.Run.Command = []string{"sh", "-ec", `test "$(cat "$MYCE_INPUT_DATA")" = "shared fixture"; touch "$1"`, "fixture", marker}
			w.Machine.SharedInputCache = &SharedInputCapacity{CapacityBytes: 1 << 20, FreeBytes: 1 << 20}
			check(t, q, w.Machine, nil)
			victim := submit(t, q, s)
			assigned := check(t, q, w.Machine, nil).Job
			if assigned == nil || assigned.ID != victim.ID {
				t.Fatal("shared-input attempt not assigned")
			}
			store, err := newAttemptStore(root, *assigned)
			if err != nil {
				t.Fatal(err)
			}
			entered, unblock := make(chan struct{}), make(chan struct{})
			var once sync.Once
			cache.beforeStage = func(ctx context.Context) {
				once.Do(func() {
					block := func() error { close(entered); <-unblock; return nil }
					if mode == "queue_restart" {
						_ = syncSharedInput(ctx, block)
					} else {
						_ = block()
					}
				})
			}
			ctx, cancel := context.WithCancel(context.Background())
			a := &attemptSupervisor{store: store, job: *assigned, worker: w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
			go a.run(ctx)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(unblock) }) }
			defer func() {
				release()
				cancel()
				select {
				case <-a.done:
				case <-time.After(testWaitTimeout):
					t.Error("actor did not stop")
				}
				pending := onlySharedStaging(t, cache)
				if pending != nil {
					<-pending.done
				}
				if err := removeSharedEntry(cache.cache.Root); err != nil {
					t.Error(err)
				}
			}()
			select {
			case <-entered:
			case <-a.done:
				_, _, _, err := a.snapshot()
				t.Fatalf("actor exited before staging: %v; job=%+v", err, job(t, q, victim.ID))
			case <-time.After(testWaitTimeout):
				t.Fatal("staging did not enter")
			}
			var demand *Job
			switch mode {
			case "cancel":
				if err := q.Cancel(victim.ID); err != nil {
					t.Fatal(err)
				}
			case "production_preemption":
				high := spec("production")
				high.Project, high.Resumable = "urgent", false
				demand = submit(t, q, high)
			case "normal":
				release()
			case "queue_restart":
				clock.Advance(RestartGrace - time.Second) // Past the old 30s I/O stall bound.
				select {
				case <-a.done:
					t.Fatal("staging exited while its local sync was still pending")
				case <-time.After(30 * time.Millisecond): // Let the real watchdog observe the fake clock.
				}
				q = reopenQueue(t, q, clock)
				handler.Store(API{Queue: q, Token: "test"}.Handler())
			}
			if mode != "normal" {
				reply, err := q.Checkin(Checkin{Machine: w.Machine, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Session: assigned.Session}}})
				if err != nil {
					t.Fatal(err)
				}
				if len(reply.Attempts) != 1 || reply.Attempts[0].Attempt != assigned.Attempt || reply.Attempts[0].Stop != (mode != "queue_restart") {
					t.Fatalf("wrong attempt directive: %+v", reply)
				}
				a.update(w, reply.Attempts[0], true, false)
			}
			if mode == "queue_restart" {
				release()
			}
			// A copy can finish just after the actor sees it pending. Its
			// preparation/publication retry is paced by this injected clock.
			await(t, func() bool {
				clock.Advance(w.Interval)
				select {
				case <-a.done:
					return true
				default:
					return false
				}
			})
			_, _, _, err = a.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			got := job(t, q, victim.ID)
			if !got.ReleaseConfirmed {
				t.Fatalf("queue retained reservation: %+v", got)
			}
			if mode == "normal" || mode == "queue_restart" {
				if got.State != "succeeded" || got.StartedAt == nil || got.InputsStagedAt == nil || got.Attempt != assigned.Attempt || got.Session != assigned.Session || got.Losses != 0 || got.InfrastructureRetries != 0 {
					t.Fatalf("original staging attempt did not finish: %+v", got)
				}
				if _, err := os.Stat(marker); err != nil {
					t.Fatal(err)
				}
				return
			}
			if got.StartedAt != nil || got.InputsStagedAt != nil {
				t.Fatal("withdrawn attempt reported a late start/staging completion")
			}
			if mode == "cancel" && got.State != "cancelled" {
				t.Fatal("cancel did not finish", got.State)
			}
			if mode == "production_preemption" {
				if got.State != "queued" || got.InfrastructureRetries != 0 || got.Losses != 0 {
					t.Fatalf("preemption consumed retries: %+v", got)
				}
				if next := check(t, q, w.Machine, nil).Job; next == nil || next.ID != demand.ID {
					t.Fatal("production could not reclaim reservation", next)
				}
			}
			for _, key := range []string{input.key() + ".use", input.key() + ".build", "admission"} {
				lock, err := cache.cache.tryLock(key)
				if err != nil || lock == nil {
					t.Fatalf("cancel retained %s lock: %v", key, err)
				}
				unlockEnvironment(lock)
			}
			// Another attempt must not start duplicate source I/O while the old
			// syscall is detached. Its own reservation remains cancellable.
			if _, done, err := cache.Acquire(context.Background(), input, nas); !errors.Is(err, errCheckpointIOPending) {
				if done != nil {
					done()
				}
				t.Fatalf("detached operation was duplicated: %v", err)
			}
			release()
			<-onlySharedStaging(t, cache).done
			if _, err := os.Stat(filepath.Join(cache.cache.Root, input.key())); !os.IsNotExist(err) {
				t.Fatal("late stage published", err)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("late stage launched payload", err)
			}
			if after := job(t, q, victim.ID); after.State != got.State || after.Attempt != got.Attempt {
				t.Fatal("late stage changed queue execution")
			}
		})
	}
}

func TestSharedInputStagingMetadataProgressAndCancellation(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(context.Context, string) error
	}{
		{"read_only", makeSharedReadOnlyContext}, {"sync", syncModelDirectoryContext},
	} {
		for _, stop := range []bool{false, true} {
			name := operation.name + map[bool]string{false: "/progress", true: "/cancel"}[stop]
			t.Run(name, func(t *testing.T) {
				clock := &workerFakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
				cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
				if err := cache.cache.init(); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				published := false
				began := clock.Now()
				err := cache.stageInput(ctx, func(ctx context.Context, stage string) error {
					if err := os.MkdirAll(filepath.Join(stage, "nested", "child"), 0700); err != nil {
						return err
					}
					progress := ctx.Value(ioProgressKey{}).(func())
					ctx = context.WithValue(ctx, ioProgressKey{}, func() {
						clock.Advance(20 * time.Second)
						progress()
						if stop {
							cancel()
						}
					})
					return operation.run(ctx, stage)
				}, func(string) error { published = true; return nil })
				<-onlySharedStaging(t, cache).done
				if stop {
					if !errors.Is(err, context.Canceled) || published {
						t.Fatal("metadata ignored cancellation", err)
					}
				} else if err != nil || !published || clock.Now().Sub(began) <= checkpointAvailabilityStallBudget {
					t.Fatal("metadata progress capped", err, clock.Now().Sub(began))
				}
			})
		}
	}
}

func onlySharedStaging(t *testing.T, cache *SharedInputCache) *sharedInputStaging {
	t.Helper()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.staging) != 1 {
		t.Fatalf("expected one source operation, got %d", len(cache.staging))
	}
	for pending := range cache.staging {
		return pending
	}
	return nil
}
