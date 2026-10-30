package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// An exit-zero result must survive inspector failure with verified, downloadable
// outputs while the exact attempt still owns its reservation (#428, #356).
func TestWorkerCleanupInspectionPreservesSuccessfulOutputs(t *testing.T) {
	realPS, err := exec.LookPath("ps")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	denied := filepath.Join(bin, "deny")
	wrapper := `#!/bin/sh
if [ "$*" = "-ax -o pid=,pgid=,stat=" ] && [ -f "$MYCE_TEST_CLEANUP_DENIED" ]; then exit 1; fi
exec "$MYCE_TEST_REAL_PS" "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "ps"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("MYCE_TEST_REAL_PS", realPS)
	t.Setenv("MYCE_TEST_CLEANUP_DENIED", denied)
	for _, grouped := range []bool{false, true} {
		name := "command"
		if grouped {
			name = "native-group"
		}
		t.Run(name, func(t *testing.T) {
			q, initial := fixture(t)
			clock := &workerFakeClock{now: initial.Now()}
			q.clock = clock
			if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer api.Close()
			s := spec("experiment")
			s.Resumable = false
			s.Needs.HostMemoryGB = 2
			_, input := retentionUpload(t, Client{URL: api.URL, Token: "test"}, "input")
			s.Files = FileManifest{"input": input}
			s.Outputs = map[string]string{"out.txt": "results"}
			s.Run.Command = []string{"sh", "-c", `printf 'retained result' > out.txt; : > "$DENIED"`}
			s.Run.Env = map[string]string{"DENIED": denied}
			if grouped {
				s.Run.MemoryGB = 1
			}
			j := submit(t, q, s)
			w := Worker{StateDir: t.TempDir(), Clock: clock, Interval: time.Millisecond, Machine: machine("worker", "fake"), Client: Client{URL: api.URL, Token: "test"}, memoryCeilings: fakeCeilings(t)}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			defer func() {
				_ = os.Remove(denied)
				cancel()
				await(t, func() bool {
					clock.Advance(100 * time.Millisecond)
					select {
					case err := <-done:
						if err != nil {
							t.Error(err)
						}
						return true
					default:
						return false
					}
				})
			}()
			await(t, func() bool { clock.Advance(100 * time.Millisecond); return job(t, q, j.ID).State == "succeeded" })
			got := job(t, q, j.ID)
			ref, ok := got.OutputRefs["out.txt"]
			if !ok || got.ReleaseConfirmed || got.CleanupCause == nil {
				t.Fatalf("result or custody lost: %+v", got)
			}
			q.mu.Lock()
			held := len(q.s.HostReleases)
			q.mu.Unlock()
			if held != 1 {
				t.Fatalf("expected held resources, got %d reservations", held)
			}
			// Durable exit facts remain zero even though cleanup was uncertain.
			receipts, _ := filepath.Glob(filepath.Join(w.StateDir, "*.result.json"))
			if len(receipts) != 1 {
				t.Fatalf("missing result receipt: %v", receipts)
			}
			data, err := os.ReadFile(receipts[0])
			var receipt Result
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &receipt); err != nil {
				t.Fatal(err)
			}
			if !receipt.Success || !receipt.ReleasePending || !strings.HasPrefix(receipt.Error, "cleanup uncertain:") {
				t.Fatalf("unexpected completion: %+v", receipt)
			}
			exits, _ := filepath.Glob(filepath.Join(w.StateDir, "*.outcome.json"))
			if grouped {
				exits, _ = filepath.Glob(filepath.Join(w.StateDir, "native-*.json"))
			}
			if len(exits) != 1 {
				t.Fatalf("missing native exit evidence: %v", exits)
			}
			data, err = os.ReadFile(exits[0])
			if err != nil {
				t.Fatal(err)
			}
			var facts memberOutcome
			if grouped {
				var native struct{ Members []nativeMember }
				if err := json.Unmarshal(data, &native); err != nil {
					t.Fatal(err)
				}
				for _, m := range native.Members {
					if m.Name == "main" {
						facts = m.memberOutcome
					}
				}
			} else if err := json.Unmarshal(data, &facts); err != nil {
				t.Fatal(err)
			}
			if facts.Exit == nil || facts.Exit.Status != 0 {
				t.Fatalf("successful exit evidence lost: %+v", facts)
			}
			dest := filepath.Join(t.TempDir(), "download")
			if err := w.Client.DownloadFile(ctx, ref, dest); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(dest); err != nil || string(data) != "retained result" {
				t.Fatalf("published bytes: %q %v", data, err)
			}
			// Replay from a different attempt must not publish or release this custody.
			staleResult := receipt
			staleResult.Attempt++
			staleResult.ReleasePending = false
			if err := q.Complete(staleResult); !errors.Is(err, ErrStale) {
				t.Fatalf("stale result accepted: %v", err)
			}
			if job(t, q, j.ID).ReleaseConfirmed {
				t.Fatal("stale attempt released custody")
			}
			if err := os.Remove(denied); err != nil {
				t.Fatal(err)
			}
			await(t, func() bool { clock.Advance(100 * time.Millisecond); return job(t, q, j.ID).ReleaseConfirmed })
			got = job(t, q, j.ID)
			if got.State != "succeeded" || got.Attempt != receipt.Attempt || got.OutputRefs["out.txt"] != ref || got.CleanupCause == nil {
				t.Fatalf("cleanup recovery changed result: %+v", got)
			}
			q.mu.Lock()
			held = len(q.s.HostReleases)
			q.mu.Unlock()
			if held != 0 {
				t.Fatal("proven cleanup retained reservation")
			}
		})
	}
}

func TestWorkerCleanupInspectionBackoff(t *testing.T) {
	for _, recover := range []bool{false, true} {
		t.Run(map[bool]string{false: "exhausted", true: "recovered"}[recover], func(t *testing.T) {
			clock := &workerFakeClock{now: time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)}
			calls := make(chan time.Time, 3)
			result := make(chan error, 1)
			go func() {
				n := 0
				_, err := inspectWorkerGroupUsing(42, clock, func(int) ([]int, error) {
					n++
					calls <- clock.Now()
					if recover && n == 3 {
						return nil, nil
					}
					return nil, exec.ErrWaitDelay
				})
				result <- err
			}()
			first := <-calls
			clock.Advance(99 * time.Millisecond)
			select {
			case <-calls:
				t.Fatal("retried before backoff")
			default:
			}
			// The first failure records its retry deadline after the callback returns.
			// Advance until each observation, then check the measured clock spacing.
			var second, third time.Time
			await(t, func() bool {
				clock.Advance(time.Millisecond)
				select {
				case second = <-calls:
					return true
				default:
					return false
				}
			})
			await(t, func() bool {
				clock.Advance(time.Millisecond)
				select {
				case third = <-calls:
					return true
				default:
					return false
				}
			})
			if second.Sub(first) < 100*time.Millisecond || third.Sub(second) < 200*time.Millisecond {
				t.Fatalf("backoff skipped: %v %v %v", first, second, third)
			}
			err := <-result
			if recover && err != nil || !recover && !errors.Is(err, exec.ErrWaitDelay) {
				t.Fatalf("inspection outcome: %v", err)
			}
		})
	}
}
