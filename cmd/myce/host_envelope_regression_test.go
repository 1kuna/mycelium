package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"mycelium/internal/myce"
)

func TestHostEnvelopeStatusUnsafeWorkerFiles(t *testing.T) {
	for _, target := range []string{"registry", "state"} {
		for _, kind := range []string{"fifo", "directory", "symlink", "oversize"} {
			t.Run(target+"/"+kind, func(t *testing.T) {
				r, c := envelopeFixture(t)
				registry := filepath.Join(c.HostStateDir, "worker-fixture.json")
				b, err := json.Marshal(myce.HostWorker{State: c.State, Host: c.HostID, Name: "fixture"})
				if err != nil {
					t.Fatal(err)
				}
				path := registry
				if target == "state" {
					if err = os.WriteFile(registry, b, 0600); err != nil {
						t.Fatal(err)
					}
					path = filepath.Join(c.State, "state.json")
				}
				switch kind {
				case "fifo":
					err = unix.Mkfifo(path, 0600)
				case "directory":
					err = os.Mkdir(path, 0700)
				case "symlink":
					err = os.Symlink(c.Config, path)
				case "oversize":
					// Valid JSON above the evidence limit must not be read in full.
					err = os.WriteFile(path, append(b, bytes.Repeat([]byte(" "), 1<<20)...), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
				type result struct {
					data    []byte
					err     error
					elapsed time.Duration
				}
				done := make(chan result, 1)
				go func() {
					var out bytes.Buffer
					start := time.Now()
					err := hostLocalCommandWithIdentity([]string{"status", "--local", "--config", c.Config, "--timeout", "80ms", "--json"}, myce.MachineProbe{}, &out, r, nil)
					done <- result{out.Bytes(), err, time.Since(start)}
				}()
				select {
				case got := <-done:
					e := validateEnvelopeOutput(t, got.data)
					if got.err == nil || got.elapsed >= 80*time.Millisecond || e.Accepted != nil || e.Counts.Active != nil {
						t.Fatalf("unsafe evidence: err=%v elapsed=%v envelope=%+v", got.err, got.elapsed, e)
					}
					found := false
					for _, failure := range e.Errors {
						found = found || failure.Code == "host_target_unsafe"
					}
					if !found {
						t.Fatal("unsafe worker evidence was not reported", e.Errors)
					}
				case <-time.After(600 * time.Millisecond):
					// Unblock the old blocking-open implementation before failing.
					if kind == "fifo" {
						fd, err := unix.Open(path, unix.O_WRONLY|unix.O_NONBLOCK, 0)
						if err == nil {
							unix.Close(fd)
						}
					}
					select {
					case <-done:
					case <-time.After(time.Second):
					}
					t.Fatal("80ms status request blocked for 600ms without an envelope")
				}
			})
		}
	}
}

func TestHostEnvelopeConcurrentClose(t *testing.T) {
	for _, waitReleased := range []bool{false, true} {
		name := "accepted"
		if waitReleased {
			name = "wait-released"
		}
		t.Run(name, func(t *testing.T) {
			r, c := envelopeFixture(t)
			prepared, resume := make(chan struct{}), make(chan struct{}, 1)
			defer close(resume)
			handler := func(first bool) myce.HostMutationHandler {
				return func(ctx context.Context, v myce.HostInvocation, m myce.HostMutation) (myce.HostMutationResult, error) {
					h := myce.HostControl{Dir: v.StateDir, Clock: myce.RealClock{}, FreshClose: true, CloseExpected: m.CloseExpected, Service: envelopeService{}, Observe: envelopeHardware}
					if first {
						h.Prepare = func() error {
							close(prepared)
							select {
							case <-resume:
								return nil
							case <-ctx.Done():
								return ctx.Err()
							}
						}
					}
					report, err := h.Disable(ctx, m.Reason, m.Grace)
					return myce.HostMutationResult{HostRevision: report.HostRevision, CleanupConfirmed: report.Confirmed, Partial: err != nil}, err
				}
			}
			args := []string{"close", "--local", "--config", c.Config, "--grace", "0", "--timeout", "2s", "--json"}
			type result struct {
				data []byte
				err  error
			}
			firstDone := make(chan result, 1)
			go func() {
				var out bytes.Buffer
				firstArgs := append([]string(nil), args...)
				if waitReleased {
					firstArgs = append(firstArgs, "--wait-released")
				}
				err := hostEnvelopeCommandWithHandler(firstArgs, myce.MachineProbe{}, &out, r, nil, handler(true))
				firstDone <- result{out.Bytes(), err}
			}()
			select {
			case <-prepared:
			case <-time.After(time.Second):
				t.Fatal("first close did not accept and reach Prepare")
			}
			// A still owns reconcile.lock. B accepts the next durable revision.
			var out bytes.Buffer
			err := hostEnvelopeCommandWithHandler(args, myce.MachineProbe{}, &out, r, nil, handler(false))
			second := validateEnvelopeOutput(t, out.Bytes())
			if err != nil || second.Accepted == nil || *second.Accepted != 3 {
				t.Fatal("second close", second, err)
			}
			resume <- struct{}{}
			select {
			case got := <-firstDone:
				first := validateEnvelopeOutput(t, got.data)
				if (got.err != nil) != waitReleased || first.Accepted == nil || *first.Accepted != 2 || first.Desired.Revision == nil || *first.Desired.Revision != 3 {
					t.Fatal("first close", first, got.err)
				}
				found := false
				for _, failure := range first.Errors {
					found = found || failure.Component == "host_intent" && failure.Code == "revision_superseded"
				}
				if !found {
					t.Fatal("superseded acceptance was not reported", first.Errors)
				}
			case <-time.After(time.Second):
				t.Fatal("first close did not finish")
			}
		})
	}
}
