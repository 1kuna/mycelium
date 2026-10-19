package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCompletionVerificationDoesNotHoldQueueLock(t *testing.T) {
	for _, fence := range []string{"cancel", "close", "interrupt", "request-cancel", "accept"} {
		t.Run(fence, func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			a, b := machine("a", "fake"), machine("b", "fake")
			a.HostID, b.HostID = "host-a", "host-b"
			check(t, q, a, nil)
			check(t, q, b, nil)
			src := filepath.Join(t.TempDir(), "out.txt")
			if err := os.WriteFile(src, []byte("finished output"), 0600); err != nil {
				t.Fatal(err)
			}
			ref, err := IdentifyFile(src)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := os.ReadFile(src)
			if err := os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
				t.Fatal(err)
			}
			s := spec("experiment")
			s.AllowedMachines = []string{a.ID}
			s.Files = FileManifest{"input": ref}
			s.Outputs = map[string]string{"out.txt": "out.txt"}
			submit(t, q, s)
			publishing := check(t, q, a, nil).Job
			s.AllowedMachines, s.Outputs = []string{b.ID}, nil
			submit(t, q, s)
			other := check(t, q, b, nil).Job
			if publishing == nil || other == nil {
				t.Fatal("missing assignments")
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			q.files.syncDir = func(path string) error {
				close(entered)
				<-release
				return syncWorkerDir(path)
			}
			r := result(publishing, true)
			r.OutputRefs = FileManifest{"out.txt": ref}
			body, _ := json.Marshal(r)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := httptest.NewRequest("POST", "/api/workers/complete", bytes.NewReader(body)).WithContext(ctx)
			request.Header.Set("Authorization", "Bearer test")
			request.Header.Set("X-Myce-Start-Gate", startGateProtocol)
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				API{Queue: q, Token: "test"}.Handler().ServeHTTP(response, request)
				close(done)
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("verification did not reach durability barrier")
			}
			progress := func(name string, fn func() error) {
				t.Helper()
				result := make(chan error, 1)
				go func() { result <- fn() }()
				select {
				case err := <-result:
					if err != nil {
						t.Fatalf("%s: %v", name, err)
					}
				case <-time.After(3 * time.Second):
					t.Fatalf("%s blocked behind output verification", name)
				}
			}
			progress("other host discharge", func() error {
				c := closeExchange(b.HostID, clock)
				c.Sessions = []HostSession{{Machine: b.ID, Session: b.Session}}
				ack, err := q.ReconcileHost(c)
				if err == nil && len(ack.Pending) != 0 {
					return fmt.Errorf("discharge still pending: %v", ack.Pending)
				}
				return err
			})
			progress("cancellation", func() error { return q.Cancel(other.ID) })
			progress("check-in", func() error {
				_, err := q.Checkin(Checkin{Machine: machine("c", "fake")})
				return err
			})
			switch fence {
			case "cancel":
				progress("publisher cancellation", func() error { return q.Cancel(publishing.ID) })
			case "close":
				progress("publisher close", func() error {
					c := closeExchange(a.HostID, clock)
					c.Sessions = []HostSession{{Machine: a.ID, Session: a.Session}}
					_, err := q.ReconcileHost(c)
					return err
				})
			case "request-cancel":
				cancel()
			case "interrupt":
				progress("interrupted acknowledgement", func() error {
					interrupted := r
					interrupted.Interrupted, interrupted.PoliteStop = true, true
					return q.Complete(interrupted)
				})
			}
			unblock()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("completion did not return")
			}
			if fence != "request-cancel" && fence != "accept" && response.Code != http.StatusConflict {
				t.Fatalf("expected stale rejection, got %d: %s", response.Code, response.Body.String())
			}
			if fence == "request-cancel" && response.Code < 400 {
				t.Fatalf("cancelled request published: %s", response.Body.String())
			}
			got := job(t, q, publishing.ID)
			if fence == "accept" {
				if response.Code != http.StatusOK || got.State != "succeeded" || got.OutputRefs["out.txt"] != ref {
					t.Fatalf("current verified result not published: %d %+v", response.Code, got)
				}
				if err := q.Complete(r); err != nil {
					t.Fatalf("completion retransmission: %v", err)
				}
				return
			}
			if got.State == "succeeded" || len(got.OutputRefs) != 0 || got.FinishedAt != nil && fence == "request-cancel" {
				t.Fatalf("fenced result published: %+v", got)
			}
			// Neither the stale replay nor the cancellation acknowledgement may
			// enter the verifier again (its injected barrier is single-use).
			if fence == "close" || fence == "interrupt" {
				if err := q.Complete(r); !errors.Is(err, ErrStale) {
					t.Fatalf("closed attempt replay: %v", err)
				}
			} else if fence == "cancel" {
				if err := q.Complete(r); err != nil {
					t.Fatalf("cancellation acknowledgement: %v", err)
				}
				requireState(t, q, publishing.ID, "cancelled")
			}
		})
	}
}
