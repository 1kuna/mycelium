package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// #2171: the final PATCH publishes before it answers (syncs, rename, unlinks).
// On a busy network share that outlasted the response window learned from
// plain chunks, and the client abandoned a PATCH the server then completed.
// Scaled down from the soak: chunks answer in 250 ms, so the window is the
// 1 s RFC 6298 floor, and publication takes 1.4 s in two 700 ms directory
// syncs. The server's injected clock advances with each step, as real time
// does, so its progress rate limit sees the steps' real spacing.
func TestUploadFinalPatchPublicationProgress(t *testing.T) {
	const step = 700 * time.Millisecond
	setup := func(t *testing.T, heartbeat bool, sync func(dir string, original func(string) error) error) (Client, FileRef) {
		q, s := creationStore(t, 32)
		clock := q.clock.(*fakeClock)
		original := s.syncDir
		s.syncDir = func(dir string) error {
			if !strings.HasSuffix(dir, "blobs") && !strings.HasSuffix(dir, "uploads") {
				return original(dir)
			}
			err := sync(dir, original)
			clock.Advance(step)
			return err
		}
		backend := API{Queue: q, Token: "fixture"}.Handler()
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !heartbeat {
				r.Header.Del(uploadFinalizingHeader)
			}
			backend.ServeHTTP(w, r)
		}))
		t.Cleanup(api.Close)
		c := Client{URL: api.URL, Token: "fixture", publication: &publicationProgress{},
			Pause: func(context.Context, time.Duration) error { return context.Canceled }}
		c.publication.timing.completed(uploadRoute("PATCH"), 250*time.Millisecond, 0, 0, 0, true, clock.Now())
		if got := c.publication.timing.window(uploadRoute("PATCH"), 0); got != clientRecoveryPeriod {
			t.Fatalf("learned chunk window %s, want the %s floor", got, clientRecoveryPeriod)
		}
		return c, creationRef("final")
	}

	t.Run("publishing", func(t *testing.T) {
		c, ref := setup(t, false, func(dir string, original func(string) error) error {
			time.Sleep(step)
			return original(dir)
		})
		started := time.Now()
		if err := c.uploadFileReader(context.Background(), strings.NewReader("final"), ref, ""); err != nil {
			t.Fatalf("abandoned a final PATCH that was still publishing after %s: %v", time.Since(started).Round(time.Millisecond), err)
		}
	})

	t.Run("silent legacy peer", func(t *testing.T) {
		release := make(chan struct{})
		c, ref := setup(t, false, func(dir string, original func(string) error) error {
			<-release // A sync that never completes reports nothing.
			return original(dir)
		})
		defer close(release)
		started := time.Now()
		err := c.uploadFileReader(context.Background(), strings.NewReader("final"), ref, "")
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("a silent server's PATCH succeeded")
		}
		if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "timeout") {
			t.Fatalf("unexpected failure: %v", err)
		}
		// One window after the last completed step, plus scheduling slack.
		if elapsed < clientRecoveryPeriod || elapsed > 3*clientRecoveryPeriod {
			t.Fatalf("silent publication given up after %s, want about %s", elapsed.Round(time.Millisecond), clientRecoveryPeriod)
		}
	})

	t.Run("caller deadline during heartbeat", func(t *testing.T) {
		release := make(chan struct{})
		entered := make(chan struct{})
		c, ref := setup(t, true, func(dir string, original func(string) error) error {
			if strings.HasSuffix(dir, "blobs") {
				close(entered)
				<-release
			}
			return original(dir)
		})
		defer close(release)
		ctx, cancel := context.WithTimeout(t.Context(), 2*clientRecoveryPeriod)
		defer cancel()
		err := c.uploadFileReader(ctx, strings.NewReader("final"), ref, "")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("owner deadline did not interrupt finalization: %v", err)
		}
		select {
		case <-entered:
		default:
			t.Fatal("deadline expired before the final publication barrier")
		}
	})
}
