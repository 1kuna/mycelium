package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #936: loss during staging makes its diagnostic log stale. That refusal is
// not a release acknowledgement, even though the payload never launched.
func TestStaleStagingReplayRetainsSenderUntilReleaseAcknowledged(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		w := recoveryWorker(t)
		w.Clock, w.control = clock, &processControl{clock: clock}
		w.Machine.Capabilities = append(w.Machine.Capabilities, "shared_inputs_v1")
		w.Machine.SharedInputCache = &SharedInputCapacity{CapacityBytes: 1 << 20, FreeBytes: 1 << 20}
		nas := t.TempDir()
		ref := sharedFixtureFile(t, nas, "input", "staged fixture")
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
		s := spec("experiment")
		s.Resumable = false
		s.SharedInputs = map[string]SharedInput{"source": {NAS: "input", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
		check(t, q, w.Machine, nil)
		victim := submit(t, q, s)
		assigned := check(t, q, w.Machine, nil).Job
		if assigned == nil || assigned.ID != victim.ID {
			t.Fatal("staging attempt not assigned")
		}
		store, err := newAttemptStore(root, *assigned)
		if err != nil {
			t.Fatal(err)
		}
		entered, unblock := make(chan struct{}), make(chan struct{})
		cache.beforeStage = func(context.Context) { close(entered); <-unblock }
		lost, allowRelease := make(chan struct{}), make(chan struct{})
		releases := make(chan Result, 2)
		api := API{Queue: q, Token: "test"}.Handler()
		releaseCalls := 0 // The single actor serializes these transport requests.
		w.Client = Client{URL: "http://worker-test.invalid", Token: "test", HTTP: &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
			recorder := httptest.NewRecorder()
			if r.URL.Path == "/api/workers/log" {
				select {
				case <-lost: // Commit loss before allowing stale replay.
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			}
			if r.URL.Path == "/api/workers/complete" {
				var release Result
				if err := json.NewDecoder(r.Body).Decode(&release); err != nil {
					return nil, err
				}
				releaseCalls++
				releases <- release
				if releaseCalls == 1 {
					http.Error(recorder, "temporarily unavailable", http.StatusServiceUnavailable)
					return recorder.Result(), nil
				}
				select {
				case <-allowRelease:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
				respondOperation(recorder, nil, q.Complete(release), "worker_result_invalid")
				return recorder.Result(), nil
			}
			api.ServeHTTP(recorder, r)
			return recorder.Result(), nil
		})}}
		ctx, cancel := context.WithCancel(context.Background())
		a := &attemptSupervisor{store: store, job: *assigned, worker: w, connected: true, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.run(ctx)
		defer func() {
			cancel()
			close(unblock)
			select {
			case <-a.done:
			case <-time.After(testWaitTimeout):
				t.Error("actor did not stop")
			}
			if pending := onlySharedStaging(t, cache); pending != nil {
				<-pending.done
			}
		}()
		select {
		case <-entered:
		case <-time.After(testWaitTimeout):
			t.Fatal("staging did not enter")
		}
		clock.Advance(LossAfter + time.Second)
		// Another worker stays healthy; this is an individual loss, not a
		// fleet-wide network silence that intentionally preserves attempts.
		check(t, q, observer(), nil)
		close(lost)
		before := job(t, q, victim.ID)
		if before.State != "queued" || len(before.Copies) != 1 || before.Copies[0].ReleaseConfirmed {
			t.Fatalf("loss did not retain exact copy custody: %+v", before)
		}
		for range 2 {
			select {
			case release := <-releases:
				if release.Execution != (Execution{ID: assigned.ID, Attempt: assigned.Attempt}) || release.Machine != assigned.Machine || release.Session != assigned.Session || !release.ReleaseOnly || release.ReleasePending {
					t.Fatalf("not an exact confirmed release: %+v", release)
				}
			case <-a.done:
				t.Fatal("stale replay retired the only release sender")
			case <-time.After(testWaitTimeout):
				t.Fatal("stale replay did not send/retry release")
			}
		}
		data, err := os.ReadFile(filepath.Join(store.dir, store.stateName))
		var retained workerState
		if err != nil || json.Unmarshal(data, &retained) != nil || retained.Job == nil || retained.Result == nil || retained.Offset != 0 || retained.Pending == 0 {
			t.Fatalf("unacknowledged release discarded durable custody: %s %v", data, err)
		}
		if got := job(t, q, victim.ID); got.Copies[0].ReleaseConfirmed {
			t.Fatal("failed release response settled custody")
		}
		close(allowRelease)
		select {
		case <-a.done:
		case <-time.After(testWaitTimeout):
			t.Fatal("acknowledged actor did not retire")
		}
		_, _, finished, err := a.snapshot()
		after := job(t, q, victim.ID)
		if err != nil || !finished || store.state.Job != nil || store.state.Result != nil || after.State != before.State || after.Attempt != before.Attempt || !after.Copies[0].ReleaseConfirmed || after.StartedAt != nil {
			t.Fatalf("release changed outcome or retained custody: finished=%t error=%v job=%+v", finished, err, after)
		}
		if _, err := os.Stat(filepath.Join(store.dir, "job-files", victim.ID, "1")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("failed staging reached payload preparation", err)
		}

	})

	t.Run("StopKillAcknowledgementSendsOneCompletion", func(t *testing.T) {
		w := recoveryWorker(t)
		w.control = &processControl{clock: w.Clock}
		root, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer root.close()
		j := recoveryJob(t, []string{"echo", "done"})
		j.Spec.Priority, j.Spec.Resumable = "production", false
		j.Machine, j.Session = w.Machine.ID, "old-session"
		store, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		finished := result(&j, true)
		store.state.Result = &finished
		if err := store.save(); err != nil {
			t.Fatal(err)
		}
		var completions []Result // Read only after the actor has joined.
		w.Client.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path != "/api/workers/complete" {
				return nil, errors.New("unexpected request: " + r.URL.Path)
			}
			var got Result
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				return nil, err
			}
			completions = append(completions, got)
			recorder := httptest.NewRecorder()
			recorder.Write([]byte(`{}`))
			return recorder.Result(), nil
		})}
		ctx, cancel := context.WithCancel(context.Background())
		a := &attemptSupervisor{store: store, job: j, worker: w, connected: true, directive: AttemptDirective{Stop: true, Kill: true}, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go a.run(ctx)
		defer func() {
			cancel()
			select {
			case <-a.done:
			case <-time.After(testWaitTimeout):
				t.Error("actor did not stop")
			}
		}()
		select {
		case <-a.done:
		case <-time.After(testWaitTimeout):
			t.Fatal("acknowledged stop did not retire")
		}
		if len(completions) == 0 {
			t.Fatal("acknowledged stop sent no release")
		}
		for _, got := range completions {
			if got.Execution != (Execution{ID: j.ID, Attempt: j.Attempt}) || got.Machine != j.Machine || got.Session != j.Session || !got.ReleaseOnly || got.ReleasePending {
				t.Fatalf("not the exact confirmed release: %+v", got)
			}
		}
		if store.state.Job != nil || store.state.Result != nil {
			t.Fatal("acknowledged stop retained custody")
		}

	})

	t.Run("ProductionStartDeadlineWorkerSendsRejectedResultCleanup", func(t *testing.T) {
		w := recoveryWorker(t)

		store, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		j := recoveryJob(t, []string{"echo", "done"})
		j.Spec.Priority = "production"
		deadline := w.Clock.Now().Add(-time.Second)
		j.Spec.StartBy = &deadline
		j.Machine, j.Session = w.Machine.ID, "old-session"
		r := result(&j, true)
		store.state.Session, store.state.Job, store.state.Result = j.Session, &j, &r
		if err = store.save(); err != nil {
			t.Fatal(err)
		}
		store.close()
		ctx, cancel := context.WithCancel(context.Background())
		// Failure teardown must release handlers before joining the gracefully
		// stopping worker, and keep the server alive until that join completes.
		teardown := make(chan struct{})
		entered := make(chan struct{}, 1)
		blockCompletion := os.Getenv("MYCE_TEST_BLOCK_DEADLINE_COMPLETION") == "1"
		completed := make(chan Result, 1)
		server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/api/workers/checkin":
				var c Checkin
				json.NewDecoder(req.Body).Decode(&c)
				c, err := normalizeAttemptCheckin(c)
				if err != nil {
					t.Error(err)
					return
				}
				reply := Reply{CheckpointRoot: filepath.Dir(j.Checkpoint)}
				if len(c.Attempts) == 0 {
					cancel()
				} else {
					if len(c.Attempts) != 1 {
						t.Error("expected one held attempt")
						return
					}
					a := c.Attempts[0]
					reply.Attempts = []AttemptDirective{{Execution: a.Execution, Session: a.Session, Stop: true, Kill: true}}
				}
				json.NewEncoder(rw).Encode(attemptReply(c, reply))
			case "/api/workers/complete":
				var got Result
				json.NewDecoder(req.Body).Decode(&got)
				if blockCompletion {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-teardown
				}
				select {
				case completed <- got:
				default:
					t.Error("unexpected duplicate cleanup completion")
				}
				rw.Write([]byte(`{}`))
			default:
				rw.Write([]byte(`{}`))
			}
		}))
		w.Client.URL = server.URL
		done := make(chan struct{})
		var runErr error
		t.Cleanup(func() {
			cancel()
			close(teardown)
			<-done
			server.Close()
			if blockCompletion {
				fmt.Println("blocked-completion teardown joined")
			}
		})
		go func() {
			defer close(done)
			runErr = w.Run(ctx)
		}()
		if blockCompletion {
			select {
			case <-entered:
				t.Fatal("intentional failure with completion handler blocked")
			case <-done:
				t.Fatalf("worker exited before blocked completion: %v", runErr)
			case <-time.After(testWaitTimeout):
				t.Fatal("completion handler was not reached")
			}
		}
		select {
		case <-done:
			err = runErr
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(testWaitTimeout):
			t.Fatal("worker stalled")
		}
		select {
		case got := <-completed:
			if !got.ReleaseOnly || got.ID != j.ID || got.Session != j.Session {
				t.Fatalf("cleanup acknowledgement: %+v", got)
			}
		default:
			t.Fatal("verified cleanup was discarded with the stale result")
		}

	})
}
