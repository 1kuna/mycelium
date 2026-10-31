package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestFleetHTTPRequestTiming(t *testing.T) {
	q, _ := fixture(t)
	mux := newRequestMux()
	API{Queue: q}.registerFleetUpgrade(mux)
	for _, path := range []string{"/api/fleet/publisher", "/api/fleet/instruction", "/api/fleet/artifact"} {
		q.measureRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mux.ServeHTTP(w, r)
			s := spanFrom(r.Context())
			if !s.measured || s.hold <= 0 {
				t.Errorf("missing queue phases for %s", path)
			}
		}), mux.patterns).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
}

// Reproduce a log prune owning the sole connection outside q.mu. An inactive
// instruction poll must neither queue for that connection nor block job reads.
func TestFleetInstructionInactiveAvoidsWriter(t *testing.T) {
	for _, state := range []string{"absent", "complete", "stopped"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				q, _ := fixture(t)
				j := submit(t, q, spec("experiment"))
				if state != "absent" {
					q.s.FleetUpgrade = &fleetUpgrade{State: state}
				}
				writer, err := q.db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close()
				before := q.db.Stats().WaitCount
				mux := newRequestMux()
				API{Queue: q}.registerFleetUpgrade(mux)
				response := httptest.NewRecorder()
				done := make(chan struct{})
				go func() {
					defer close(done)
					mux.ServeHTTP(response, httptest.NewRequest("GET", "/api/fleet/instruction?host=fixture&machine=fixture&session=fixture", nil))
				}()
				synctest.Wait()
				// Do not start a mutex-blocked reader in the synctest bubble:
				// mutex waits are not durable waits. Probe before the real read.
				readable := q.mu.TryRLock()
				if readable {
					q.mu.RUnlock()
					if got, err := q.jobSnapshot(j.ID); err != nil || got.ID != j.ID {
						t.Errorf("concurrent job read: %+v %v", got, err)
					}
				}
				waiting := q.db.Stats().WaitCount != before
				select {
				case <-done:
				default:
					t.Error("inactive poll waited for the prune's writer connection")
				}
				// Release before reporting failures so the old implementation also
				// exits cleanly and demonstrates the regression without a timeout.
				if err := writer.Close(); err != nil {
					t.Error(err)
				}
				<-done
				if waiting || !readable || response.Code != http.StatusOK {
					t.Fatalf("writer_wait=%t reader_available=%t status=%d", waiting, readable, response.Code)
				}
				var instruction FleetUpdateInstruction
				if err := json.Unmarshal(response.Body.Bytes(), &instruction); err != nil || instruction.Operation != "" || response.Header().Get("Cache-Control") != "no-store" {
					t.Fatalf("inactive instruction: %+v %v", instruction, err)
				}
			})
		})
	}
}

func TestFleetInstructionPruneAndQueueWrite(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "inactive", true: "active"}[active], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var q *Queue
				var c *fakeClock
				if active {
					q, c, _ = fleetFixture(t, "host", "host")
					if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
						t.Fatal(err)
					}
				} else {
					q, c = fixture(t)
				}
				ended := c.Now().Add(-2 * DefaultLogRetention)
				if err := q.update(func() error {
					q.putJob("expired", &Job{ID: "expired", State: "succeeded", FinishedAt: &ended, ReleaseConfirmed: true})
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := q.db.Exec("INSERT INTO logs(job,attempt,text) VALUES('expired',1,'old')"); err != nil {
					t.Fatal(err)
				}
				writer, err := q.db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close()
				before := q.db.Stats().WaitCount
				pruned := make(chan error, 1)
				go func() { pruned <- q.pruneBatchLogs() }()
				synctest.Wait()
				if q.db.Stats().WaitCount == before {
					t.Fatal("pruner did not reach the held writer")
				}
				// Hold the ordinary write just before it takes q.mu. The pruner
				// and poll are already live; releasing both gates must drain all
				// three operations without introducing a writer -> q.mu edge.
				writeReady, allowWrite := make(chan struct{}), make(chan struct{})
				q.checkpointProbe = func() bool { close(writeReady); <-allowWrite; return true }
				written := make(chan error, 1)
				front := q.s.Front
				go func() { written <- q.update(func() error { q.s.Front++; return nil }) }()
				<-writeReady
				q.checkpointProbe = func() bool { return true }
				mux := newRequestMux()
				API{Queue: q}.registerFleetUpgrade(mux)
				response := httptest.NewRecorder()
				polled := make(chan struct{})
				go func() {
					defer close(polled)
					mux.ServeHTTP(response, httptest.NewRequest("GET", "/api/fleet/instruction?host=host&machine=@queue&session=queue-old", nil))
				}()
				synctest.Wait()
				close(allowWrite)
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				if err := <-written; err != nil {
					t.Error(err)
				}
				if err := <-pruned; err != nil {
					t.Error(err)
				}
				<-polled
				if q.s.Front != front+1 {
					t.Error("ordinary queue mutation did not commit")
				}
				if response.Code != http.StatusOK {
					t.Fatal(response.Code, response.Body.String())
				}
				var rows int
				if err := q.db.QueryRow("SELECT COUNT(*) FROM logs WHERE job='expired'").Scan(&rows); err != nil || rows != 0 {
					t.Fatalf("prune not completed: rows=%d err=%v", rows, err)
				}
			})
		})
	}
}

func TestFleetInstructionSlowSpanWriterPhases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q, _, _ := fleetFixture(t, "host", "host")
		if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		old := log.Writer()
		log.SetOutput(&logs)
		defer log.SetOutput(old)
		writer, err := q.db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		mux := newRequestMux()
		API{Queue: q}.registerFleetUpgrade(mux)
		done := make(chan struct{})
		go func() {
			defer close(done)
			q.measureRequests(mux, mux.patterns).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/fleet/instruction?host=private-host", nil))
		}()
		synctest.Wait()
		// synctest advances virtual wall time; no real sleep or queue-clock
		// change is involved in this observational timing check.
		<-time.After(2 * time.Second)
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		<-done
		line := logs.String()
		if strings.Count(line, "event=slow_request") != 1 || !strings.Contains(line, "writer_wait_ms=2000.000 work_ms=0.000") || !strings.Contains(line, "mutex_wait_ms=0.000") || strings.Contains(line, "private-host") {
			t.Fatal(line)
		}
	})
}
