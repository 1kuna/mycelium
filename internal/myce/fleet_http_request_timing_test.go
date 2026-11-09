package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
)

// Reproduce a log prune owning the sole connection outside q.mu. An inactive
// instruction poll must neither queue for that connection nor block job reads.

// Retained regression: #474 held writer must not stall inactive instruction read.
func TestFleetInstructionInactiveAvoidsWriter(t *testing.T) {
	for _, state := range []string{"absent"} {
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
