package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Reads must finish while a transaction is suspended with q.mu held, see only
// its preceding commit, and never share mutable output with other readers.
// The same check covers a successful commit and a rollback under -race.
func TestCommittedReadDuringUpdate(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, spec("experiment"))
	handler := API{Queue: q, Token: "test"}.Handler()
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint(rollback), func(t *testing.T) {
			before := q.s.Jobs[j.ID].Spec.Name
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			abort := errors.New("rollback fixture")
			go func() {
				done <- q.update(func() error {
					q.s.Jobs[j.ID].Spec.Name = "next-" + fmt.Sprint(rollback)
					q.s.Jobs[j.ID].Reason = "same transaction"
					close(entered)
					<-release
					if rollback {
						return abort
					}
					return nil
				})
			}()
			<-entered
			var wg sync.WaitGroup
			errs := make(chan error, 124)
			for i := range 124 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					path := "/api/jobs/" + j.ID
					if i%31 == 0 {
						path = "/api/status?jobs=active"
					}
					r := httptest.NewRequest("GET", path, nil)
					r.Header.Set("Authorization", "Bearer test")
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					if w.Code != 200 {
						errs <- fmt.Errorf("GET %s: %d", path, w.Code)
						return
					}
					var got Job
					if i%31 == 0 {
						var s Snapshot
						if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil || len(s.Jobs) != 1 {
							errs <- fmt.Errorf("status jobs: %d, %v", len(s.Jobs), err)
							return
						}
						got = *s.Jobs[0]
					} else if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
						errs <- err
						return
					}
					if got.Spec.Name != before || got.Reason == "same transaction" {
						errs <- fmt.Errorf("uncommitted job: %+v", got)
					}
					got.Spec.Needs.Platform.Required = append(got.Spec.Needs.Platform.Required, "changed")
				}(i)
			}
			readDone := make(chan struct{})
			go func() { wg.Wait(); close(readDone) }()
			select {
			case <-readDone:
			case <-time.After(5 * time.Second):
				t.Error("GETs waited for an uncommitted writer")
			}
			close(release)
			if err := <-done; !errors.Is(err, abort) && rollback || err != nil && !rollback {
				t.Fatal(err)
			}
			<-readDone
			close(errs)
			for err := range errs {
				t.Error(err)
			}
			got, err := q.jobSnapshot(j.ID)
			if err != nil || got.Spec.Name != q.s.Jobs[j.ID].Spec.Name {
				t.Fatal("commit/rollback visibility", err)
			}
			got.Spec.Needs.Platform.Any = false
			fresh, _ := q.jobSnapshot(j.ID)
			if !fresh.Spec.Needs.Platform.Any {
				t.Fatal("caller changed the published generation")
			}
		})
	}
}
