package myce

import (
	"encoding/json"
	"errors"
	"testing"
)

// Rollback must restore both a keyed answer and nested job state. Exercise cold
// and live records, including entering through the key before touching the job.
func TestQueueRecordsKeyedRollback(t *testing.T) {
	for _, cold := range []bool{false, true} {
		for _, fromKey := range []bool{false, true} {
			t.Run(map[bool]string{false: "live", true: "cold"}[cold]+"/"+map[bool]string{false: "job", true: "key"}[fromKey], func(t *testing.T) {
				q, _ := fixture(t)
				s := spec("production")
				s.SubmissionKey = "undo"
				j := submit(t, q, s)
				if cold {
					run := check(t, q, machine("worker", "fake"), nil).Job
					finish(t, q, run, true)
				}
				before, err := json.Marshal(q.s)
				if err != nil {
					t.Fatal(err)
				}
				rejected := errors.New("rejected")
				if err := q.update(func() error {
					var live *Job
					if fromKey {
						live = q.touchSubmission(submissionDigest(s.Project, s.SubmissionKey)).Job
					} else {
						live = q.touchJob(j.ID)
					}
					live.Spec.Run.Command[0] = "mutated"
					live.Reason = "mutated"
					return rejected
				}); !errors.Is(err, rejected) {
					t.Fatal(err)
				}
				after, err := json.Marshal(q.s)
				if err != nil {
					t.Fatal(err)
				}
				if string(before) != string(after) {
					t.Fatal("job or retained key changed after rollback")
				}
				if _, err := q.CancelJob(j.ID); err != nil {
					t.Fatal(err)
				}
				replay := submit(t, q, s)
				if replay.CancelledAt == nil || replay.ID != j.ID {
					t.Fatal("rollback broke key alias", replay)
				}
			})
		}
	}
}
