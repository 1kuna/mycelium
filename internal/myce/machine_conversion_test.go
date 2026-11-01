package myce

import (
	"reflect"
	"testing"
	"time"
)

func TestM4QueuePreservesPausedJob(t *testing.T) {
	q, clock, mapping := m4Queue(t)
	s := spec("experiment")
	s.SubmissionKey = "paused-through-conversion"
	j := submit(t, q, s)
	until := clock.Now().Add(time.Hour)
	paused := pauseForTest(t, q, j.ID, &until)
	if _, err := q.ConvertMachines(mapping, ModelConfig{}, false); err != nil {
		t.Fatal(err)
	}
	q = reopenCancellationQueue(t, q, clock)
	got := submit(t, q, s)
	if got.ID != j.ID || got.State != "paused" || got.PauseRevision != paused.PauseRevision || !reflect.DeepEqual(got.Pause, paused.Pause) || !reflect.DeepEqual(got.Spec, j.Spec) || got.Queued != j.Queued || got.WaitingSince != j.WaitingSince || got.Front != j.Front {
		t.Fatal("conversion/reopen lost paused submission", got)
	}
	if resumed, err := q.ResumeJob(j.ID, paused.PauseRevision); err != nil || resumed.State != "queued" {
		t.Fatal("converted job cannot resume", resumed, err)
	}
}
