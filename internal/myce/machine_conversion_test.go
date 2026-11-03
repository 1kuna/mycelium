package myce

import (
	"errors"
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

func TestMachineConversionFencesQueuedExclusions(t *testing.T) {
	q, _, mapping := m4Queue(t)
	s := spec("experiment")
	s.ExcludedMachines = []string{"host-gpu1"}
	j := submit(t, q, s)
	for _, dry := range []bool{true, false} {
		_, err := q.ConvertMachines(mapping, ModelConfig{}, dry)
		var blocked *QueuedRetiredMachinesError
		if !errors.As(err, &blocked) || !reflect.DeepEqual(blocked.Jobs, []string{j.ID}) {
			t.Fatal("conversion lost exclusion", err)
		}
	}
	if q.s.Machines["host-gpu1"].Replacement != nil {
		t.Fatal("refused conversion retired excluded machine")
	}
}

func TestMachineRollbackFencesQueuedExclusions(t *testing.T) {
	q, _, mapping := m4Queue(t)
	if _, err := q.ConvertMachines(mapping, ModelConfig{}, false); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.ExcludedMachines = []string{"host"}
	j := submit(t, q, s)
	for _, dry := range []bool{true, false} {
		err := q.RollbackMachines(dry)
		var blocked *QueuedRetiredMachinesError
		if !errors.As(err, &blocked) || !reflect.DeepEqual(blocked.Jobs, []string{j.ID}) {
			t.Fatal("rollback lost exclusion", err)
		}
	}
}
