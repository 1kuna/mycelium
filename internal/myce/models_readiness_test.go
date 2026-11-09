package myce

import "testing"

// A ready engine must beat a colder, cheaper host without advancing the clock.

// Last-resort policy cannot strand a call while an idle ready engine exists.
func TestModelLoadedAffinityLastResortHost(t *testing.T) {
	t.Parallel()
	q, clock, warm := modelFixture(t)
	first := addModelCall(t, q, "experiment")
	reportModel(t, q, warm, "ready")
	if err := q.finishCall(first.ID); err != nil {
		t.Fatal(err)
	}
	lastResortPolicy(t, q, hostID(&warm), true)
	cold := machine("aaa", "fake")
	cold.ModelURL = "http://127.0.0.1:1235"
	cold.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, cold, nil)
	now := clock.Now()
	call := addModelCall(t, q, "production")
	if call.Machine != warm.ID || len(q.s.ModelSlots) != 1 || !clock.Now().Equal(now) {
		t.Fatalf("ready last-resort engine must win immediately: machine=%q slots=%d", call.Machine, len(q.s.ModelSlots))
	}

	t.Run("ModelLoadedAffinityScarceHost", func(t *testing.T) {

		q, clock, warm := modelFixture(t)
		first := addModelCall(t, q, "experiment")
		reportModel(t, q, warm, "ready")
		if err := q.finishCall(first.ID); err != nil {
			t.Fatal(err)
		}
		cold := machine("aaa", "fake")
		cold.CapabilityProfile.SystemMemoryBytes = 16 << 30
		cold.CapabilityProfile.Accelerators[0].MemoryBytes = 8 << 30
		cold.ModelURL = "http://127.0.0.1:1235"
		cold.ModelConfigHash = modelConfigHash(q.models)
		check(t, q, cold, nil)

		now := clock.Now()
		call := addModelCall(t, q, "production")
		if call.Machine != warm.ID || len(q.s.ModelSlots) != 1 || !clock.Now().Equal(now) {
			t.Fatalf("ready engine must win immediately: machine=%q slots=%d", call.Machine, len(q.s.ModelSlots))
		}

	})
}
