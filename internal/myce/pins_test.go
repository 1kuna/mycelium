package myce

import "testing"

func TestDeviceMaintenancePinsAndRetry(t *testing.T) {
	q, clock, legacy := modelFixture(t)
	legacy.Leaving = true
	check(t, q, legacy, nil)
	m := m3Machine()
	m.ModelURL, m.ModelConfigHash = "http://127.0.0.1:1234", modelConfigHash(q.models)
	machineCheck(t, q, m)
	changeDevice(t, q, m.HostID, "0", "disable")
	pin := ModelPin{Model: "test", Project: "test", Machine: m.ID, Device: "0"}
	if err := q.Pin(pin); err != nil {
		t.Fatal(err)
	}
	if q.s.ModelSlots[slotKey(m.ID, "test", "0")] != nil {
		t.Fatal("pin placed a new engine on the disabled device")
	}
	if err := q.Unpin("test", "test"); err != nil {
		t.Fatal(err)
	}
	pin.Device = "1"
	if err := q.Pin(pin); err != nil {
		t.Fatal(err)
	}
	slot := q.s.ModelSlots[slotKey(m.ID, "test", "1")]
	if slot == nil || slot.State != "loading" {
		t.Fatal("pin could not use enabled sibling", slot)
	}
	for _, state := range []string{"ready", "failed"} {
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "test", Device: "1", Attempt: slot.Attempt, State: state, RuntimeFailed: state == "failed", CallsMeasured: true}}}); err != nil {
			t.Fatal(err)
		}
	}
	if slot.State != "failed" || slot.RetryAt == nil {
		t.Fatal("model failure did not enter retry backoff", slot)
	}
	attempt := slot.Attempt
	changeDevice(t, q, m.HostID, "1", "disable")
	clock.Advance(slot.RetryAt.Sub(clock.Now()))
	machineCheck(t, q, m)
	if slot.State != "failed" || slot.Attempt != attempt {
		t.Fatal("pin retried an engine on a disabled device", slot)
	}
	changeDevice(t, q, m.HostID, "1", "enable")
	if slot.State != "loading" || slot.Attempt == attempt {
		t.Fatal("enable did not restore pinned engine retry", slot)
	}
}
