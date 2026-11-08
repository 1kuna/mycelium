package myce

import (
	"testing"
	"time"
)

func TestResidentPinSurvivesUnknownDeviceMemory(t *testing.T) {
	for _, inventory := range []bool{false, true} {
		name := "legacy"
		if inventory {
			name = "device inventory"
		}
		t.Run(name, func(t *testing.T) {
			q, clock, m := modelFixture(t)
			device := ""
			if inventory {
				m = m3Machine()
				m.ModelURL, m.ModelConfigHash = "http://127.0.0.1:1234", modelConfigHash(q.models)
				machineCheck(t, q, m)
				device = "1"
			}
			if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID, Device: device}); err != nil {
				t.Fatal(err)
			}
			slot := q.s.ModelSlots[slotKey(m.ID, "test", device)]
			attempt := slot.Attempt
			report := func() {
				t.Helper()
				if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Device: device, Attempt: attempt, State: "ready"}}}); err != nil {
					t.Fatal(err)
				}
			}
			report()
			clock.Advance(q.models.Idle + time.Second)
			if inventory {
				m.Devices[1].Reason, m.Devices[1].FreeMemoryGB = "device_probe_unavailable", 0
			} else {
				m.MemoryError, m.FreeMemoryGB = "device_probe_unavailable", 0
			}
			report()
			pin := q.s.Pins[0]
			if pin.Assigned != m.ID || pin.AssignedDevice != device || !q.slotPinned(slot) || slot.State != "ready" || slot.Attempt != attempt {
				t.Fatalf("unknown memory drained the resident pinned attempt: pin=%+v slot=%+v", pin, slot)
			}
			// Recovery cannot replace the attempt that survived the observation gap.
			if inventory {
				m.Devices[1].Reason, m.Devices[1].FreeMemoryGB = "", m.Devices[1].BudgetGB
			} else {
				m.MemoryError, m.FreeMemoryGB = "", m.TotalMemoryGB
			}
			report()
			if got := q.s.ModelSlots[slotKey(m.ID, "test", device)]; got != slot || got.State != "ready" || got.Attempt != attempt || !q.slotPinned(got) {
				t.Fatalf("memory recovery reloaded the pinned engine: %+v", got)
			}
		})
	}
}

func TestUnknownDeviceMemoryDoesNotPlaceNewPinnedEngine(t *testing.T) {
	q, _, m := modelFixture(t)
	// Leave the old numeric free-memory value intact: unknown is not capacity.
	m.MemoryError = "device_probe_unavailable"
	check(t, q, m, nil)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	addModelCall(t, q, "production")
	if q.s.Pins[0].Assigned != "" || len(q.s.ModelSlots) != 0 {
		t.Fatal("unknown memory admitted a new engine", q.s.Pins, q.s.ModelSlots)
	}
}

func TestResidentPinUnknownMemoryStillHonorsReleaseFences(t *testing.T) {
	for _, fence := range []string{"leave", "session", "health", "drain", "unpin", "device fault"} {
		t.Run(fence, func(t *testing.T) {
			q, clock, m := modelFixture(t)
			if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
				t.Fatal(err)
			}
			reportModel(t, q, m, "ready")
			slot := q.s.ModelSlots[slotKey(m.ID, "test")]
			clock.Advance(q.models.Idle + time.Second)
			m.MemoryError = "device_probe_unavailable"
			report := ModelReport{Name: "test", Attempt: slot.Attempt, State: "ready"}
			previousSession := ""
			switch fence {
			case "leave":
				m.Leaving = true
			case "session":
				previousSession = m.Session
				m.Session = "replacement"
			case "health":
				report.HealthDegraded = true
			case "drain":
				slot.State = "draining"
			case "unpin":
				if err := q.Unpin("test", "test"); err != nil {
					t.Fatal(err)
				}
			case "device fault":
				m.MemoryError = "device_fault_nvidia_xid"
			}
			if _, err := q.Checkin(Checkin{Machine: m, Fresh: previousSession != "", PreviousSession: previousSession, ModelReports: []ModelReport{report}}); err != nil {
				t.Fatal(err)
			}
			for _, p := range q.s.Pins {
				if p.Assigned != "" {
					t.Fatalf("%s retained the old pin: %+v", fence, p)
				}
			}
			if q.slotPinned(slot) {
				t.Fatalf("%s kept the old engine protected", fence)
			}
		})
	}
}
