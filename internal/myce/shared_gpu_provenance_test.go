package myce

import (
	"strings"
	"testing"
	"time"
)

func TestSharedDeviceHostOnlyLearningKeepsCard(t *testing.T) {
	for _, layout := range []string{"separate", "unified"} {
		t.Run(layout, func(t *testing.T) {
			q, clock := fixture(t)
			m := sharedUnifiedMachine()
			m.CapabilityProfile.MemoryLayout = layout
			m.Devices[0].MemoryMode = layout
			m.Devices[0].UnifiedMemory = layout == "unified"
			m.Devices[0].ExtensionFingerprint = "synthetic-cuda-v1"
			s := plainGPUSpec()
			s.Needs.Memory = "auto"
			if layout == "separate" {
				// A recipe/bootstrap hint survives host-only learning as a
				// positive device default, which used to authorize sharing.
				s.Needs.MemoryGB, s.Needs.HostMemoryGB = 8, 1
			}
			seed := assignGPU(t, q, m, submit(t, q, s))
			if err := q.Started(result(seed, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			host := 1.0
			r := result(seed, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}

			// A host peak alone must not authorize joining a declared tenant.
			blocker := assignGPU(t, q, m, submit(t, q, sharedSpec("experiment", 13)))
			queued := submit(t, q, s)
			if got := assignedJob(machineCheck(t, q, m, blocker)); got != nil {
				t.Fatal("host-only learning authorized joining a busy card", got.MemoryEnvelope)
			}
			finish(t, q, blocker, true)
			unknown := assignedJob(machineCheck(t, q, m))
			if unknown == nil || unknown.ID != queued.ID {
				t.Fatal("unmeasured work could not run alone", job(t, q, queued.ID).Reason)
			}
			if layout == "separate" && (unknown.MemoryEnvelope.Basis != "exploratory" || !strings.Contains(unknown.MemoryEnvelope.Source, "unknown device peak; exclusive card")) {
				t.Fatal("host-only evidence lost exploratory card exclusivity", unknown.MemoryEnvelope)
			}
			if err := q.Started(result(unknown, false)); err != nil {
				t.Fatal(err)
			}
			// Nor may a declared tenant join an already running default vector.
			peer := submit(t, q, sharedSpec("experiment", 13))
			if got := assignedJob(machineCheck(t, q, m, unknown)); got != nil {
				t.Fatal("declared work joined an unmeasured tenant", got.ID)
			}

			// Once a complete run measures device use, auto work may share again.
			clock.Advance(time.Second)
			device := 8.0
			r = result(unknown, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: &device}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			runningPeer := assignedJob(machineCheck(t, q, m))
			if runningPeer == nil || runningPeer.ID != peer.ID {
				t.Fatal("declared peer did not start after release")
			}
			next := submit(t, q, s)
			measured := assignedJob(machineCheck(t, q, m, runningPeer))
			if measured == nil || measured.ID != next.ID || measured.MemoryEnvelope.Basis != "learned" || measured.Spec.Needs.MemoryGB != 8+minimumHostMemoryGB || strings.Contains(measured.MemoryEnvelope.Source, "GPU recipe default") {
				t.Fatal("accepted device evidence did not enable sharing", measured, job(t, q, next.ID).Reason)
			}
		})
	}
}
