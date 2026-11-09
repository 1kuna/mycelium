package myce

import (
	"encoding/json"
	"testing"
)

// #1110: a nonzero idle baseline, followed by useful work around 2% PSI,
// drained and froze indefinitely under the old detector. Use its unchanged
// positive-band branch (pressurePSI=false) to replay that exact mechanism.
func TestHostHygieneIdlePSIAdmissionReplay(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		name := "new_worker"
		if legacy {
			name = "legacy_worker"
		}
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			h := HostHealth{}
			sample := func(psi, swap, wake float64) {
				clock.Advance(signalRefresh)
				h.observe(HostHealthSample{At: clock.Now(), HostHealthReading: HostHealthReading{
					Pressure: psi, SwapBytes: swap, WakeSeconds: wake, Load: 1, pressurePSI: !legacy,
				}})
			}
			for range hostHealthHistorySize {
				sample(.02, 0, .001)
			}
			_, limit := healthBand(h.Baseline, func(s HostHealthSample) float64 { return s.Pressure })
			for range hostHealthHistorySize + hostHealthConfirmation {
				sample(2, 0, .001)
			}
			if legacy && (h.DrainReason != "host_memory_pressure" || len(h.Baseline) != hostHealthHistorySize || h.Baseline[0].Pressure != .02) {
				t.Fatal("old positive-band detector did not reproduce frozen drain", h)
			}
			if !legacy && h.DrainReason != "" {
				t.Fatal("ordinary working PSI drained new worker", h.DrainReason)
			}
			t.Logf("idle limit=%g; after %d working samples: worker drain=%q, baseline=%d", limit, hostHealthHistorySize+hostHealthConfirmation, h.DrainReason, len(h.Baseline))
			m := cpuMachine("worker", "host", "cpu", 4, 31)
			m.HostAvailableMemoryGB = 17
			report := func() {
				t.Helper()
				// The old report loses its local source flag, exactly as on wire.
				data, err := json.Marshal(h)
				if err != nil {
					t.Fatal(err)
				}
				m.HostHealth = &HostHealth{}
				if err := json.Unmarshal(data, m.HostHealth); err != nil {
					t.Fatal(err)
				}
				check(t, q, m, nil)
			}
			report()
			stored := q.s.Machines[m.ID]
			if got := q.machineLimitReason(stored); got != "" || !q.hostLaunchAllowed(stored, m.Session) {
				t.Fatal("ordinary PSI blocked admission", got)
			}
			if !q.hostMaintenanceStatus(m.HostID).Healthy {
				t.Fatal("consumer health still reports degraded")
			}
			e := stored.HostHealth.PressureEvaluation
			if e == nil || e.BaselineSamples != hostHealthHistorySize || e.RecoverySamples != hostHealthConfirmation || len(e.Corroboration) != 0 || legacy && e.PressureLimit != limit {
				t.Fatal("missing effective band/recovery evidence", e)
			}
			if legacy && (e.ReportedDrainReason != "host_memory_pressure" || m.HostHealth.DrainReason != "host_memory_pressure") {
				t.Fatal("lost original worker diagnosis")
			}
			s := cpuSpec(1, 1)
			s.Priority = "production"
			job := submit(t, q, s)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != job.ID {
				t.Fatal("production did not receive an assignment")
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal("production start refused", err)
			}
			finish(t, q, run, true)
			// Genuine pressure must still drain even after the new worker has
			// learned ordinary PSI. Swap alone supplies independent harm here.
			for i := range hostHealthConfirmation {
				sample(6, float64(i+1)*(1<<20), .001)
			}
			report()
			stored = q.s.Machines[m.ID]
			if h.DrainReason != "host_memory_pressure" || stored.HostHealth.DrainReason != "host_memory_pressure" || q.hostLaunchAllowed(stored, m.Session) || q.hostMaintenanceStatus(m.HostID).Healthy {
				t.Fatal("genuine pressure did not drain")
			}
			if e := stored.HostHealth.PressureEvaluation; len(e.Corroboration) != 1 || e.Corroboration[0] != "swap_growth" {
				t.Fatal("missing swap corroboration", e)
			}
			// A latch caused by real harm also clears after three distinct
			// healthy completions, even when work sustains above-idle PSI.
			for range hostHealthConfirmation {
				sample(2, hostHealthConfirmation*(1<<20), .001)
			}
			report()
			if got := q.hostHygieneReason(q.s.Machines[m.ID]); got != "" {
				t.Fatal("sustained ordinary work did not recover", got)
			}
		})
	}
}

func TestHostHygienePSIWakeCorroboration(t *testing.T) {
	q, clock := fixture(t)
	h := HostHealth{}
	sample := func(wake float64) {
		clock.Advance(signalRefresh)
		h.observe(HostHealthSample{At: clock.Now(), HostHealthReading: HostHealthReading{Pressure: 2, WakeSeconds: wake, pressurePSI: true}})
	}
	for range hostHealthWarmup {
		sample(.001)
	}
	for range hostHealthConfirmation {
		clock.Advance(signalRefresh)
		h.observe(HostHealthSample{At: clock.Now(), HostHealthReading: HostHealthReading{Pressure: 6, WakeSeconds: 1, pressurePSI: true}})
	}
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	m.HostHealth = &h
	check(t, q, m, nil)
	stored := q.s.Machines[m.ID]
	if stored.HostHealth.DrainReason != "host_memory_pressure" || q.hostLaunchAllowed(stored, m.Session) {
		t.Fatal("PSI with local latency did not drain")
	}
	if e := stored.HostHealth.PressureEvaluation; len(e.Corroboration) != 1 || e.Corroboration[0] != "wake_latency" {
		t.Fatal("missing wake corroboration", e)
	}
}
