package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// A reporting sensor can be fresh at send time while its report spends longer
// than the entire frozen validity behind queue transactions.
func TestHostHealthQueueDeliveryDelay(t *testing.T) {
	q, clock := fixture(t)
	m := machine("reporting-health", "fake")
	observation := &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}
	report := func(age float64) Machine {
		next := m
		next.HostHealth = &HostHealth{Probe: observation, AgeSeconds: age, History: []HostHealthSample{{At: clock.Now()}}}
		return next
	}
	check(t, q, report(.01), nil)
	for _, delay := range []time.Duration{4 * time.Second, 12 * time.Second, 6 * time.Second} {
		clock.Advance(time.Second)
		incoming := report(.01)
		q.mu.Lock()
		done := make(chan error, 1)
		go func() { _, err := q.CheckinContext(context.Background(), Checkin{Machine: incoming}); done <- err }()
		await(t, func() bool {
			q.contactMu.Lock()
			defer q.contactMu.Unlock()
			c := q.contacts[m.ID]
			return c != nil && len(c.pending) != 0
		})
		clock.Advance(delay)
		// A maintenance turn may win the mutex before the replacement check-in.
		reasonBefore := q.hostHygieneReason(q.s.Machines[m.ID])
		q.mu.Unlock()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		reasonAfter := q.hostHygieneReason(q.s.Machines[m.ID])
		if reasonBefore != "" || reasonAfter != "" {
			t.Fatalf("queue delay %s invalidated reporting sensor: before=%q after=%q", delay, reasonBefore, reasonAfter)
		}
	}
}

func TestHostHealthQueueDelayStoppedSensor(t *testing.T) {
	for _, reporting := range []bool{false, true} {
		t.Run(map[bool]string{false: "silent", true: "reporting"}[reporting], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("stopped-health", "fake")
			frozen := &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}
			at := clock.Now()
			m.HostHealth = &HostHealth{Probe: frozen, AgeSeconds: .01, History: []HostHealthSample{{At: at}}}
			check(t, q, m, nil)
			q.mu.Lock()
			clock.Advance(12 * time.Second)
			// Queue occupancy cannot consume a replacement opportunity.
			if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "" {
				t.Fatal(reason)
			}
			q.mu.Unlock()
			if reporting {
				// The stopped sensor's worker-local age includes the entire queue stall.
				m.HostHealth.AgeSeconds += clock.Now().Sub(at).Seconds()
				check(t, q, m, nil)
			} else {
				remaining := frozen.freshness() - 10*time.Millisecond
				clock.Advance(remaining - time.Nanosecond)
				if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "" {
					t.Fatal("expired before measured opportunity bound", reason)
				}
				clock.Advance(time.Nanosecond)
			}
			if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
				t.Fatal("stopped sensor renewed", reason)
			}
			t.Logf("stopped sensor expired: reporting=%v wall=%s, frozen opportunity allowance=%s", reporting, clock.Now().Sub(at), frozen.freshness())
		})
	}
}

func TestHostHealthQueueDelayFrozenStatus(t *testing.T) {
	q, clock := fixture(t)
	m := machine("snapshot-health", "fake")
	m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}, AgeSeconds: .01, History: []HostHealthSample{{At: clock.Now()}}}
	check(t, q, m, nil)
	q.mu.Lock()
	clock.Advance(12 * time.Second)
	q.mu.Unlock()
	view, _, err := q.captureStatusObservation("", nil)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(3 * time.Second)
	if reason := view.hostHygieneReason(view.s.Machines[m.ID]); reason != "" {
		t.Fatal("status lost or advanced queue delivery evidence", reason)
	}
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
		t.Fatal("live queue failed to expire silent sensor", reason)
	}
}

func TestHostHealthQueueDelayDetachedViews(t *testing.T) {
	for _, advice := range []bool{false, true} {
		t.Run(map[bool]string{false: "immutable-status", true: "submission-advice"}[advice], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("detached-health", "fake")
			m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}, AgeSeconds: .01, History: []HostHealthSample{{At: clock.Now()}}}
			check(t, q, m, nil)
			q.mu.Lock()
			clock.Advance(12 * time.Second)
			q.mu.Unlock()
			capture := func() *Queue {
				var view *Queue
				var err error
				if advice {
					q.mu.Lock()
					snapshot := q.captureSubmissionAdvice()
					q.mu.Unlock()
					view, err = snapshot.planner(context.Background())
				} else {
					view, _, err = q.captureReadObservation("", nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				return view
			}
			view := capture()
			if reason := view.hostHygieneReason(view.s.Machines[m.ID]); reason != "" {
				t.Fatal("detached view lost queue processing credit", reason)
			}
			clock.Advance(3 * time.Second)
			if reason := view.hostHygieneReason(view.s.Machines[m.ID]); reason != "" {
				t.Fatal("detached view kept live freshness state", reason)
			}
			fresh := capture()
			if reason := fresh.hostHygieneReason(fresh.s.Machines[m.ID]); reason != "host_probe_unknown" {
				t.Fatal("new detached view hid stopped sensor expiry", reason)
			}
		})
	}
}

func TestHostHealthQueueDelayFailureAndRollback(t *testing.T) {
	q, clock := fixture(t)
	m := machine("fenced-health", "fake")
	m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}, AgeSeconds: .01, History: []HostHealthSample{{At: clock.Now()}}}
	check(t, q, m, nil)
	q.mu.Lock()
	clock.Advance(12 * time.Second)
	q.mu.Unlock()
	rejected := errors.New("reject transaction")
	if err := q.update(func() error {
		q.touchMachine(m.ID).HostHealth = &HostHealth{}
		return rejected
	}); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "" {
		t.Fatal("rollback lost accepted queue delivery evidence", reason)
	}
	// A failed observation must refuse immediately, however slow the queue was.
	m.HostHealth.History[len(m.HostHealth.History)-1].Error = "sensor failed"
	check(t, q, m, nil)
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
		t.Fatal("queue delay hid sensor failure", reason)
	}
}

// A cancelled/retried HTTP lane may overlap an older check-in still preparing
// outside q.mu. Queue receipt order must prevent that success undoing refusal.
func TestHostHealthQueueDelayOlderSuccessCannotClearFailure(t *testing.T) {
	q, clock := fixture(t)
	m := machine("ordered-health", "fake")
	m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}, AgeSeconds: .01, History: []HostHealthSample{{At: clock.Now()}}}
	check(t, q, m, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	q.checkpointProbe = func() bool {
		block := false
		once.Do(func() { block = true })
		if block {
			close(entered)
			<-release
		}
		return true
	}
	done := make(chan error, 1)
	go func() { _, err := q.Checkin(Checkin{Machine: m}); done <- err }()
	<-entered
	clock.Advance(time.Second)
	newer := m
	h := cloneHostHealth(*m.HostHealth)
	newer.HostHealth = &h
	newer.HostHealth.History[len(h.History)-1].Error = "sensor failed"
	check(t, q, newer, nil)
	clock.Advance(4 * time.Second)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
		t.Fatal("older delayed success cleared newer failure", reason)
	}
	// A genuinely later successful observation can still recover the same session.
	m.HostHealth.History[len(m.HostHealth.History)-1].At = clock.Now()
	check(t, q, m, nil)
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "" {
		t.Fatal("new successful observation failed to recover", reason)
	}
}

func TestHostHealthQueueDelayClockRollbackRefuses(t *testing.T) {
	q, clock := fixture(t)
	m := machine("clock-health", "fake")
	m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}, AgeSeconds: .01, History: []HostHealthSample{{At: clock.Now()}}}
	check(t, q, m, nil)
	q.checkpointProbe = func() bool { clock.Advance(-time.Second); return true }
	check(t, q, m, nil)
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
		t.Fatal("queue clock rollback hid invalid observation age", reason)
	}
}

// The queue may spend its time preparing a check-in outside q.mu. Only that
// session's fresh pending report can preserve its previous health evidence.
func TestHostHealthQueueDelayPendingPreparation(t *testing.T) {
	for _, signal := range []string{"healthy", "stopped", "failed", "late"} {
		t.Run(signal, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("preparing-health", "fake")
			m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}, AgeSeconds: .01, History: []HostHealthSample{{At: clock.Now()}}}
			check(t, q, m, nil)
			peer := m
			peer.ID = "unrelated-health"
			check(t, q, peer, nil)
			if signal == "late" {
				clock.Advance(2200 * time.Millisecond)
			} else {
				clock.Advance(1900 * time.Millisecond)
			}
			incoming := m
			h := cloneHostHealth(*m.HostHealth)
			incoming.HostHealth = &h
			if signal == "stopped" {
				h.AgeSeconds = 2.1
			}
			if signal == "failed" {
				h.History[len(h.History)-1].Error = "sensor failed"
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			q.checkpointProbe = func() bool {
				block := false
				once.Do(func() { block = true })
				if block {
					close(entered)
					<-release
				}
				return true
			}
			done := make(chan error, 1)
			go func() { _, err := q.Checkin(Checkin{Machine: incoming}); done <- err }()
			<-entered
			clock.Advance(300 * time.Millisecond)
			q.mu.Lock()
			reason := q.hostHygieneReason(q.s.Machines[m.ID])
			unrelated := q.hostHygieneReason(q.s.Machines[peer.ID])
			q.mu.Unlock()
			view, _, snapshotErr := q.captureStatusObservation("", nil)
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			expected := "host_probe_unknown"
			if signal == "healthy" {
				expected = ""
			}
			if reason != expected {
				t.Fatalf("%s pending preparation: got %q want %q", signal, reason, expected)
			}
			if unrelated != "host_probe_unknown" {
				t.Fatal("pending preparation extended an unrelated worker", unrelated)
			}
			clock.Advance(3 * time.Second)
			if reason := view.hostHygieneReason(view.s.Machines[m.ID]); reason != expected {
				t.Fatal("snapshot lost frozen pending-preparation evidence", reason)
			}
		})
	}
}

func TestHostHealthQueueDelayPendingLifecycle(t *testing.T) {
	for _, outcome := range []string{"cancelled", "rejected"} {
		t.Run(outcome, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("preparation-lifecycle", "fake")
			m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}, AgeSeconds: .01, History: []HostHealthSample{{At: clock.Now()}}}
			check(t, q, m, nil)
			clock.Advance(1900 * time.Millisecond)
			incoming := m
			if outcome == "rejected" {
				incoming.FreeMemoryGB = -1
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			q.checkpointProbe = func() bool {
				block := false
				once.Do(func() { block = true })
				if block {
					close(entered)
					<-release
				}
				return true
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := q.CheckinContext(ctx, Checkin{Machine: incoming}); done <- err }()
			<-entered
			clock.Advance(300 * time.Millisecond)
			view, _, err := q.captureStatusObservation("", nil)
			if outcome == "cancelled" {
				cancel()
				await(t, func() bool {
					q.mu.Lock()
					defer q.mu.Unlock()
					return q.hostHygieneReason(q.s.Machines[m.ID]) == "host_probe_unknown"
				})
			}
			close(release)
			result := <-done
			if err != nil {
				t.Fatal(err)
			}
			if result == nil {
				t.Fatal("invalid replacement accepted")
			}
			if outcome == "cancelled" && !errors.Is(result, context.Canceled) {
				t.Fatal(result)
			}
			if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
				t.Fatal("finished preparation retained health credit", reason)
			}
			if reason := view.hostHygieneReason(view.s.Machines[m.ID]); reason != "" {
				t.Fatal("snapshot followed live preparation cleanup", reason)
			}
		})
	}
}

// After committing the report, reply hydration is still queue-side processing
// outside q.mu. The sender cannot deliver a replacement until it gets its reply.
func TestHostHealthQueueDelayReplyPreparation(t *testing.T) {
	for _, first := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement", true: "first-report"}[first], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("reply-health", "fake")
			m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .001, ValidSeconds: 2.1}, AgeSeconds: .01, History: []HostHealthSample{{At: clock.Now()}}}
			if !first {
				check(t, q, m, nil)
			}
			clock.Advance(1900 * time.Millisecond)
			q.fileLifecycle.Lock()
			done := make(chan error, 1)
			go func() { _, err := q.Checkin(Checkin{Machine: m}); done <- err }()
			var once sync.Once
			release := func() {
				once.Do(func() {
					q.fileLifecycle.Unlock()
					if err := <-done; err != nil {
						t.Error(err)
					}
				})
			}
			defer release()
			await(t, func() bool {
				q.mu.Lock()
				defer q.mu.Unlock()
				stored := q.s.Machines[m.ID]
				return stored != nil && stored.LastSeen.Equal(clock.Now())
			})
			clock.Advance(3 * time.Second)
			q.mu.Lock()
			reason := q.hostHygieneReason(q.s.Machines[m.ID])
			q.mu.Unlock()
			if reason != "" {
				t.Fatal("queue reply preparation expired accepted sensor", reason)
			}
			release()
			if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "" {
				t.Fatal("completed reply lost measured processing credit", reason)
			}
			clock.Advance(2090 * time.Millisecond)
			if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
				t.Fatal("silent sensor survived its post-reply measured bound", reason)
			}
		})
	}
}

// A restarted worker has not learned a report gap. Its first observation can
// have ~one second of validity even when the queue takes several seconds to
// process its report and offer service for the next one (#1025 live restart).
func TestHostHealthQueueDelayWorkerRestart(t *testing.T) {
	q, clock := fixture(t)
	m := machine("restart-health", "fake")
	dir := t.TempDir()
	monitor, err := openHostHealth(dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	frozen := &ProbeObservation{PeriodSeconds: 1, CostSeconds: .0036, ValidSeconds: 1.021}
	complete := func(monitor *hostHealthMonitor) {
		at := clock.Now()
		clock.Advance(3600 * time.Microsecond)
		if err := monitor.record(context.Background(), at, HostHealthReading{WakeSeconds: .0036}, nil, frozen); err != nil {
			t.Fatal(err)
		}
	}
	complete(monitor)
	m.HostHealth = monitor.report(nil)
	check(t, q, m, nil)
	previous := m.Session
	clock.Advance(10 * time.Second)
	// Reopen the actual saved health history but not the prior process's
	// report-cadence estimator. A fresh sample must establish the new session.
	monitor, err = openHostHealth(dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	complete(monitor)
	m.Session = "restarted-session"
	m.HostHealth = monitor.report(nil) // First send, before a report gap exists.
	q.mu.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: previous})
		done <- err
	}()
	await(t, func() bool {
		q.contactMu.Lock()
		defer q.contactMu.Unlock()
		for pending := range q.healthPreparing[m.ID] {
			if pending.session == m.Session {
				return true
			}
		}
		return false
	})
	clock.Advance(2482400 * time.Microsecond)
	q.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Mirror the live status: 2.511 s after acceptance the physical sample
	// age is 4.997 s, but queue occupancy consumed no replacement opportunity.
	q.mu.Lock()
	clock.Advance(2511 * time.Millisecond)
	stored := q.s.Machines[m.ID]
	age := stored.HostHealth.AgeSeconds + clock.Now().Sub(stored.LastSeen).Seconds()
	if age < 4.996 || age > 4.998 {
		q.mu.Unlock()
		t.Fatal("restart fixture lost observed physical age", age)
	}
	reason := q.hostHygieneReason(stored)
	q.mu.Unlock()
	if reason != "" {
		t.Fatal("queue delay invalidated first restarted report", reason)
	}
	// No second sample is delivered. Available service, rather than the
	// probe period alone in physical time, bounds expiry without any constant.
	remaining := frozen.freshness() - time.Duration(m.HostHealth.AgeSeconds*float64(time.Second))
	clock.Advance(remaining - time.Nanosecond)
	if reason := q.hostHygieneReason(stored); reason != "" {
		t.Fatal("cold evidence expired before measured service bound", reason)
	}
	clock.Advance(time.Nanosecond)
	if reason := q.hostHygieneReason(stored); reason != "host_probe_unknown" {
		t.Fatal("stopped restarted sensor survived measured service bound", reason)
	}
	// A subsequent report of the unchanged sensor cannot renew that evidence.
	m.HostHealth = monitor.report(nil)
	check(t, q, m, nil)
	if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
		t.Fatal("restart contact renewed a stopped sensor", reason)
	}
	t.Logf("cold validity=%s, physical age=%fs, remaining service bound=%s", frozen.freshness(), age, remaining)
}

// Queue-owned response work still blocks the worker's serialized check-in.
// Exercise both real NAS response preparation and the actual JSON writer.
type healthReplyWriter struct {
	*httptest.ResponseRecorder
	entered, release chan struct{}
	once             sync.Once
}

func (w *healthReplyWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.ResponseRecorder.Write(b)
}

func TestHostHealthQueueDelayHTTPResponse(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "cancelled"}[cancelled], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("http-cold-health", "fake")
			m.HostHealth = &HostHealth{Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .0036, ValidSeconds: 1.021}, AgeSeconds: .0036, History: []HostHealthSample{{At: clock.Now()}}}
			handler := API{Queue: q, Token: "test"}.Handler()
			nas := q.resolveNASInspections()
			nas.mu.Lock()
			body, err := json.Marshal(Checkin{AttemptProtocol: keyedAttemptProtocol, Machine: m})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request := httptest.NewRequest("POST", "/api/workers/checkin", bytes.NewReader(body)).WithContext(ctx)
			request.Header.Set("Authorization", "Bearer test")
			request.Header.Set("X-Myce-Start-Gate", startGateProtocol)
			writer := &healthReplyWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan struct{})
			go func() { handler.ServeHTTP(writer, request); close(done) }()
			var releaseNAS, releaseWriter sync.Once
			defer func() {
				releaseNAS.Do(nas.mu.Unlock)
				releaseWriter.Do(func() { close(writer.release) })
				<-done
			}()
			await(t, func() bool {
				q.mu.Lock()
				defer q.mu.Unlock()
				return q.s.Machines[m.ID] != nil
			})
			clock.Advance(3 * time.Second)
			if cancelled {
				cancel()
			}
			expected := ""
			if cancelled {
				expected = "host_probe_unknown"
			}
			q.mu.Lock()
			reason := q.hostHygieneReason(q.s.Machines[m.ID])
			q.mu.Unlock()
			if reason != expected {
				t.Fatal("NAS response preparation freshness", reason)
			}
			releaseNAS.Do(nas.mu.Unlock)
			<-writer.entered
			clock.Advance(3 * time.Second)
			q.mu.Lock()
			reason = q.hostHygieneReason(q.s.Machines[m.ID])
			q.mu.Unlock()
			if reason != expected {
				t.Fatal("response encoding freshness", reason)
			}
			releaseWriter.Do(func() { close(writer.release) })
			<-done
			if writer.Code != 200 {
				t.Fatal("check-in response", writer.Code)
			}
			if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != expected {
				t.Fatal("completed HTTP response freshness", reason)
			}
			if !cancelled {
				remaining := m.HostHealth.Probe.freshness() - time.Duration(m.HostHealth.AgeSeconds*float64(time.Second))
				clock.Advance(remaining)
				if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
					t.Fatal("silent sensor survived HTTP completion service bound", reason)
				}
			}
		})
	}
}

// A legacy Linux PSI latch can be cleared by queue classification. That healthy
// report needs the same processing credit as an already-clear worker report.
func TestHostHealthQueueDelayLegacyPSIRecovery(t *testing.T) {
	for _, corroborated := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovered", true: "corroborated-drain"}[corroborated], func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("legacy-psi-health", "fake", "cpu", 4, 16)
			h := &HostHealth{DrainReason: "host_memory_pressure", Probe: &ProbeObservation{PeriodSeconds: 1, CostSeconds: .0036, ValidSeconds: 1.021}, AgeSeconds: .0036}
			for i := range hostHealthWarmup {
				h.Baseline = append(h.Baseline, HostHealthSample{At: clock.Now().Add(time.Duration(i-hostHealthWarmup-hostHealthConfirmation) * time.Second), HostHealthReading: HostHealthReading{Pressure: .02, WakeSeconds: .001}})
			}
			for i := range hostHealthConfirmation {
				s := HostHealthSample{At: clock.Now().Add(time.Duration(i-hostHealthConfirmation+1) * time.Second), HostHealthReading: HostHealthReading{Pressure: 2, WakeSeconds: .001}}
				if corroborated {
					s.SwapGrowth = 1 << 20
				}
				h.History = append(h.History, s)
			}
			m.HostHealth = h
			q.fileLifecycle.Lock()
			done := make(chan error, 1)
			go func() { _, err := q.Checkin(Checkin{Machine: m}); done <- err }()
			var once sync.Once
			release := func() {
				once.Do(func() {
					q.fileLifecycle.Unlock()
					if err := <-done; err != nil {
						t.Error(err)
					}
				})
			}
			defer release()
			await(t, func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.s.Machines[m.ID] != nil })
			clock.Advance(3 * time.Second)
			expected := ""
			if corroborated {
				expected = "host_memory_pressure"
			}
			q.mu.Lock()
			reason := q.hostHygieneReason(q.s.Machines[m.ID])
			q.mu.Unlock()
			if reason != expected {
				t.Fatalf("classified PSI during reply preparation: got %q want %q", reason, expected)
			}
			if h.DrainReason != "host_memory_pressure" || h.PressureEvaluation != nil {
				t.Fatal("preparation mutated original worker report")
			}
			release()
			if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != expected {
				t.Fatal("completed PSI reply lost classification or processing credit", reason)
			}
			if !corroborated {
				clock.Advance(h.Probe.freshness() - time.Duration(h.AgeSeconds*float64(time.Second)))
				if reason := q.hostHygieneReason(q.s.Machines[m.ID]); reason != "host_probe_unknown" {
					t.Fatal("recovered PSI sensor survived measured service bound", reason)
				}
			}
		})
	}
}
