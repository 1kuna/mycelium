package myce

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// A machine record shaped like a live status record: rolling health history,
// extension and environment reports, devices and disk telemetry. All synthetic.
func realisticMachine(id string) Machine {
	m := cpuMachine(id, id, "cpu", 32, 96)
	m.ExtensionFingerprint, m.CPUExtensionFingerprint = "synthetic-cpu", "synthetic-cpu"
	m.Devices = []MachineDevice{}
	for d := range 2 {
		device := MachineDevice{ID: fmt.Sprint(d), Platform: "cuda", CapabilityProfile: &CapabilityProfile{OS: "linux", Arch: "amd64", MemoryLayout: "separate", Accelerators: []AcceleratorProfile{{Type: "cuda", MemoryBytes: 48 << 30}}}, MemoryMode: "fixed", Selector: fmt.Sprint(d), TotalMemoryGB: 48, BudgetGB: 46, FreeMemoryGB: 40, Identity: fmt.Sprintf("synthetic-card-%d", d), ExtensionFingerprint: "synthetic-cuda"}
		for e := range 20 {
			device.ExtensionReports = append(device.ExtensionReports, ExtensionReport{Device: device.ID, Name: fmt.Sprintf("extension-%02d", e), Version: "1.4.2", Digest: fmt.Sprintf("%064x", e), Platform: "cuda", Work: "inference", Fingerprint: "synthetic-cuda", Setup: true, Qualified: true})
		}
		m.Devices = append(m.Devices, device)
	}
	for e := range 60 {
		m.ExtensionReports = append(m.ExtensionReports, ExtensionReport{Name: fmt.Sprintf("extension-%02d", e), Version: "2.0.1", Digest: fmt.Sprintf("%064x", e+100), Platform: "cpu", Work: "batch", Fingerprint: "synthetic-cpu", Setup: true, Qualified: e%7 != 0, CheckID: fmt.Sprintf("check-%02d", e), CheckDigest: fmt.Sprintf("%064x", e+200), CheckState: "passed"})
	}
	for e := range 25 {
		key := fmt.Sprintf("environment-%02d", e)
		m.Environments = append(m.Environments, key)
		r := EnvironmentReport{Key: key, State: "ready", BuildSeconds: 41.5}
		if e < 2 { // Workers keep a log excerpt only for failed builds.
			r.State, r.Error, r.Log = "failed", "dependency resolution failed", strings.Repeat("unresolved synthetic dependency\n", 40)
		}
		m.EnvironmentReports = append(m.EnvironmentReports, r)
	}
	health := &HostHealth{AgeSeconds: 0.4}
	for i := range hostHealthHistorySize {
		health.History = append(health.History, recordHealthSample(i))
		health.Baseline = append(health.Baseline, recordHealthSample(i))
	}
	for i := range 40 {
		health.Foreign = append(health.Foreign, ForeignConsumer{PID: 1000 + i, Executable: fmt.Sprintf("service-%02d", i), CPUPercent: 1.5, RSSBytes: 300 << 20, User: "owner", AgeSeconds: 86400})
	}
	m.HostHealth = health
	for i, root := range []string{"cache", "checkpoint", "job-files", "scratch"} {
		change := 1024.0 * float64(i)
		m.DiskVolumes = append(m.DiskVolumes, DiskVolume{ID: root, Roots: []string{root}, TotalBytes: 2 << 40, FreeBytes: 1 << 40, ChangeBytesPerHour: &change, SampledAt: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC), SampleIntervalSeconds: 30})
	}
	m.CapabilityProfile.Availability = &Availability{Mode: "dedicated", Available: true, Signals: OwnerSignals{IdleKnown: true, IdleSeconds: 3600, Power: "ac", Heat: "nominal"}}
	m.IOPressure = &IOPressure{Some: &PressureStall{Avg10: 0.1}, SampledAt: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)}
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 128, UsedGB: 30, MyceliumGB: 12}
	m.CachedModels = []string{"model-a", "model-b", "model-c"}
	m.CacheFreeBytes = 500 << 30
	return m
}

func recordHealthSample(i int) HostHealthSample {
	return HostHealthSample{HostHealthReading: HostHealthReading{Load: 3 + float64(i%5)/10, Pressure: 0.2, SwapBytes: 1 << 20, WakeSeconds: 0.001}, At: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second), HeartbeatSeconds: 1, DiskFree: map[string]float64{"cache": 0.5, "checkpoint": 0.6, "scratch": 0.7}, Mounts: []HostMountHealth{{Root: "nas", LatencySeconds: 0.004}, {Root: "checkpoint", LatencySeconds: 0.003}, {Root: "job-files", LatencySeconds: 0.002}}}
}

// Current environment receipts identify a queue-issued preparation. Unscoped
// invented receipts fall back to legacy target inference, which is not a
// steady-worker report and may legitimately change the durable record.
func offerMachineRecordEnvironments(t testing.TB, q *Queue, m *Machine) {
	t.Helper()
	if err := q.update(func() error {
		if q.s.PreparationPlans == nil {
			q.s.PreparationPlans = map[string]HostLaunch{}
		}
		for i := range m.EnvironmentReports {
			r := &m.EnvironmentReports[i]
			r.Attempt = m.ID + "-" + r.Key
			q.s.PreparationPlans[r.Attempt] = HostLaunch{Machine: m.ID, Session: m.Session,
				Kind: "environment", Key: r.Key, Attempt: r.Attempt}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// heartbeat changes only what a steady worker refreshes on each check-in.
func heartbeat(m *Machine, i int) {
	m.HostHealth.History = append(m.HostHealth.History[1:len(m.HostHealth.History):len(m.HostHealth.History)], recordHealthSample(hostHealthHistorySize+i))
	m.HostHealth.AgeSeconds = 0.1 + float64(i%9)/10
	m.HostAvailableMemoryGB = 90 + float64(i%7)
	m.MemoryUsedGB = float64(i % 5)
	m.MemoryObservation.UsedGB = 30 + float64(i%11)/10
	m.CacheFreeBytes = int64(500<<30) - int64(i%13)<<20
	m.CapabilityProfile.Availability.Signals.IdleSeconds = 3600 + float64(i)
	m.IOPressure.SampledAt = m.IOPressure.SampledAt.Add(time.Second)
	for v := range m.DiskVolumes {
		m.DiskVolumes[v].FreeBytes -= uint64(i%3) << 20
		m.DiskVolumes[v].SampledAt = m.DiskVolumes[v].SampledAt.Add(time.Second)
	}
	for d := range m.Devices {
		free := 40 - float64(i%4)
		m.Devices[d].FreeMemoryGB = free
		m.Devices[d].Telemetry = &DeviceTelemetry{At: time.Date(2026, 11, 3, 12, 0, i, 0, time.UTC), AgeSeconds: 0.2, FreeGB: &free, Source: "synthetic"}
	}
}

// machineRecordWrites counts machine-record bytes one transaction rewrote.
func machineRecordWrites(before map[string][]byte, after map[string][]byte) (bytes, records int) {
	for key, data := range after {
		if !strings.HasPrefix(key, "machines/") {
			continue
		}
		old := before[key]
		if len(old) == len(data) && (len(data) == 0 || &old[0] == &data[0]) {
			continue
		}
		bytes += len(data)
		records++
	}
	return bytes, records
}

// Steady heartbeats from five realistic workers, with real SQLite FULL commits.
func BenchmarkCheckinMachineRecord(b *testing.B) {
	clock := &fakeClock{now: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)}
	q, err := Open(b.TempDir()+"/queue.db", b.TempDir(), clock)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { q.Close() })
	q.checkpointProbe = func() bool { return true }
	workers := make([]Machine, 5)
	for i := range workers {
		workers[i] = realisticMachine(fmt.Sprintf("worker-%d", i))
		offerMachineRecordEnvironments(b, q, &workers[i])
		if _, err := q.Checkin(Checkin{Machine: workers[i], period: time.Second}); err != nil {
			b.Fatal(err)
		}
	}
	admissionWaitBenchmarkSettle(q)
	q.mu.Lock()
	full, _ := json.Marshal(q.s.Machines["worker-0"])
	stored := len(q.persisted["machines/worker-0"])
	q.mu.Unlock()
	b.Logf("machine record: %d bytes in memory, %d bytes stored", len(full), stored)
	if _, err := q.db.Exec("PRAGMA wal_autocheckpoint=0; PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		b.Fatal(err)
	}
	var dbPath string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&dbPath); err != nil {
		b.Fatal(err)
	}
	var sum queuePersistenceTiming
	var phases [5]time.Duration
	q.observePersistence = func(p queuePersistenceTiming) {
		sum.Marshal += p.Marshal
		sum.Commit += p.Commit
		sum.Bytes += p.Bytes
		sum.Records += p.Records
	}
	q.observePersistenceHold = func(p queueHoldPhases) {
		for i, v := range p.phases {
			phases[i] += v
		}
	}
	machineBytes, machineRecords := 0, 0
	cpu := benchmarkCPU()
	b.ResetTimer()
	for i := range b.N {
		clock.Advance(time.Second / 5)
		w := &workers[i%len(workers)]
		heartbeat(w, i)
		q.mu.Lock()
		before := q.persisted
		snapshot := make(map[string][]byte, len(before))
		for key, data := range before {
			if strings.HasPrefix(key, "machines/") {
				snapshot[key] = data
			}
		}
		q.mu.Unlock()
		if _, err := q.Checkin(Checkin{Machine: *w, period: time.Second}); err != nil {
			b.Fatal(err)
		}
		q.mu.Lock()
		n, records := machineRecordWrites(snapshot, q.persisted)
		q.mu.Unlock()
		machineBytes += n
		machineRecords += records
	}
	b.StopTimer()
	admissionWaitBenchmarkSettle(q)
	used := benchmarkCPU() - cpu
	info, err := os.Stat(dbPath + "-wal")
	if err != nil {
		b.Fatal(err)
	}
	n := float64(b.N)
	var hold time.Duration
	for _, v := range phases {
		hold += v
	}
	b.ReportMetric(float64(used)/1e6/n, "CPU-ms/checkin")
	b.ReportMetric(holdMS(hold)/n, "hold-ms/checkin")
	b.ReportMetric(holdMS(phases[holdBody])/n, "body-ms/checkin")
	b.ReportMetric(holdMS(phases[holdPersist])/n, "persist-ms/checkin")
	b.ReportMetric(holdMS(sum.Marshal)/n, "marshal-ms/checkin")
	b.ReportMetric(float64(sum.Bytes)/n, "payload-B/checkin")
	b.ReportMetric(float64(machineBytes)/n, "machine-B/checkin")
	b.ReportMetric(float64(machineRecords)/n, "machine-records/checkin")
	b.ReportMetric(float64(info.Size()-32)/n, "WAL-B/checkin")
}

func storedMachine(t *testing.T, q *Queue, id string) ([]byte, Machine) {
	t.Helper()
	q.mu.RLock()
	data := q.persisted["machines/"+id]
	q.mu.RUnlock()
	var m Machine
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return data, m
}

// A steady heartbeat changes only the reported half, so it rewrites no machine
// record, even when the worker lists its reports in another order.
func TestSteadyHeartbeatRewritesNoMachineRecord(t *testing.T) {
	q, clock := fixture(t)
	w := realisticMachine("worker")
	offerMachineRecordEnvironments(t, q, &w)
	machineCheck(t, q, w)
	before, _ := storedMachine(t, q, "worker")
	for i := range 10 {
		clock.Advance(time.Second)
		heartbeat(&w, i)
		slices.Reverse(w.EnvironmentReports)
		machineCheck(t, q, w)
		if after, _ := storedMachine(t, q, "worker"); &after[0] != &before[0] {
			t.Fatalf("heartbeat %d rewrote the machine record", i)
		}
	}
	w.MaxJobs = 3
	machineCheck(t, q, w)
	if after, got := storedMachine(t, q, "worker"); &after[0] == &before[0] || got.MaxJobs != 3 {
		t.Fatal("setting change was not stored")
	}
}

// Restart keeps identity and the session fence. The reported half is absent
// until the worker's next check-in; until then nothing is placed on it, its
// running work is not stopped and no host alert opens. One check-in re-learns it.
func TestMachineRecordRestartRelearnsOnFirstCheckin(t *testing.T) {
	q, clock := fixture(t)
	w := realisticMachine("worker")
	offerMachineRecordEnvironments(t, q, &w)
	running := submit(t, q, cpuSpec(2, 2))
	a := assignedJob(machineCheck(t, q, w))
	if a == nil || a.ID != running.ID {
		t.Fatal("no assignment", a)
	}
	heartbeat(&w, 1)
	machineCheck(t, q, w, a)
	requireState(t, q, a.ID, "running")
	q.mu.RLock()
	seen := q.s.Machines["worker"].LastSeen
	acceptedReports := slices.Clone(q.s.Machines["worker"].EnvironmentReports)
	q.mu.RUnlock()
	_, stored := storedMachine(t, q, "worker")
	if stored.HostHealth != nil || stored.DiskVolumes != nil || !stored.LastSeen.IsZero() || stored.CapabilityProfile.Availability != nil || stored.Devices[0].Telemetry != nil {
		t.Fatal("stored reported fields")
	}
	if stored.Session != w.Session || len(stored.Devices) != 2 || len(stored.ExtensionReports) != 60 || len(stored.EnvironmentReports) != 25 {
		t.Fatal("durable half not stored")
	}
	for _, want := range acceptedReports {
		if !slices.ContainsFunc(stored.EnvironmentReports, func(got EnvironmentReport) bool { return reflect.DeepEqual(got, want) }) {
			t.Fatalf("accepted environment report %q not stored", want.Key)
		}
	}

	clock.Advance(5 * time.Second)
	q = reopenQueue(t, q, clock)
	queued := submit(t, q, cpuSpec(2, 2))
	q.mu.RLock()
	m := q.s.Machines["worker"]
	closed := m.Session == w.Session && m.LastSeen.Equal(seen) && m.HostHealth == nil && m.DiskVolumes == nil && q.unreported["worker"] && !q.hostLaunchAllowed(m, m.Session)
	q.mu.RUnlock()
	if !closed {
		t.Fatal("restart did not fail closed")
	}
	intruder := w
	intruder.Session = "intruder"
	if _, err := q.Checkin(Checkin{Machine: intruder, period: time.Second, AttemptProtocol: keyedAttemptProtocol}); err == nil || !strings.Contains(err.Error(), "already joined") {
		t.Fatal("another session took the fenced name", err)
	}
	// A log line is contact, not a report: placement stays closed.
	if err := q.AppendLog(LogChunk{Result: Result{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Machine: a.Machine, Session: a.Session}, Text: "still running\n"}); err != nil {
		t.Fatal(err)
	}
	q.mu.RLock()
	events := len(q.s.Health.Events)
	q.mu.RUnlock()
	for range 3 {
		clock.Advance(20 * time.Second)
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	q.mu.RLock()
	still, quiet := q.unreported["worker"], len(q.s.Health.Events) == events
	q.mu.RUnlock()
	if !still || !quiet {
		t.Fatalf("before its check-in: unreported=%t, no new alerts=%t", still, quiet)
	}
	requireState(t, q, a.ID, "running")
	requireState(t, q, queued.ID, "queued")

	heartbeat(&w, 2)
	r := machineCheck(t, q, w, a)
	q.mu.RLock()
	m = q.s.Machines["worker"]
	relearned := !q.unreported["worker"] && m.HostHealth != nil && len(m.DiskVolumes) == 4 && m.CapabilityProfile.Availability != nil && m.LastSeen.After(seen) && q.hostLaunchAllowed(m, m.Session)
	q.mu.RUnlock()
	if !relearned {
		t.Fatal("first check-in did not re-learn the worker")
	}
	if b := assignedJob(r); b == nil || b.ID != queued.ID {
		t.Fatal("placement did not resume after the check-in", b)
	}
}
