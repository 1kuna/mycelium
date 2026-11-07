package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"
)

// These bounds limit diagnostic state and cold-start statistical uncertainty,
// not host capacity. Three consecutive outliers exclude a single brief spike.
const hostHealthHistorySize = 64
const hostHealthWarmup = 8
const hostHealthConfirmation = 3

type HostHealthReading struct {
	Load        float64 `json:"load"`
	Pressure    float64 `json:"pressure"`
	SwapBytes   float64 `json:"swap_bytes"`
	WakeSeconds float64 `json:"wake_seconds"`
}

type HostHealthSample struct {
	HostHealthReading
	At               time.Time          `json:"at"`
	HeartbeatSeconds float64            `json:"heartbeat_seconds"`
	SwapGrowth       float64            `json:"swap_growth_bytes_per_second"`
	DiskFree         map[string]float64 `json:"disk_free,omitempty"`
	Mounts           []HostMountHealth  `json:"mounts,omitempty"`
	Error            string             `json:"error,omitempty"`
}

type HostMountHealth struct {
	Adverse               bool    `json:"adverse,omitempty"`
	Root                  string  `json:"root"`
	LatencySeconds        float64 `json:"latency_seconds"`
	PendingSeconds        float64 `json:"pending_seconds"`
	OperationAgeSeconds   float64 `json:"operation_age_seconds,omitempty"`
	SlowThresholdSeconds  float64 `json:"slow_threshold_seconds,omitempty"`
	StallAllowanceSeconds float64 `json:"stall_allowance_seconds,omitempty"`
	Reason                string  `json:"reason,omitempty"`
}

// Executable and Service are basenames/identities only; never argv or env.
type ForeignConsumer struct {
	PID        int     `json:"pid"`
	Executable string  `json:"executable"`
	Service    string  `json:"service,omitempty"`
	CPUPercent float64 `json:"cpu_percent"`
	RSSBytes   uint64  `json:"rss_bytes"`
}

type HostHealth struct {
	DiskRecovery     map[string]float64 `json:"disk_recovery,omitempty"`
	PersistenceError string             `json:"persistence_error,omitempty"`
	History          []HostHealthSample `json:"history"`
	Baseline         []HostHealthSample `json:"baseline"`
	DrainReason      string             `json:"drain_reason,omitempty"`
	Candidate        string             `json:"candidate,omitempty"`
	Consecutive      int                `json:"consecutive,omitempty"`
	Foreign          []ForeignConsumer  `json:"foreign,omitempty"`
	ForeignError     string             `json:"foreign_error,omitempty"`
	ForeignAt        time.Time          `json:"foreign_at,omitempty"`
	AgeSeconds       float64            `json:"age_seconds"`
}

// A robust prediction band is learned separately on every host. The widest
// observed adjacent change supplies resolution when a baseline is constant.
// Three interquartile ranges is Tukey's far-outlier fence, not a fleet limit.
func healthBand(samples []HostHealthSample, value func(HostHealthSample) float64) (float64, float64) {
	v := make([]float64, len(samples))
	var resolution float64
	for i, s := range samples {
		v[i] = value(s)
		if i > 0 {
			resolution = max(resolution, absHealth(v[i]-v[i-1]))
		}
	}
	sort.Float64s(v)
	q1, q3 := v[len(v)/4], v[len(v)*3/4]
	spread := max(q3-q1, resolution)
	return q1 - 3*spread, q3 + 3*spread
}
func absHealth(v float64) float64 { return max(v, -v) }

func (h *HostHealth) observe(s HostHealthSample) {
	if len(h.History) > 0 {
		previous := h.History[len(h.History)-1]
		if !s.At.After(previous.At) {
			return
		}
		if s.Error == "" && previous.Error == "" && s.SwapBytes >= previous.SwapBytes {
			s.SwapGrowth = (s.SwapBytes - previous.SwapBytes) / s.At.Sub(previous.At).Seconds()
		}
	}
	reason := ""
	if s.Error == "" && len(h.Baseline) >= hostHealthWarmup {
		high := func(f func(HostHealthSample) float64) bool { _, hi := healthBand(h.Baseline, f); return f(s) > hi }
		pressure := high(func(s HostHealthSample) float64 { return s.Pressure })
		swap := high(func(s HostHealthSample) float64 { return s.SwapGrowth })
		latency := high(func(s HostHealthSample) float64 { return s.WakeSeconds })
		heartbeat := s.HeartbeatSeconds > 0 && high(func(s HostHealthSample) float64 { return s.HeartbeatSeconds })
		switch {
		case pressure:
			reason = "host_memory_pressure"
		case swap && latency:
			reason = "host_swap_growth"
		case latency && high(func(s HostHealthSample) float64 { return s.Load }):
			reason = "host_load_latency"
		case heartbeat && latency:
			reason = "host_heartbeat_latency"
		}
		for key, free := range s.DiskFree {
			base := slices.DeleteFunc(slices.Clone(h.Baseline), func(s HostHealthSample) bool { _, ok := s.DiskFree[key]; return !ok })
			if len(base) >= hostHealthWarmup {
				lo, _ := healthBand(base, func(s HostHealthSample) float64 { return s.DiskFree[key] })
				// Reserve the largest observed per-sample consumption over
				// the confirmation horizon. A harmless capacity step on a
				// large disk is not pressure merely for leaving a flat band.
				var burst float64
				previous, known := 0.0, false
				for _, sample := range append(slices.Clone(h.History), s) {
					if available, ok := sample.DiskFree[key]; ok {
						if known {
							burst = max(burst, previous-available)
						}
						previous, known = available, true
					}
				}
				if free == 0 || free < lo && free <= burst*hostHealthConfirmation {
					reason = "host_disk_pressure"
					if h.DiskRecovery == nil {
						h.DiskRecovery = map[string]float64{}
					}
					h.DiskRecovery[key] = max(h.DiskRecovery[key], min(max(0, lo), burst*hostHealthConfirmation))
				}
			}
		}
		// History eviction is not evidence of newly available space. Retain
		// the measured burst reserve until this volume positively recovers.
		for key, reserve := range h.DiskRecovery {
			if free, ok := s.DiskFree[key]; !ok || free == 0 || free < reserve {
				reason = "host_disk_pressure"
			} else {
				delete(h.DiskRecovery, key)
			}
		}
	}
	// Missing sensor evidence cannot establish recovery or poison a baseline.
	if s.Error == "" {
		candidate := reason
		if reason == "" && h.DrainReason != "" {
			candidate = "recovered"
		}
		if candidate != h.Candidate {
			h.Candidate, h.Consecutive = candidate, 0
		}
		if candidate != "" {
			h.Consecutive++
		}
		if h.Consecutive >= hostHealthConfirmation {
			h.DrainReason = reason
		}
		// Freeze on the first adverse sample, so sustained trouble does not
		// teach the worker that degradation is its new healthy baseline.
		if reason == "" && h.DrainReason == "" && !slices.ContainsFunc(s.Mounts, func(m HostMountHealth) bool { return m.Adverse || m.Reason != "" }) {
			h.Baseline = append(h.Baseline, s)
			if len(h.Baseline) > hostHealthHistorySize {
				h.Baseline = h.Baseline[1:]
			}
		}
	} else {
		h.Candidate, h.Consecutive = "", 0
	}
	h.History = append(h.History, s)
	if len(h.History) > hostHealthHistorySize {
		h.History = h.History[1:]
	}
}

type hostHealthMonitor struct {
	mu              sync.Mutex
	clock           Clock
	dir             string
	health          HostHealth
	heartbeat       float64
	source          func(context.Context) (HostHealthReading, error)
	foreign         func(context.Context) ([]ForeignConsumer, error)
	mounts          map[string]*hostMountProbe
	disks           map[string]float64
	done            chan struct{}
	persist         func(HostHealth) error
	restoredFaults  map[string]StorageFault
	restoredSources map[string]string
}

// Version the on-disk probe evidence only. The wire shape is unchanged, and
// other host evidence and historical diagnostics survive an upgraded probe.
const hostMountProbeVersion = "checkpoint-access-v2"

type hostHealthDisk struct {
	HostHealth
	MountProbeVersion string                  `json:"mount_probe_version,omitempty"`
	StorageFaults     map[string]StorageFault `json:"storage_faults,omitempty"`
	// Private recovery custody, deliberately outside the path-free fault/wire.
	RecoverySources map[string]string `json:"recovery_sources,omitempty"`
}

func openHostHealth(dir string, clock Clock) (*hostHealthMonitor, error) {
	m := &hostHealthMonitor{dir: dir, clock: clock, mounts: map[string]*hostMountProbe{}}
	m.persist = func(h HostHealth) error {
		faults, sources := m.storageFaultDisk()
		return saveWorkerJSON(dir, "host-health.json", hostHealthDisk{HostHealth: h, MountProbeVersion: hostMountProbeVersion, StorageFaults: faults, RecoverySources: sources})
	}
	b, err := os.ReadFile(filepath.Join(dir, "host-health.json"))
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	var saved hostHealthDisk
	if err := decodeWorkerJSON(b, &saved); err != nil {
		return nil, err
	}
	m.health = saved.HostHealth
	m.restoredFaults, m.restoredSources = saved.StorageFaults, saved.RecoverySources
	if saved.MountProbeVersion != hostMountProbeVersion {
		for i := range m.health.Baseline {
			if saved.MountProbeVersion == "stat-readdir-v1" {
				m.health.Baseline[i].Mounts = slices.DeleteFunc(slices.Clone(m.health.Baseline[i].Mounts), func(m HostMountHealth) bool { return m.Root == "checkpoint" })
			} else {
				m.health.Baseline[i].Mounts = nil
			}
		}
	}
	if len(m.health.History) > hostHealthHistorySize {
		m.health.History = m.health.History[len(m.health.History)-hostHealthHistorySize:]
	}
	if len(m.health.Baseline) > hostHealthHistorySize {
		m.health.Baseline = m.health.Baseline[len(m.health.Baseline)-hostHealthHistorySize:]
	}
	// A restart is a break in the consecutive evidence, not recovery.
	m.health.Candidate, m.health.Consecutive = "", 0
	return m, nil
}

func (m *hostHealthMonitor) sample(ctx context.Context) error {
	at := m.clock.Now()
	r, err := m.source(ctx)
	s := HostHealthSample{HostHealthReading: r, At: at}
	if err != nil {
		s.Error = "host_sensor_unavailable"
	}
	var foreign []ForeignConsumer
	var foreignErr error
	m.mu.Lock()
	foreignDue := m.health.ForeignAt.IsZero() || at.Sub(m.health.ForeignAt) >= time.Minute
	m.mu.Unlock()
	if foreignDue && m.foreign != nil {
		foreign, foreignErr = m.foreign(ctx)
	}
	m.mu.Lock()
	s.HeartbeatSeconds, s.DiskFree = m.heartbeat, m.disks
	for _, probe := range m.mounts {
		s.Mounts = append(s.Mounts, probe.sample(at, m.health.Baseline))
	}
	slices.SortFunc(s.Mounts, func(a, b HostMountHealth) int {
		if a.Root < b.Root {
			return -1
		}
		if a.Root > b.Root {
			return 1
		}
		return 0
	})
	m.health.observe(s)
	if foreignDue && m.foreign != nil {
		m.health.Foreign, m.health.ForeignAt, m.health.ForeignError = foreign, at, ""
		if foreignErr != nil {
			m.health.ForeignError = "foreign_inventory_unavailable"
		}
	}
	m.health.PersistenceError = ""
	durable := cloneHostHealth(m.health)
	m.mu.Unlock()
	// A stuck local fsync must not own the heartbeat/status snapshot mutex.
	err = m.persist(durable)
	m.mu.Lock()
	if err != nil {
		m.health.PersistenceError = "host_history_write_failed"
	}
	m.mu.Unlock()
	return err
}

func (m *hostHealthMonitor) snapshot(disks []DiskVolume) *HostHealth {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disks = map[string]float64{}
	for _, v := range disks {
		if v.Error == "" && !v.Retained && (len(v.Roots) > 1 || !slices.Contains(v.Roots, "checkpoint")) {
			m.disks[v.ID] = float64(v.FreeBytes)
		}
	}
	h := m.health
	if len(h.History) > 0 {
		h.AgeSeconds = max(0, m.clock.Now().Sub(h.History[len(h.History)-1].At).Seconds())
	}
	// observe may append into these backing arrays on its next tick.
	h = cloneHostHealth(h)
	// A fault can arrive after a persisted sample or before the first sample
	// following restart. Snapshot the active latch, never the old success.
	for _, p := range m.mounts {
		if p.fault == nil || !p.fault.Active {
			continue
		}
		if len(h.History) == 0 {
			h.History = []HostHealthSample{{At: m.clock.Now()}}
		}
		last := &h.History[len(h.History)-1]
		last.Mounts = slices.Clone(last.Mounts)
		found := false
		for i := range last.Mounts {
			if last.Mounts[i].Root == p.root {
				last.Mounts[i].Reason, last.Mounts[i].Adverse = p.reason, true
				found = true
			}
		}
		if !found {
			last.Mounts = append(last.Mounts, HostMountHealth{Root: p.root, Reason: p.reason, Adverse: true})
		}
	}
	return &h
}

func cloneHostHealth(h HostHealth) HostHealth {
	h.History, h.Baseline, h.Foreign = slices.Clone(h.History), slices.Clone(h.Baseline), slices.Clone(h.Foreign)
	if h.DiskRecovery != nil {
		copy := map[string]float64{}
		for key, value := range h.DiskRecovery {
			copy[key] = value
		}
		h.DiskRecovery = copy
	}
	return h
}

func startWorkerHealth(ctx context.Context, w Worker, root *workerStore) (*hostHealthMonitor, error) {
	dir := root.dir
	w.CheckpointRoot = root.state.CheckpointRoot
	source := w.HostHealthSource
	if source == nil {
		source = workerHostHealthSource
	}
	if source == nil {
		return nil, nil
	}
	m, err := openHostHealth(dir, w.Clock)
	if err != nil {
		return nil, err
	}
	m.source = source
	if workerForeignConsumers != nil {
		m.foreign = func(ctx context.Context) ([]ForeignConsumer, error) { return workerForeignConsumers(ctx, dir) }
	}
	m.done = make(chan struct{})
	for role, path := range workerHealthMounts(w) {
		m.mounts[role] = &hostMountProbe{root: role, path: path, clock: m.clock, probe: recoveryMountProbe(w, role, path)}
	}
	for role, fault := range m.restoredFaults {
		p := m.mounts[role]
		if p == nil && role == "job-files" && w.JobFileRoot != "" {
			p = &hostMountProbe{root: role, path: w.JobFileRoot, clock: m.clock, probe: recoveryMountProbe(w, "nas", w.JobFileRoot)}
			m.mounts[role] = p
		}
		if p != nil {
			source := m.restoredSources[role]
			if source != "" && !withinNAS(p.path, source) && (w.ServiceConfig == nil || w.ServiceConfig.NAS == "" || !withinNAS(w.ServiceConfig.NAS, source)) {
				// Preserve the hold across a restart, but never read an old
				// source outside the newly configured storage scope.
				source = ""
			}
			restoreStorageFault(p, fault, source)
		}
	}
	go func() {
		defer close(m.done)
		// Reuse the existing observation cadence. No sampling sleeps occur in
		// scheduling; the injected clock decides which sample is due.
		tick := time.NewTicker(signalRefresh)
		defer tick.Stop()
		for {
			probeCtx, cancel := context.WithTimeout(ctx, signalRefresh)
			_ = m.sample(probeCtx)
			cancel()
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	return m, nil
}

func (m *hostHealthMonitor) wait() {
	if m != nil && m.done != nil {
		<-m.done
	}
}

func (m *hostHealthMonitor) noteHeartbeat(d time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.heartbeat = d.Seconds()
	m.mu.Unlock()
}

func hostHygieneReason(m *Machine) string {
	if m != nil && m.HostHealth != nil {
		return m.HostHealth.DrainReason
	}
	return ""
}

func (q *Queue) hostHygieneReason(m *Machine) string {
	for _, other := range q.s.Machines {
		if hostID(other) == hostID(m) && other.Replacement == nil && !other.Gone && !q.unreported[other.ID] {
			if reason := hostHygieneReason(other); reason != "" {
				return reason
			}
		}
	}
	return hostHygieneReason(m)
}
