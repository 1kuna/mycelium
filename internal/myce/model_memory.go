package myce

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// ModelMemoryPeaks describes sampled, absolute engine memory for one attempt.
// Request is the latest completed activation in status. The interval API also
// retains individually received request windows.
type ModelMemoryPeaks struct {
	Load    *ModelMemoryWindow `json:"load,omitempty"`
	Steady  *ModelMemoryWindow `json:"steady,omitempty"`
	Request *ModelMemoryWindow `json:"request,omitempty"`
}

type ModelMemoryWindow struct {
	StartedAt time.Time              `json:"started_at"`
	EndedAt   *time.Time             `json:"ended_at,omitempty"`
	RequestID string                 `json:"request_id,omitempty"`
	Overlap   bool                   `json:"overlaps_other_requests,omitempty"`
	Host      ModelMemoryMeasurement `json:"host"`
	Device    ModelMemoryMeasurement `json:"device"`
}

type ModelMemoryMeasurement struct {
	PeakGB              *float64   `json:"peak_gb,omitempty"`
	Source              string     `json:"source,omitempty"`
	SampledAt           *time.Time `json:"sampled_at,omitempty"`
	Samples             int        `json:"samples"`
	Complete            bool       `json:"complete"`
	Constrained         bool       `json:"constrained"`
	PressureUnavailable bool       `json:"pressure_unavailable,omitempty"`
	Unavailable         string     `json:"unavailable,omitempty"`
}

const (
	modelMemoryHostCgroup    = "cgroup anon, kernel and shmem samples"
	modelMemoryDevicePID     = "per-process device memory samples"
	modelMemoryNoSample      = "no sample within window"
	modelMemoryTrailer       = "X-Myce-Model-Memory"
	modelMemoryRequestHeader = "X-Myce-Model-Request-ID"
)

type modelMemoryChannel struct {
	sampling deviceSampling
	value    ModelMemoryMeasurement
	period   time.Duration
}

func (c *modelMemoryChannel) observe(at, samplingAt time.Time, gb *float64, source, unavailable string, period time.Duration, constrained, unknown bool) {
	c.value.Constrained = c.value.Constrained || constrained
	c.value.PressureUnavailable = c.value.PressureUnavailable || unknown
	c.period = period
	if gb == nil || !validMemoryValue(*gb) {
		c.sampling.incomplete = true
		c.value.Unavailable = unavailable
		return
	}
	if c.value.PeakGB == nil || *gb > *c.value.PeakGB {
		value := *gb
		c.value.PeakGB, c.value.Source = &value, source
	}
	c.value.SampledAt = &at
	c.sampling.observe(samplingAt, *gb, period)
}

func (c *modelMemoryChannel) snapshot(at time.Time, unavailable string) ModelMemoryMeasurement {
	out := c.value
	if coverage := c.sampling.finish(at, c.period); coverage != nil {
		out.Samples, out.Complete, out.Unavailable = coverage.Samples, coverage.Complete, ""
	} else if out.Unavailable == "" {
		out.Unavailable = unavailable
	}
	return out
}

type modelMemoryWindow struct {
	start        time.Time
	paused       time.Duration
	id           string
	overlap      bool
	host, device modelMemoryChannel
}

func newModelMemoryWindow(at time.Time) *modelMemoryWindow {
	return &modelMemoryWindow{start: at, host: modelMemoryChannel{sampling: deviceSampling{start: at}}, device: modelMemoryChannel{sampling: deviceSampling{start: at}}}
}

func (w *modelMemoryWindow) snapshot(at time.Time, ended bool, deviceUnavailable string, paused time.Duration) *ModelMemoryWindow {
	coverageAt := at.Add(-w.paused - paused)
	out := &ModelMemoryWindow{
		StartedAt: w.start, RequestID: w.id, Overlap: w.overlap,
		Host: w.host.snapshot(coverageAt, modelMemoryNoSample), Device: w.device.snapshot(coverageAt, deviceUnavailable),
	}
	// Containment and host pressure also affect how the device peak can
	// be interpreted. A successful driver read alone proves neither.
	out.Device.Constrained = out.Device.Constrained || out.Host.Constrained
	out.Device.PressureUnavailable = out.Device.PressureUnavailable || out.Host.PressureUnavailable || out.Host.Samples == 0
	if ended {
		out.EndedAt = &at
	}
	return out
}

// Only the existing attempt samplers feed this tracker. Changing windows and
// taking snapshots never performs sensor I/O or blocks on a sampler.
type modelMemoryTracker struct {
	mu                   sync.Mutex
	clock                Clock
	deviceUnavailable    string
	load, idle           *modelMemoryWindow
	idleSince            time.Time
	idlePausedAt         time.Time
	requests             map[*modelMemoryWindow]bool
	peaks                ModelMemoryPeaks
	stopped              bool
	lastHost, lastDevice *float64
}

func (m *modelMemoryTracker) snapshot() *ModelMemoryPeaks {
	m.mu.Lock()
	defer m.mu.Unlock()
	peaks := m.peaks
	at := m.clock.Now()
	if m.load != nil {
		peaks.Load = m.load.snapshot(at, false, m.deviceUnavailable, 0)
	}
	if m.idle != nil {
		peaks.Steady = m.idle.snapshot(at, false, m.deviceUnavailable, m.idlePause(at))
	}
	if peaks.Load == nil && peaks.Steady == nil && peaks.Request == nil {
		return nil
	}
	return &peaks
}

func (m *modelMemoryTracker) begin() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.load = newModelMemoryWindow(m.clock.Now())
	m.deviceUnavailable = modelMemoryNoSample
	m.peaks, m.stopped = ModelMemoryPeaks{}, false
	m.lastHost, m.lastDevice = nil, nil
}

func (m *modelMemoryTracker) setDeviceUnavailable(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deviceUnavailable = reason
}

func (m *modelMemoryTracker) ready() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.load != nil {
		at := m.clock.Now()
		m.peaks.Load = m.load.snapshot(at, true, m.deviceUnavailable, 0)
		m.load, m.idle, m.idleSince = nil, newModelMemoryWindow(at), at
	}
}

func (m *modelMemoryTracker) stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	at := m.clock.Now()
	if m.load != nil {
		m.peaks.Load = m.load.snapshot(at, true, m.deviceUnavailable, 0)
		m.load = nil
	}
	if m.idle != nil {
		m.peaks.Steady = m.idle.snapshot(at, true, m.deviceUnavailable, m.idlePause(at))
		m.idle = nil
	}
	m.stopped = true
}

func (m *modelMemoryTracker) startRequest(id string) func() *ModelMemoryWindow {
	m.mu.Lock()
	defer m.mu.Unlock()
	at := m.clock.Now()
	window := newModelMemoryWindow(at)
	if len(m.requests) == 0 {
		m.idlePausedAt = at
	}
	if !validCustodianID(id) {
		id = ID()
	}
	window.id = id
	for active := range m.requests {
		active.overlap, window.overlap = true, true
	}
	if m.requests == nil {
		m.requests = make(map[*modelMemoryWindow]bool)
	}
	m.requests[window] = true
	return func() *ModelMemoryWindow {
		m.mu.Lock()
		defer m.mu.Unlock()
		at := m.clock.Now()
		delete(m.requests, window)
		m.peaks.Request = window.snapshot(at, true, m.deviceUnavailable, 0)
		if len(m.requests) == 0 {
			if m.idle != nil {
				m.idle.paused += m.idlePause(at)
				m.idleSince = at
			}
			m.idlePausedAt = time.Time{}
		}
		return m.peaks.Request
	}
}

func (m *modelMemoryTracker) idlePause(at time.Time) time.Duration {
	if m.idlePausedAt.IsZero() {
		return 0
	}
	return max(0, at.Sub(m.idlePausedAt))
}

func (m *modelMemoryTracker) observe(start, at time.Time, peak *AttemptMemoryPeak, period time.Duration, device bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	if !device {
		m.lastHost = peak.HostGB
	}
	if device || peak.DeviceGB != nil {
		m.lastDevice = peak.DeviceGB
		if peak.DeviceUnavailable == deviceMemoryUnsupported {
			m.deviceUnavailable = deviceMemoryUnsupported
		}
	}
	record := func(window *modelMemoryWindow) {
		if window == nil || start.Before(window.start) || at.Before(start) {
			return
		}
		if device || peak.DeviceGB != nil {
			source := peak.DeviceSource
			if source == "" {
				source = modelMemoryDevicePID
			}
			window.device.observe(at, at.Add(-window.paused), peak.DeviceGB, source, peak.DeviceUnavailable, period, peak.constrained(), peak.PressureUnavailable)
		}
		if !device {
			source := peak.Source
			if source == "" {
				source = modelMemoryHostCgroup
			}
			window.host.observe(at, at.Add(-window.paused), peak.HostGB, source, "", period, peak.constrained(), peak.PressureUnavailable)
		}
	}
	record(m.load)
	if len(m.requests) == 0 && !start.Before(m.idleSince) {
		record(m.idle)
	}
	for window := range m.requests {
		record(window)
	}
}

type modelMemoryEngine interface {
	modelMemory() *modelMemoryTracker
}

func (l *LlamaEngine) modelMemory() *modelMemoryTracker { return l.memoryPeaks }

func (m *workerModel) memorySnapshot() *ModelMemoryPeaks {
	if e, ok := m.engine.(modelMemoryEngine); ok {
		if memory := e.modelMemory(); memory != nil {
			return memory.snapshot()
		}
	}
	return nil
}

func (m *workerModel) trackRequest(w http.ResponseWriter, id string) func() {
	if e, ok := m.engine.(modelMemoryEngine); ok {
		if memory := e.modelMemory(); memory != nil {
			finish := memory.startRequest(id)
			if !validCustodianID(id) {
				return func() { finish() }
			}
			w.Header().Add("Trailer", modelMemoryTrailer)
			return func() {
				if data, err := json.Marshal(finish()); err == nil {
					w.Header().Set(modelMemoryTrailer, string(data))
				}
			}
		}
	}
	return func() {}
}

func copyModelWorkerResponseHeaders(dst, src http.Header) {
	copyProxyHeaders(dst, src)
	if len(dst.Values("Trailer")) != 0 {
		// An HTTP/1.1 response with a fixed length cannot carry trailers.
		dst.Del("Content-Length")
	}
}

func readModelRequestMemory(response *http.Response, id string) *ModelMemoryWindow {
	var sample ModelMemoryWindow
	if err := json.Unmarshal([]byte(response.Trailer.Get(modelMemoryTrailer)), &sample); err != nil ||
		sample.RequestID != id || sample.StartedAt.IsZero() || sample.EndedAt == nil || sample.EndedAt.Before(sample.StartedAt) {
		return nil
	}
	return &sample
}

func newestModelMemoryRequest(old, next *ModelMemoryWindow) *ModelMemoryWindow {
	if next == nil || next.EndedAt == nil || old != nil && old.EndedAt != nil && next.EndedAt.Before(*old.EndedAt) {
		return old
	}
	return next
}

// Model memory measurements use the same owned-process probes as batch work.
func (l *LlamaEngine) observeModelMemory() {
	if l.memoryPeaks == nil {
		return
	}
	w := Worker{Machine: l.memoryMachine, Clock: l.Clock}
	if l.readinessWorker != nil {
		w = *l.readinessWorker
		w.Clock = l.Clock
	}
	if w.processMemory == nil && workerProcessMemory != nil {
		w.processMemory = workerProcessMemory()
	}
	memory := l.memoryAttempt
	owned := &process{cmd: l.cmd, memory: memory}
	owned.birth, _ = processBirth(l.cmd.Process.Pid)
	deviceTelemetry := memory != nil && backendFor(w.Machine.GPU).attemptMemory != nil
	switch {
	case deviceTelemetry, memory == nil && w.Machine.GPU == "metal" && w.processMemory != nil && w.processMemory.OwnedFootprint != nil:
		l.memoryPeaks.setDeviceUnavailable(modelMemoryNoSample)
	case backendFor(w.Machine.GPU).attemptMemory != nil:
		l.memoryPeaks.setDeviceUnavailable(deviceMemoryNoCgroup)
	default:
		l.memoryPeaks.setDeviceUnavailable(deviceMemoryUnsupported)
	}
	j := Job{Spec: Spec{Needs: ResourceNeeds{Platform: Platform{Required: []string{w.Machine.GPU}}}}}
	if memory != nil {
		l.stopMemory = w.observeAttemptMemoryWith(memory, j, nil, deviceMemorySamplePeriod, l.memoryPeaks.observe)
	} else {
		l.stopMemory = w.observeAttemptMemoryWith(nil, j, func() []int { return []int{owned.cmd.Process.Pid} }, deviceMemorySamplePeriod, l.memoryPeaks.observe, owned)
	}
}

// The model execs in the launch root itself. Unlike control-anchor accounting,
// include that root's RSS via the existing group inventory, with birth checks
// on both sides so a recycled group identity cannot supply memory evidence.
func verifiedProcessGroupResident(probe *processMemoryProbe, roots map[int]string, birthOf func(int) (string, error)) (uint64, int, error) {
	groups := map[int]bool{}
	check := func() error {
		for pid, saved := range roots {
			birth, err := birthOf(pid)
			if err != nil || saved == "" || birth != saved {
				return errLivenessUnavailable
			}
			groups[pid] = true
		}
		return nil
	}
	if err := check(); err != nil {
		return 0, 0, err
	}
	bytes, members, err := probe.Resident(groups)
	if err != nil {
		return 0, 0, err
	}
	if err := check(); err != nil {
		return 0, 0, err
	}
	return bytes, members, nil
}
