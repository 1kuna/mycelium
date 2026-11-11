package myce

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/probeguard"
)

// A byte-bounded ring per device retains individual lines, including bursts.
// The wire envelope holds one worst-case escaped four-table host snapshot.
// This bounds copying during driver storms independently of uptime or host size;
// its cost is explicit eviction of older optional snapshots. Generic authored
// reports and unique attempt outcomes retain their existing policies.
// Counts explicitly disclose eviction; no log-rate or admission gate is added.
// Text reads are limited to 64 KiB: proc memory tables normally occupy a few
// pages; truncation preserves control-plane capacity on very large NUMA hosts.
const faultSectionBytes = 64 << 10

// Four proc tables, with JSON's worst-case six-byte escape per input byte.
const faultHistoryBytes = 4 * faultSectionBytes * 6

type FaultEvidence struct {
	Event             string            `json:"event"`
	Kind              string            `json:"kind"`
	Device            string            `json:"device"`
	KernelAt          time.Time         `json:"kernel_at"`
	CapturedAt        time.Time         `json:"captured_at"`
	CaptureNS         int64             `json:"capture_ns"`
	Replaced          uint64            `json:"replaced,omitempty"`
	DeviceAttribution string            `json:"device_attribution"`
	Attempts          []FaultAttempt    `json:"attempts"`
	Host              map[string]string `json:"host"`
	OmittedAttempts   int               `json:"omitted_attempts,omitempty"`
	Unavailable       []string          `json:"unavailable,omitempty"`
}

type FaultAttempt struct {
	AssignedAt *time.Time `json:"assigned_at,omitempty"`
	Execution
	Session      string    `json:"session"`
	PIDs         []int     `json:"pids"`
	PIDError     bool      `json:"pid_unavailable,omitempty"`
	HostGB       *float64  `json:"host_gb,omitempty"`
	HostAt       time.Time `json:"host_at"`
	DeviceGB     *float64  `json:"device_gb,omitempty"`
	DeviceAt     time.Time `json:"device_at"`
	DeviceSource string    `json:"device_source,omitempty"`
}

// No commands, argv, environment, process names or raw kernel text are retained.
func boundedFaultRead(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, faultSectionBytes+1))
}

func localFaultHost() (map[string]string, []string) {
	if runtime.GOOS != "linux" {
		return nil, []string{"host_page_state_unsupported"}
	}
	probeguard.Check()
	return readFaultHost(boundedFaultRead)
}

func readFaultHost(read func(string) ([]byte, error)) (map[string]string, []string) {
	host := map[string]string{}
	var unavailable []string
	for _, name := range []string{"meminfo", "vmstat", "buddyinfo", "pressure/memory"} {
		b, err := read("/proc/" + name)
		if err != nil {
			unavailable = append(unavailable, name)
			continue
		}
		if len(b) > faultSectionBytes {
			b = b[:faultSectionBytes]
			unavailable = append(unavailable, name+"_truncated")
		}
		// Fixed proc tables contain counters only. Keep numeric/punctuation tokens
		// and known field names, never arbitrary source text.
		var lines []string
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			valid := true
			for _, token := range f {
				for _, r := range token {
					if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || strings.ContainsRune("_:,.=", r)) {
						valid = false
					}
				}
			}
			if valid {
				lines = append(lines, strings.Join(f, " "))
			}
		}
		host[name] = strings.Join(lines, "\n")
	}
	return host, unavailable
}

func allocationKernelFault(d MachineDevice, line string) (string, string) {
	if match := backendFor(d.Platform).allocationFault; match != nil {
		return match(d, line)
	}
	return "", ""
}

func cudaAllocationKernelFault(d MachineDevice, line string) (string, string) {
	s := strings.ToLower(line)
	if !strings.Contains(s, "nvrm:") || !(strings.Contains(s, "nv_err_no_memory") || strings.Contains(s, "allocation failed with big page size")) {
		return "", ""
	}
	// NVRM often omits its PCI address. Keep the ambiguity explicit on every
	// CUDA device rather than inventing a positively identified allocating card.
	if kernelPCI.MatchString(line) {
		if !kernelLineOnDevice(d, line) {
			return "", ""
		}
		return "nvrm_allocation", "pci"
	}
	return "nvrm_allocation", "cuda_candidates_no_kernel_device"
}

func (w *Worker) captureFault(d MachineDevice, line KernelLogLine, key, kind, attribution string) *FaultEvidence {
	started := time.Now() // Measurement only; event/decision times use Clock.
	e := &FaultEvidence{Event: key, Kind: kind, Device: d.ID, KernelAt: line.At, CapturedAt: w.Clock.Now(), DeviceAttribution: attribution}
	source := w.FaultHostSource
	if source == nil {
		source = workerFaultHost
	}
	e.Host, e.Unavailable = source()
	var processes []*process
	if w.control != nil {
		w.control.mu.Lock()
		for _, p := range w.control.processes {
			processes = append(processes, p)
		}
		w.control.mu.Unlock()
	}
	for _, p := range processes {
		j := p.job
		if j.Spec.Needs.cpuOnly() || !slices.Contains(j.AllocatedDevices, d.ID) {
			continue
		}
		a := FaultAttempt{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, AssignedAt: j.AssignedAt}
		p.mu.Lock()
		memory := p.memory
		roots := p.rssRootsLocked()
		if p.native != nil {
			memory = p.native.memory
		}
		p.mu.Unlock()
		ids := map[int]bool{}
		if memory != nil {
			a.PIDError = memory.processIDs(ids) != nil
		} else {
			source := w.FaultProcessSource
			if source == nil {
				source = func(roots map[int]string) ([]int, error) {
					return faultOwnedPIDs(roots, processBirth, workerGroupMembers)
				}
			}
			members, err := source(roots)
			a.PIDError = err != nil
			for _, pid := range members {
				ids[pid] = true
			}
		}
		for pid := range ids {
			a.PIDs = append(a.PIDs, pid)
		}
		sort.Ints(a.PIDs)
		s := w.hostMemorySamples[jobKey(j)]
		if !s.at.IsZero() {
			gb := s.gb
			a.HostGB, a.HostAt = &gb, s.at
		}
		if !s.deviceAt.IsZero() {
			gb := s.deviceGB
			a.DeviceGB, a.DeviceAt, a.DeviceSource = &gb, s.deviceAt, s.source
		}
		e.Attempts = append(e.Attempts, a)
	}
	slices.SortFunc(e.Attempts, func(a, b FaultAttempt) int { return strings.Compare(a.ID, b.ID) })
	boundFaultEvidence(e)
	e.CaptureNS = time.Since(started).Nanoseconds()
	return e
}

// Called in Checkin's transaction. Only immutable, bounded worker data is used;
// host/proc/kernel/core reads all happened outside the queue process.
func (q *Queue) acceptFaultEvidence(m *Machine) {
	for _, d := range m.Devices {
		for _, e := range d.FaultEvidence {
			if e == nil {
				continue
			}
			for _, a := range e.Attempts {
				j := q.s.Jobs[a.ID]
				if j == nil || j.Attempt != a.Attempt || j.Session != a.Session || j.Machine != m.ID || j.Session != m.Session || !slices.Contains(j.AllocatedDevices, d.ID) {
					continue
				}
				next := retainFaultEvidence(j.FaultEvidence, e)
				if slices.Equal(next, j.FaultEvidence) {
					continue
				}
				j = q.touchJob(j.ID)
				j.FaultEvidence = next
			}
		}
	}
}

// Retention is byte based: large NUMA/process inventories retain fewer events.
// Capture records do not grow with uptime or a driver error storm.
func retainFaultEvidence(old []*FaultEvidence, e *FaultEvidence) []*FaultEvidence {
	for _, v := range old {
		if v.Event == e.Event && v.Device == e.Device {
			return old
		}
	}
	next := append(slices.Clone(old), e)
	size := 0
	for _, v := range next {
		size += faultEvidenceBytes(v)
	}
	for len(next) > 1 && size > faultHistoryBytes {
		size -= faultEvidenceBytes(next[0])
		copy := *e
		copy.Replaced += 1 + next[0].Replaced
		e = &copy
		next = append(slices.Clone(next[1:len(next)-1]), e)
	}
	return next
}

// Conservative encoded-size charge: a JSON string byte can expand to six
// bytes. No serialization, hashing or filesystem work runs under q.mu.
func faultEvidenceBytes(e *FaultEvidence) int {
	n := 1024 + 6*(len(e.Event)+len(e.Kind)+len(e.Device)+len(e.DeviceAttribution))
	for k, v := range e.Host {
		n += 6*(len(k)+len(v)) + 8
	}
	for _, v := range e.Unavailable {
		n += 6*len(v) + 4
	}
	for _, a := range e.Attempts {
		n += 512 + 6*(len(a.ID)+len(a.Session)+len(a.DeviceSource)) + 24*len(a.PIDs)
	}
	return n
}
func boundFaultEvidence(e *FaultEvidence) {
	for faultEvidenceBytes(e) > faultHistoryBytes && len(e.Attempts) > 0 {
		e.Attempts = e.Attempts[:len(e.Attempts)-1]
		e.OmittedAttempts++
	}
	for _, name := range []string{"buddyinfo", "vmstat", "meminfo", "pressure/memory"} {
		if faultEvidenceBytes(e) <= faultHistoryBytes {
			break
		}
		delete(e.Host, name)
		e.Unavailable = append(e.Unavailable, name+"_retention_budget")
	}
}

// A file-core pointer is metadata only. Mycelium never opens, copies or deletes
// the core, whose contents can include credentials and application memory.
func corePatternPath(pattern string, pid int, at time.Time, dir string) (string, bool) {
	if strings.HasPrefix(pattern, "|") || strings.ContainsAny(pattern, "\n\x00") {
		return "", false
	}
	pattern = strings.ReplaceAll(pattern, "%%", "\x00")
	pattern = strings.ReplaceAll(pattern, "%p", strconv.Itoa(pid))
	pattern = strings.ReplaceAll(pattern, "%P", strconv.Itoa(pid))
	pattern = strings.ReplaceAll(pattern, "%t", strconv.FormatInt(at.Unix(), 10))
	if strings.Contains(pattern, "%") {
		return "", false
	}
	pattern = strings.ReplaceAll(pattern, "\x00", "%")
	if !filepath.IsAbs(pattern) {
		pattern = filepath.Join(dir, pattern)
	}
	return pattern, true
}

// Reuse cleanup's whole session/group inventory, fenced by recorded root and
// member births on both sides of enumeration. A vanished root is unknown.
func faultOwnedPIDs(roots map[int]string, birthOf func(int) (string, error), membersOf func(int) ([]int, error)) ([]int, error) {
	if len(roots) == 0 {
		return nil, errors.New("attempt process ownership unavailable")
	}
	ids := map[int]string{}
	for root, saved := range roots {
		before, err := birthOf(root)
		if err != nil || saved == "" || before != saved {
			return nil, errors.New("attempt process ownership changed")
		}
		members, err := membersOf(root)
		if err != nil {
			return nil, err
		}
		for _, pid := range members {
			birth, err := birthOf(pid)
			if err != nil || birth == "" {
				return nil, errors.New("attempt member ownership unavailable")
			}
			ids[pid] = birth
		}
	}
	for root, saved := range roots {
		after, err := birthOf(root)
		if err != nil || after != saved {
			return nil, errors.New("attempt process ownership changed")
		}
	}
	var pids []int
	for pid, saved := range ids {
		after, err := birthOf(pid)
		if err != nil || after != saved {
			return nil, errors.New("attempt member ownership changed")
		}
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	return pids, nil
}
func faultAtLine(base *FaultEvidence, line KernelLogLine, key, kind, attribution string) *FaultEvidence {
	copy := *base
	copy.Event, copy.Kind, copy.KernelAt, copy.DeviceAttribution = key, kind, line.At, attribution
	copy.Attempts = nil
	for _, a := range base.Attempts {
		if a.AssignedAt == nil || line.At.IsZero() || !line.At.Before(*a.AssignedAt) {
			copy.Attempts = append(copy.Attempts, a)
		}
	}
	return &copy
}
