package myce

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"mycelium/internal/probeguard"
)

// These counters are observations, never admission or release authority. Pointers
// distinguish a measured zero from an unavailable OS/controller counter.
type AttemptCPUTime struct {
	// Linux cgroup cpu.pressure includes scheduling delay from other host work,
	// unlike quota throttling. Nil on older workers/unsupported platforms.
	Pressure         *IOPressure `json:"pressure,omitempty"`
	UsageSeconds     *float64    `json:"usage_seconds,omitempty"`
	UserSeconds      *float64    `json:"user_seconds,omitempty"`
	SystemSeconds    *float64    `json:"system_seconds,omitempty"`
	Periods          *uint64     `json:"periods,omitempty"`
	ThrottledPeriods *uint64     `json:"throttled_periods,omitempty"`
	ThrottledSeconds *float64    `json:"throttled_seconds,omitempty"`
	Source           string      `json:"source,omitempty"`
	Unavailable      string      `json:"unavailable,omitempty"`
	LowerBound       bool        `json:"lower_bound,omitempty"`
}
type PressureStall struct {
	Avg10   float64 `json:"avg10"`
	Avg60   float64 `json:"avg60"`
	Avg300  float64 `json:"avg300"`
	TotalUS uint64  `json:"total_usec"`
}
type IOPressure struct {
	Some        *PressureStall `json:"some,omitempty"`
	Full        *PressureStall `json:"full,omitempty"`
	Unavailable string         `json:"unavailable,omitempty"`
	SampledAt   time.Time      `json:"sampled_at"`
	AgeSeconds  float64        `json:"age_seconds"`
	ReceivedAt  time.Time      `json:"received_at,omitempty"`
}
type AttemptIO struct {
	ReadBytes   *uint64     `json:"read_bytes,omitempty"`
	WriteBytes  *uint64     `json:"write_bytes,omitempty"`
	ReadOps     *uint64     `json:"read_ops,omitempty"`
	WriteOps    *uint64     `json:"write_ops,omitempty"`
	Pressure    *IOPressure `json:"pressure,omitempty"`
	Unavailable string      `json:"unavailable,omitempty"`
}
type AttemptResources struct {
	CPU            AttemptCPUTime `json:"cpu"`
	IO             AttemptIO      `json:"io"`
	SampledAt      time.Time      `json:"sampled_at"`
	AgeSeconds     float64        `json:"age_seconds"`
	ElapsedSeconds float64        `json:"elapsed_seconds"`
	ReceivedAt     time.Time      `json:"received_at,omitempty"`
}
type AttemptResourceFacts struct {
	Attempt   int               `json:"attempt"`
	Machine   string            `json:"machine"`
	Session   string            `json:"session"`
	Resources *AttemptResources `json:"resources"`
}

func readIOPressure(read func(string) ([]byte, error), path string, at time.Time) *IOPressure {
	p := &IOPressure{SampledAt: at}
	b, err := read(path)
	if err != nil {
		p.Unavailable = "read_failed"
		return p
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if f[0] != "some" && f[0] != "full" {
			continue
		}
		values := map[string]string{}
		for _, s := range f[1:] {
			k, v, ok := strings.Cut(s, "=")
			if ok {
				values[k] = v
			}
		}
		stall := &PressureStall{}
		valid := true
		for k, out := range map[string]*float64{"avg10": &stall.Avg10, "avg60": &stall.Avg60, "avg300": &stall.Avg300} {
			v, e := strconv.ParseFloat(values[k], 64)
			if e != nil || !validMemoryValue(v) || v > 100 {
				valid = false
			}
			*out = v
		}
		stall.TotalUS, err = strconv.ParseUint(values["total"], 10, 64)
		if err != nil || !valid {
			p.Unavailable = "invalid_counters"
			p.Some = nil
			p.Full = nil
			return p
		}
		if f[0] == "some" {
			p.Some = stall
		} else {
			p.Full = stall
		}
	}
	if p.Some == nil {
		p.Unavailable = "invalid_counters"
		p.Full = nil
	}
	return p
}

var workerIOPressure = localIOPressure
var workerResourceCPU = localResourceCPU

func localIOPressure(clock Clock) *IOPressure {
	probeguard.Check()
	if runtime.GOOS != "linux" {
		return &IOPressure{SampledAt: clock.Now(), Unavailable: "unsupported_os"}
	}
	return readIOPressure(os.ReadFile, "/proc/pressure/io", clock.Now())
}

func cgroupResources(a *memoryAttempt, at time.Time) *AttemptResources {
	r := &AttemptResources{SampledAt: at, CPU: AttemptCPUTime{Source: "cgroup_v2", Unavailable: "read_failed"}, IO: AttemptIO{Unavailable: "read_failed"}}
	if a == nil {
		r.CPU.Unavailable = "no_local_cgroup"
		r.IO.Unavailable = "no_local_cgroup"
		return r
	}
	if b, err := a.fs.ReadFile(filepath.Join(a.path, "cpu.stat")); err == nil {
		m := memoryCounters(b)
		seconds := func(k string) *float64 {
			n, ok := m[k]
			if !ok {
				return nil
			}
			v := float64(n) / 1e6
			return &v
		}
		count := func(k string) *uint64 {
			n, ok := m[k]
			if !ok {
				return nil
			}
			return &n
		}
		r.CPU.UsageSeconds, r.CPU.UserSeconds, r.CPU.SystemSeconds = seconds("usage_usec"), seconds("user_usec"), seconds("system_usec")
		r.CPU.Periods, r.CPU.ThrottledPeriods, r.CPU.ThrottledSeconds = count("nr_periods"), count("nr_throttled"), seconds("throttled_usec")
		r.CPU.Unavailable = ""
		if r.CPU.UsageSeconds == nil {
			r.CPU.Unavailable = "invalid_counters"
		}
	}
	r.CPU.Pressure = readIOPressure(a.fs.ReadFile, filepath.Join(a.path, "cpu.pressure"), at)
	r.IO.Pressure = readIOPressure(a.fs.ReadFile, filepath.Join(a.path, "io.pressure"), at)
	if b, err := a.fs.ReadFile(filepath.Join(a.path, "io.stat")); err == nil {
		// Parent counters already include descendants. Sum devices exactly once;
		// never recurse into member cgroups or publish their device identities.
		totals := map[string]uint64{"rbytes": 0, "wbytes": 0, "rios": 0, "wios": 0}
		valid := true
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			f := strings.Fields(line)
			if len(f) == 0 {
				continue
			}
			row := map[string]bool{}
			for _, field := range f[1:] {
				k, v, ok := strings.Cut(field, "=")
				if _, known := totals[k]; !ok || !known {
					continue
				}
				n, e := strconv.ParseUint(v, 10, 64)
				if e != nil || row[k] || n > ^uint64(0)-totals[k] {
					valid = false
					continue
				}
				row[k] = true
				totals[k] += n
			}
			for k := range totals {
				if !row[k] {
					valid = false
				}
			}
		}
		if valid {
			r.IO.Unavailable = ""
			rb, wb, ro, wo := totals["rbytes"], totals["wbytes"], totals["rios"], totals["wios"]
			r.IO.ReadBytes, r.IO.WriteBytes, r.IO.ReadOps, r.IO.WriteOps = &rb, &wb, &ro, &wo
		} else {
			r.IO.Unavailable = "invalid_counters"
		}
	}
	return r
}

// Retain each observed process lifetime after exit, rather than the maximum of
// the live tree sum (which loses exited siblings). Short-lived unseen children
// remain a lower bound. lstart fences PID reuse, and launch custody fences roots.
func resourceTreeCPU(data []byte, roots map[int]string, seen map[string]float64) (float64, error) {
	type row struct {
		pid, parent, group int
		cpu                float64
		birth              string
	}
	rows := []row{}
	owned := map[int]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 9 {
			return 0, errLivenessUnavailable
		}
		pid, e1 := strconv.Atoi(f[0])
		ppid, e2 := strconv.Atoi(f[1])
		pgid, e3 := strconv.Atoi(f[2])
		cpu, e4 := parseLivenessCPUTime(f[3])
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
			return 0, errLivenessUnavailable
		}
		rows = append(rows, row{pid, ppid, pgid, cpu, strings.Join(f[4:], " ")})
		if _, ok := roots[pid]; ok {
			owned[pid] = true
		}
		if _, ok := roots[pgid]; ok {
			owned[pid] = true
		}
	}
	children := map[int][]int{}
	for _, r := range rows {
		children[r.parent] = append(children[r.parent], r.pid)
	}
	todo := []int{}
	for id := range owned {
		todo = append(todo, id)
	}
	for i := 0; i < len(todo); i++ {
		for _, id := range children[todo[i]] {
			if !owned[id] {
				owned[id] = true
				todo = append(todo, id)
			}
		}
	}
	current := map[string]float64{}
	total := seen[""] // Retired lifetimes need one scalar, never an unbounded PID log.
	for _, r := range rows {
		if owned[r.pid] {
			key := strconv.Itoa(r.pid) + ":" + r.birth
			current[key] = max(seen[key], r.cpu)
		}
	}
	for key, cpu := range seen {
		if key != "" {
			if _, live := current[key]; !live {
				total += cpu
			}
		}
		delete(seen, key)
	}
	seen[""] = total
	for key, cpu := range current {
		seen[key] = cpu
		total += cpu
	}
	return total, nil
}

func localResourceCPU(ctx context.Context, roots map[int]string, seen map[string]float64) (float64, error) {
	probeguard.Check()
	if len(roots) == 0 {
		return 0, errLivenessUnavailable
	}
	for id, b := range roots {
		birth, err := processBirth(id)
		if err != nil || b == "" || birth != b {
			return 0, errLivenessUnavailable
		}
	}
	cmd := exec.CommandContext(ctx, "ps", "-ax", "-o", "pid=,ppid=,pgid=,time=,lstart=")
	cmd.WaitDelay = attemptMemorySamplePeriod
	data, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	for id, b := range roots {
		birth, err := processBirth(id)
		if err != nil || birth != b {
			return 0, errLivenessUnavailable
		}
	}
	return resourceTreeCPU(data, roots, seen)
}

type resourceObserver struct {
	mu     sync.Mutex
	clock  Clock
	start  time.Time
	read   func() *AttemptResources
	value  *AttemptResources
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (o *resourceObserver) sample() {
	r := o.read()
	r.SampledAt = o.clock.Now()
	r.ElapsedSeconds = max(0, o.clock.Now().Sub(o.start).Seconds())
	o.mu.Lock()
	defer o.mu.Unlock()
	o.value = retainResourceCounters(o.value, r)
}

// Final counter reads can fail during teardown. Preserve prior measurements
// explicitly as lower bounds.
func retainResourceCounters(previous, r *AttemptResources) *AttemptResources {
	if r == nil {
		return previous
	}
	copy := *r
	r = &copy
	// A failed last read is explicit; retain previous CPU as a labeled lower
	// bound, never let a missing cgroup silently turn a measured run into zero.
	if old := previous; old != nil && r.CPU.UsageSeconds == nil && old.CPU.UsageSeconds != nil {
		r.CPU = old.CPU
		r.CPU.LowerBound = true
		r.CPU.Unavailable = "final_read_failed"
	}
	if old := previous; old != nil && r.IO.ReadBytes == nil && old.IO.ReadBytes != nil {
		pressure := r.IO.Pressure
		r.IO = old.IO
		r.IO.Pressure = pressure
		r.IO.Unavailable = "final_read_failed"
	}
	return r
}
func (o *resourceObserver) snapshot() *AttemptResources {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.value == nil {
		return nil
	}
	r := *o.value
	r.AgeSeconds = max(0, o.clock.Now().Sub(r.SampledAt).Seconds())
	return &r
}
func (o *resourceObserver) finish() *AttemptResources {
	if o == nil {
		return nil
	}
	o.once.Do(func() { o.cancel(); <-o.done; o.sample() })
	return o.snapshot()
}

func (w Worker) observeResources(p *process, memory *memoryAttempt) *resourceObserver {
	clock := w.Clock
	if clock == nil {
		clock = RealClock{}
	}
	o := &resourceObserver{clock: clock, start: clock.Now()}
	period := attemptMemorySamplePeriod
	seen := map[string]float64{}
	o.read = func() *AttemptResources { return cgroupResources(memory, clock.Now()) }
	if memory == nil && runtime.GOOS == "darwin" {
		// Native measurement found 32ms per ps tree inventory (6.4% of a
		// core at the RSS cadence). Reuse the existing five-second CPU/liveness
		// cadence instead: tenfold less inventory cost. Exit rusage catches roots;
		// unseen children remain explicitly a lower bound.
		period = attemptStorageSamplePeriod
		o.read = func() *AttemptResources {
			p.mu.Lock()
			root, birth := 0, p.birth
			if p.cmd != nil && p.cmd.Process != nil {
				root = p.cmd.Process.Pid
			}
			if p.native != nil && p.native.supervisor != nil {
				root = p.native.supervisor.Process.Pid
				birth = p.native.custody.Groups[root][root]
			}
			p.mu.Unlock()
			roots := map[int]string{}
			if root > 0 {
				roots[root] = birth
			}
			ctx, cancel := context.WithTimeout(context.Background(), period)
			defer cancel()
			cpu, err := 0.0, errLivenessUnavailable
			if workerResourceCPU != nil {
				cpu, err = workerResourceCPU(ctx, roots, seen)
			}
			r := &AttemptResources{SampledAt: clock.Now(), CPU: AttemptCPUTime{Source: "sampled_process_tree", LowerBound: true}, IO: AttemptIO{Unavailable: "unsupported_os", Pressure: &IOPressure{Unavailable: "unsupported_os", SampledAt: clock.Now()}}}
			if err != nil {
				r.CPU.Unavailable = "read_failed"
			} else {
				r.CPU.UsageSeconds = &cpu
			}
			return r
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	o.cancel = cancel
	o.done = make(chan struct{})
	// Sampling is independent of heartbeat/control and never runs under q.mu.
	go func() {
		defer close(o.done)
		o.sample()
		tick := time.NewTicker(period)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				o.sample()
			}
		}
	}()
	return o
}

func (q *Queue) retainAttemptResources(j *Job, r *AttemptResources) {
	if j != nil && q.s.Jobs[j.ID] == j {
		q.touchJob(j.ID)
	}
	if r == nil {
		return
	}
	r = safeAttemptResources(r)
	if r == nil {
		return
	}
	r.ReceivedAt = q.clock.Now()
	q.observeSharedCPU(j, r)
	facts := AttemptResourceFacts{Attempt: j.Attempt, Machine: j.Machine, Session: j.Session, Resources: r}
	for i, old := range j.ResourceAttempts {
		if old.Attempt == facts.Attempt && old.Machine == facts.Machine && old.Session == facts.Session {
			j.ResourceAttempts[i] = facts
			return
		}
	}
	j.ResourceAttempts = append(j.ResourceAttempts, facts)

}

// Root rusage catches short commands between process-tree samples. The sampled
// aggregate includes the root, so take max, never add it a second time.
func withExitCPU(r *AttemptResources, state *os.ProcessState) *AttemptResources {
	if r == nil || state == nil || r.CPU.Source != "sampled_process_tree" {
		return r
	}
	n := state.UserTime().Seconds() + state.SystemTime().Seconds()
	if r.CPU.UsageSeconds == nil || n > *r.CPU.UsageSeconds {
		r.CPU.UsageSeconds = &n
	}
	r.CPU.Unavailable = ""
	r.CPU.LowerBound = true
	return r
}

func (p *process) resourceSnapshot() *AttemptResources {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	o := p.resources
	p.mu.Unlock()
	return o.snapshot()
}

// Clone and validate the small telemetry value at ingress. No raw OS error,
// path, process identity or arbitrary source label enters public records.
func safeResourceReason(s string) string {
	switch s {
	case "", "read_failed", "final_read_failed", "invalid_counters", "no_local_cgroup", "unsupported_os":
		return s
	}
	return "invalid_counters"
}
func safeIOPressure(p *IOPressure) *IOPressure {
	if p == nil {
		return nil
	}
	v := *p
	v.ReceivedAt = time.Time{}
	v.Unavailable = safeResourceReason(v.Unavailable)
	if !validMemoryValue(v.AgeSeconds) {
		return nil
	}
	for _, field := range []**PressureStall{&v.Some, &v.Full} {
		if *field != nil {
			stall := **field
			if !validMemoryValue(stall.Avg10) || stall.Avg10 > 100 || !validMemoryValue(stall.Avg60) || stall.Avg60 > 100 || !validMemoryValue(stall.Avg300) || stall.Avg300 > 100 {
				return nil
			}
			*field = &stall
		}
	}
	return &v
}
func safeAttemptResources(r *AttemptResources) *AttemptResources {
	if r == nil || !validMemoryValue(r.AgeSeconds) || !validMemoryValue(r.ElapsedSeconds) {
		return nil
	}
	v := *r
	v.CPU.Unavailable = safeResourceReason(v.CPU.Unavailable)
	v.IO.Unavailable = safeResourceReason(v.IO.Unavailable)
	switch v.CPU.Source {
	case "cgroup_v2", "sampled_process_tree":
	default:
		v.CPU = AttemptCPUTime{Unavailable: "invalid_counters"}
	}
	for _, field := range []**float64{&v.CPU.UsageSeconds, &v.CPU.UserSeconds, &v.CPU.SystemSeconds, &v.CPU.ThrottledSeconds} {
		if *field != nil {
			n := **field
			if !validMemoryValue(n) {
				return nil
			}
			*field = &n
		}
	}
	for _, field := range []**uint64{&v.CPU.Periods, &v.CPU.ThrottledPeriods, &v.IO.ReadBytes, &v.IO.WriteBytes, &v.IO.ReadOps, &v.IO.WriteOps} {
		if *field != nil {
			n := **field
			*field = &n
		}
	}
	v.CPU.Pressure = safeIOPressure(v.CPU.Pressure)
	v.IO.Pressure = safeIOPressure(v.IO.Pressure)
	return &v
}
