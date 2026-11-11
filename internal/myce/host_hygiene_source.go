package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"mycelium/internal/probeguard"
)

var workerHostHealthSource = localHostHealth
var workerForeignConsumers = localForeignConsumers

func localHostHealth(ctx context.Context) (HostHealthReading, error) {
	probeguard.Check()
	probeStart := time.Now()
	r := HostHealthReading{pressurePSI: runtime.GOOS == "linux"}
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return r, err
		}
		fields := strings.Fields(string(b))
		if len(fields) == 0 {
			return r, errors.New("load unavailable")
		}
		r.Load, err = strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return r, err
		}
		b, err = os.ReadFile("/proc/pressure/memory")
		if err != nil {
			return r, err
		}
		r.Pressure, err = sharedMemoryPSI(b)
		if err != nil {
			return r, err
		}
		b, err = os.ReadFile("/proc/meminfo")
		if err != nil {
			return r, err
		}
		values := map[string]float64{}
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				values[f[0]], _ = strconv.ParseFloat(f[1], 64)
			}
		}
		r.SwapBytes = max(0, values["SwapTotal:"]-values["SwapFree:"]) * 1024
		r.availableBytes = values["MemAvailable:"] * 1024
	} else if runtime.GOOS == "darwin" {
		b, err := exec.CommandContext(ctx, "/usr/sbin/sysctl", "-n", "vm.loadavg", "kern.memorystatus_vm_pressure_level", "vm.swapusage").Output()
		if err != nil {
			return r, err
		}
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		if len(lines) != 3 {
			return r, errors.New("host counters unavailable")
		}
		f := strings.Fields(lines[0])
		if len(f) < 2 {
			return r, errors.New("load unavailable")
		}
		r.Load, err = strconv.ParseFloat(f[1], 64)
		if err != nil {
			return r, err
		}
		r.Pressure, err = strconv.ParseFloat(strings.TrimSpace(lines[1]), 64)
		if err != nil {
			return r, err
		}
		f = strings.Fields(lines[2])
		found := false
		for i := 0; i+2 < len(f); i++ {
			if f[i] == "used" && f[i+1] == "=" {
				value := f[i+2]
				if len(value) < 2 {
					break
				}
				n, e := strconv.ParseFloat(value[:len(value)-1], 64)
				if e != nil {
					return r, e
				}
				scale := map[byte]float64{'K': 1024, 'M': 1 << 20, 'G': 1 << 30}[value[len(value)-1]]
				if scale == 0 {
					break
				}
				r.SwapBytes, found = n*scale, true
			}
		}
		if !found {
			return r, errors.New("swap unavailable")
		}
	} else {
		return r, errors.New("host counters unsupported")
	}
	// Timer lateness measures scheduler latency without spinning. The OS wait
	// measures elapsed sensor time; decisions use injected-clock samples.
	started := time.Now()
	quantum := max(time.Nanosecond, time.Since(probeStart)/time.Duration(runtime.NumCPU()))
	timer := time.NewTimer(quantum)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return r, ctx.Err()
	case <-timer.C:
	}
	r.WakeSeconds = max(0, time.Since(started).Seconds()-quantum.Seconds())
	return r, nil
}

type healthProcess struct {
	pid, parent      int
	cpu, age         float64
	rss              uint64
	user, executable string
}

type foreignInventory struct {
	processes []ForeignConsumer
	services  []ForeignService
}

// psElapsed parses ps etime ("[[dd-]hh:]mm:ss"), which Linux and macOS share.
func psElapsed(value string) (float64, bool) {
	days := 0.0
	if d, rest, ok := strings.Cut(value, "-"); ok {
		n, err := strconv.ParseUint(d, 10, 32)
		if err != nil {
			return 0, false
		}
		days, value = float64(n), rest
	}
	seconds := 0.0
	for _, part := range strings.Split(value, ":") {
		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return 0, false
		}
		seconds = seconds*60 + float64(n)
	}
	return days*86400 + seconds, true
}

// Columns: pid ppid pcpu rss(KiB) etime user comm. The command keeps only its
// basename; argv and environment are never read.
func parseHealthProcesses(b []byte) []healthProcess {
	var processes []healthProcess
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 7 {
			continue
		}
		pid, e1 := strconv.Atoi(f[0])
		parent, e2 := strconv.Atoi(f[1])
		cpu, e3 := strconv.ParseFloat(f[2], 64)
		rss, e4 := strconv.ParseUint(f[3], 10, 64)
		age, ok := psElapsed(f[4])
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !ok || !validMemoryValue(cpu) {
			continue
		}
		processes = append(processes, healthProcess{pid: pid, parent: parent, cpu: cpu, age: age, rss: rss * 1024, user: f[5], executable: filepath.Base(strings.Join(f[6:], " "))})
	}
	return processes
}

// leaders keeps the top CPU and top memory (RSS plus attributed GPU) entries.
// These display bounds never affect admission.
func leaders[T any](out []T, cpu func(T) float64, memory func(T) uint64, id func(T) string) []T {
	byMemory := func(a, b T) int {
		if memory(a) != memory(b) {
			if memory(a) > memory(b) {
				return -1
			}
			return 1
		}
		return strings.Compare(id(a), id(b))
	}
	slices.SortFunc(out, func(a, b T) int {
		if cpu(a) != cpu(b) {
			if cpu(a) > cpu(b) {
				return -1
			}
			return 1
		}
		return byMemory(a, b)
	})
	result := slices.Clone(out[:min(5, len(out))])
	slices.SortFunc(out, byMemory)
	for _, p := range out[:min(5, len(out))] {
		if !slices.ContainsFunc(result, func(v T) bool { return id(v) == id(p) }) {
			result = append(result, p)
		}
	}
	return result
}

// foreignHealthInventory excludes the worker tree and verified attempt
// members. services maps a PID to its service or container identity; a
// process without one inherits its nearest labelled ancestor's (launchd
// labels only a job's main process). gpu is driver-attributed device memory.
func foreignHealthInventory(processes []healthProcess, owned map[int]bool, services map[int]string, gpu map[int]uint64) foreignInventory {
	parent := map[int]int{}
	for _, p := range processes {
		parent[p.pid] = p.parent
	}
	for changed := true; changed; {
		changed = false
		for _, p := range processes {
			if owned[p.parent] && !owned[p.pid] {
				owned[p.pid], changed = true, true
			}
		}
	}
	service := func(pid int) string {
		seen := map[int]bool{}
		for pid > 1 && !seen[pid] {
			if s := services[pid]; s != "" {
				return s
			}
			seen[pid] = true
			pid = parent[pid]
		}
		return ""
	}
	var out []ForeignConsumer
	totals := map[string]*ForeignService{}
	for _, p := range processes {
		if owned[p.pid] {
			continue
		}
		c := ForeignConsumer{PID: p.pid, Executable: p.executable, Service: service(p.pid), CPUPercent: p.cpu, RSSBytes: p.rss, User: p.user, AgeSeconds: p.age, GPUBytes: gpu[p.pid]}
		out = append(out, c)
		if c.Service == "" {
			continue
		}
		t := totals[c.Service]
		if t == nil {
			t = &ForeignService{Service: c.Service}
			totals[c.Service] = t
		}
		t.Processes++
		t.CPUPercent += c.CPUPercent
		t.RSSBytes += c.RSSBytes
		t.GPUBytes += c.GPUBytes
		if c.AgeSeconds >= t.AgeSeconds {
			t.AgeSeconds, t.User = c.AgeSeconds, c.User
		}
	}
	var grouped []ForeignService
	for _, t := range totals {
		grouped = append(grouped, *t)
	}
	return foreignInventory{
		processes: leaders(out, func(c ForeignConsumer) float64 { return c.CPUPercent }, func(c ForeignConsumer) uint64 { return c.RSSBytes + c.GPUBytes }, func(c ForeignConsumer) string { return fmt.Sprintf("%012d", c.PID) }),
		services:  leaders(grouped, func(s ForeignService) float64 { return s.CPUPercent }, func(s ForeignService) uint64 { return s.RSSBytes + s.GPUBytes }, func(s ForeignService) string { return s.Service }),
	}
}

// Linux cgroup paths name systemd units and scopes (including Docker's
// systemd driver, docker-ID.scope); the cgroupfs driver uses /docker/ID.
func cgroupService(b []byte) string {
	service := ""
	for _, line := range strings.Split(string(b), "\n") {
		components := strings.Split(line, "/")
		for i, component := range components {
			if strings.HasSuffix(component, ".service") || strings.HasSuffix(component, ".scope") {
				service = component
			} else if component == "docker" && i+1 < len(components) && components[i+1] != "" {
				service = "docker-" + components[i+1]
			}
		}
	}
	return service
}

// The device query can stall with its driver. It runs off the sampler lane,
// at most one at a time; the inventory uses the latest completed answer.
type foreignGPUQuery struct {
	mu      sync.Mutex
	running bool
	latest  map[int]uint64
}

var foreignGPU foreignGPUQuery

func parseForeignGPU(b []byte) map[int]uint64 {
	out := map[int]uint64{}
	for _, line := range strings.Split(string(b), "\n") {
		pidText, used, ok := strings.Cut(line, ",")
		if !ok {
			continue
		}
		pid, e1 := strconv.Atoi(strings.TrimSpace(pidText))
		mib, e2 := strconv.ParseUint(strings.TrimSpace(used), 10, 64)
		if e1 == nil && e2 == nil && pid > 0 {
			out[pid] += mib << 20 // A process can hold memory on several devices.
		}
	}
	return out
}

func (g *foreignGPUQuery) usage(ctx context.Context) map[int]uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.running {
		if path, err := exec.LookPath("nvidia-smi"); err == nil {
			g.running = true
			go func() {
				cmd := exec.CommandContext(ctx, path, "--query-compute-apps=pid,used_gpu_memory", "--format=csv,noheader,nounits")
				// A descendant can hold the pipe after exit; bound only that drain.
				cmd.WaitDelay = time.Second
				b, err := cmd.Output()
				g.mu.Lock()
				defer g.mu.Unlock()
				g.running = false
				g.latest = nil // A failed query is unknown, not the last answer.
				if err == nil {
					g.latest = parseForeignGPU(b)
				}
			}()
		}
	}
	return g.latest
}

func localForeignConsumers(ctx context.Context, dir string) (foreignInventory, error) {
	probeguard.Check()
	owned, err := hostHealthOwnedPIDs(dir)
	if err != nil {
		return foreignInventory{}, err
	}
	format := "pid=,ppid=,pcpu=,rss=,etime=,user=,comm="
	if runtime.GOOS == "linux" {
		format = "pid=,ppid=,pcpu=,rss=,etime=,user:32=,comm=" // Untruncated names.
	}
	b, err := exec.CommandContext(ctx, "ps", "-axo", format).Output()
	if err != nil {
		return foreignInventory{}, err
	}
	processes := parseHealthProcesses(b)
	services := map[int]string{}
	var serviceErr error
	if runtime.GOOS == "darwin" {
		data, err := exec.CommandContext(ctx, "launchctl", "list").Output()
		if err != nil {
			serviceErr = fmt.Errorf("service inventory unavailable")
		}
		for _, line := range strings.Split(string(data), "\n") {
			f := strings.Fields(line)
			if len(f) == 3 {
				if pid, err := strconv.Atoi(f[0]); err == nil {
					services[pid] = f[2]
				}
			}
		}
	} else if runtime.GOOS == "linux" {
		for _, p := range processes {
			if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", p.pid)); err == nil {
				if s := cgroupService(b); s != "" {
					services[p.pid] = s
				}
			}
		}
	}
	return foreignHealthInventory(processes, owned, services, foreignGPU.usage(ctx)), serviceErr
}

// Include surviving exact-attempt groups even after reparenting. A PID alone
// is never ownership: a matching recorded kernel birth is required.
func hostHealthOwnedPIDs(dir string) (map[int]bool, error) {
	owned := map[int]bool{os.Getpid(): true}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name != "state.json" && name != "legacy-state.json" && !(strings.HasPrefix(name, "state-attempt-") && strings.HasSuffix(name, ".json")) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var state workerState
		if err = decodeWorkerState(b, &state); err != nil {
			return nil, err
		}
		if state.PID > 0 && state.Birth != "" {
			birth, err := processBirth(state.PID)
			if err == nil && birth == state.Birth {
				owned[state.PID] = true
			}
		}
		for _, members := range state.Groups {
			for pid, expected := range members {
				if expected != "" {
					birth, err := processBirth(pid)
					if err == nil && birth == expected {
						owned[pid] = true
					}
				}
			}
		}
	}
	return owned, nil
}
