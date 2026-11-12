package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

// Match managed-storage sampling's five-second cadence. No scan reads payload
// bytes. 4096 metadata entries caps CPU/syscalls even on fast large trees;
// 25ms (0.5% of the cadence) caps cooperative local scanning. ReadDir batches
// avoid WalkDir's unbounded whole-directory allocation/sort. An OS syscall can
// exceed the budget: one background collector retains that slot until return,
// without blocking control, spawning retries or treating partial totals as idle.
const livenessScanEntries = 4096
const livenessScanBudget = 25 * time.Millisecond

var errLivenessUnavailable = errors.New("liveness observation unavailable")

func livenessWorkBytes(ctx context.Context, root string) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, livenessScanBudget)
	defer cancel()
	return scanLivenessWork(ctx, root, livenessScanEntries)
}

func scanLivenessWork(ctx context.Context, root string, limit int) (float64, error) {
	entries := 0
	var total float64
	var walk func(int, string) error
	walk = func(parent int, name string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries++
		if entries > limit {
			return errLivenessUnavailable
		}
		var st unix.Stat_t
		if err := unix.Fstatat(parent, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		switch st.Mode & unix.S_IFMT {
		case unix.S_IFREG:
			total += float64(st.Size)
			return ctx.Err()
		case unix.S_IFDIR:
		default:
			return nil // Never follow symlinks or open special files.
		}
		fd, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		f := os.NewFile(uintptr(fd), "liveness-directory")
		defer f.Close()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			batch, err := f.ReadDir(64)
			if err != nil && err != io.EOF {
				return err
			}
			for _, entry := range batch {
				if err := walk(fd, entry.Name()); err != nil {
					return err
				}
			}
			if err == io.EOF {
				return ctx.Err()
			}
		}
	}
	if err := walk(unix.AT_FDCWD, root); err != nil {
		return 0, err
	}
	return total, nil
}

// ps time is [[days-]hours:]minutes:seconds[.fraction] on Linux/macOS.
func parseLivenessCPUTime(text string) (float64, error) {
	days := 0.0
	if d, rest, ok := strings.Cut(text, "-"); ok {
		n, err := strconv.ParseUint(d, 10, 32)
		if err != nil {
			return 0, err
		}
		days, text = float64(n), rest
	}
	parts := strings.Split(text, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, errLivenessUnavailable
	}
	total := 0.0
	for _, p := range parts {
		n, err := strconv.ParseFloat(p, 64)
		if err != nil || !validMemoryValue(n) {
			return 0, errLivenessUnavailable
		}
		total = total*60 + n
	}
	return days*86400 + total, nil
}

func livenessTreeCPU(data []byte, root int) (float64, string, error) {
	type row struct {
		pid, parent, group int
		cpu                float64
		birth              string
	}
	rows := []row{}
	found := false
	owned := map[int]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 9 {
			return 0, "", errLivenessUnavailable
		}
		pid, e1 := strconv.Atoi(fields[0])
		parent, e2 := strconv.Atoi(fields[1])
		group, e3 := strconv.Atoi(fields[2])
		cpu, e4 := parseLivenessCPUTime(fields[3])
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
			return 0, "", errLivenessUnavailable
		}
		rows = append(rows, row{pid, parent, group, cpu, strings.Join(fields[4:], " ")})
		if pid == root {
			found = true
		}
		if pid == root || group == root {
			owned[pid] = true
		}
	}
	if !found {
		return 0, "", errLivenessUnavailable
	}
	// A child can have its own process group. Seed the anchor's group then follow
	// parent edges to include members' descendants without counting other jobs.
	children := map[int][]int{}
	for _, r := range rows {
		children[r.parent] = append(children[r.parent], r.pid)
	}
	queue := []int{}
	for pid := range owned {
		queue = append(queue, pid)
	}
	for i := 0; i < len(queue); i++ {
		for _, child := range children[queue[i]] {
			if !owned[child] {
				owned[child] = true
				queue = append(queue, child)
			}
		}
	}
	total := 0.0
	members := []string{}
	for _, r := range rows {
		if owned[r.pid] {
			total += r.cpu
			members = append(members, fmt.Sprintf("%d:%s", r.pid, r.birth))
		}
	}
	slices.Sort(members)
	return total, strings.Join(members, ";"), nil
}

var workerLivenessCPU = localLivenessCPU

func livenessProcessCPU(ctx context.Context, p *process, source func(context.Context, int, string) (float64, string, error)) (float64, string, error) {
	p.mu.Lock()
	cmd := p.cmd
	memory := p.memory
	if p.native != nil {
		cmd = p.native.supervisor
		memory = p.native.memory
	}
	root := 0
	birth := p.birth
	if cmd != nil && cmd.Process != nil {
		root = cmd.Process.Pid
		if p.native != nil {
			birth = p.native.custody.Groups[root][root]
		}
	}
	p.mu.Unlock()
	if memory != nil {
		if value, err := livenessCgroupCPU(memory); err == nil {
			return value, "cgroup", nil
		}
	}
	if root == 0 || birth == "" || source == nil {
		return 0, "", errLivenessUnavailable
	}
	return source(ctx, root, birth)
}

func livenessCgroupCPU(memory *memoryAttempt) (float64, error) {
	events, err := memory.fs.ReadFile(filepath.Join(memory.path, "cgroup.events"))
	if err != nil || !strings.Contains("\n"+string(events), "\npopulated 1\n") {
		return 0, errLivenessUnavailable
	}
	data, err := memory.fs.ReadFile(filepath.Join(memory.path, "cpu.stat"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "usage_usec" {
			n, err := strconv.ParseUint(fields[1], 10, 64)
			return float64(n) / 1e6, err
		}
	}
	return 0, errLivenessUnavailable
}

func localLivenessCPU(ctx context.Context, root int, ownedBirth string) (float64, string, error) {
	probeguard.Check()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return sampleLivenessCPU(ctx, root, ownedBirth, processBirth, func(ctx context.Context) ([]byte, error) {
		probe := exec.CommandContext(ctx, "ps", "-ax", "-o", "pid=,ppid=,pgid=,time=,lstart=")
		probe.WaitDelay = time.Second
		return probe.Output()
	})
}

// The launch custody identity, not the first observation, owns the anchor.
// Checking only equality across one ps call can adopt a PID reused beforehand.
func sampleLivenessCPU(ctx context.Context, root int, ownedBirth string, birth func(int) (string, error), inventory func(context.Context) ([]byte, error)) (float64, string, error) {
	before, err := birth(root)
	if err != nil || ownedBirth == "" || before != ownedBirth {
		return 0, "", errLivenessUnavailable
	}
	data, err := inventory(ctx)
	if err != nil || ctx.Err() != nil {
		return 0, "", errLivenessUnavailable
	}
	after, err := birth(root)
	if err != nil || after != ownedBirth {
		return 0, "", errLivenessUnavailable
	}
	return livenessTreeCPU(data, root)
}

func startAttemptLiveness(parent context.Context, clock Clock, interval time.Duration, p *process, spool string) *attemptLivenessSampler {
	ctx, cancel := context.WithCancel(parent)
	s := &attemptLivenessSampler{cancel: cancel, done: make(chan struct{})}
	root := filepath.Join(spool, "job-files", p.job.ID, strconv.Itoa(p.job.Attempt))
	source := workerLivenessCPU
	go func() {
		defer close(s.done)
		tick := time.NewTicker(max(interval, attemptStorageSamplePeriod))
		defer tick.Stop()
		var cpu, dir livenessCounter
		var members string
		for ctx.Err() == nil {
			n, identity, err := livenessProcessCPU(ctx, p, source)
			if identity != members && n <= cpu.value {
				cpu.valid = false
			}
			members = identity
			s.mu.Lock()
			s.report.CPU = cpu.observe(n, err, clock.Now(), s.report.CPU)
			s.mu.Unlock()
			n, err = livenessWorkBytes(ctx, root)
			s.mu.Lock()
			s.report.WorkDir = dir.observe(n, err, clock.Now(), s.report.WorkDir)
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	return s
}
