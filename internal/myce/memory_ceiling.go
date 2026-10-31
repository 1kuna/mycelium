package myce

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// All paths stay below the worker service: disable confirms the entire subtree.
type memoryFS interface {
	ReadFile(string) ([]byte, error)
	ReadDir(string) ([]os.DirEntry, error)
	WriteFile(string, []byte) error
	Mkdir(string) error
	Remove(string) error
}
type osMemoryFS struct{}

func (osMemoryFS) ReadDir(p string) ([]os.DirEntry, error) { return os.ReadDir(p) }
func (osMemoryFS) ReadFile(p string) ([]byte, error)       { return os.ReadFile(p) }
func (osMemoryFS) WriteFile(p string, b []byte) error      { return os.WriteFile(p, b, 0600) }
func (osMemoryFS) Mkdir(p string) error                    { return os.Mkdir(p, 0700) }
func (osMemoryFS) Remove(p string) error                   { return os.Remove(p) }

type memoryCeilings struct {
	fs       memoryFS
	root     string
	pageSize int // Zero uses the host kernel page size.
}
type memoryAttempt struct {
	fs           memoryFS
	path         string
	bytes        int64
	pageSize     int
	hostBaseline *gpuHostState // Captured while the attempt cgroup is still empty.
}

var ErrMemoryCeilingsRequired = errors.New("required memory ceilings unavailable")

func delegateMemory(fs memoryFS, root string, pid int) (*memoryCeilings, error) {
	b, err := fs.ReadFile(filepath.Join(root, "cgroup.controllers"))
	if err != nil {
		return nil, err
	}
	if !strings.Contains(" "+strings.TrimSpace(string(b))+" ", " memory ") {
		return nil, errors.New("cgroup v2 memory controller not delegated")
	}
	leaf := filepath.Join(root, "worker")
	if err = fs.Mkdir(leaf); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	// Domain controllers cannot be enabled while the parent has internal processes.
	if err = fs.WriteFile(filepath.Join(leaf, "cgroup.procs"), []byte(strconv.Itoa(pid))); err != nil {
		return nil, err
	}
	members, err := fs.ReadFile(filepath.Join(leaf, "cgroup.procs"))
	if err != nil || !strings.Contains(" "+strings.Join(strings.Fields(string(members)), " ")+" ", " "+strconv.Itoa(pid)+" ") {
		return nil, errors.New("worker leaf membership not confirmed")
	}
	members, err = fs.ReadFile(filepath.Join(root, "cgroup.procs"))
	if err != nil || strings.TrimSpace(string(members)) != "" {
		return nil, errors.New("worker service parent still contains processes")
	}
	if err = fs.WriteFile(filepath.Join(root, "cgroup.subtree_control"), []byte("+memory")); err != nil {
		return nil, err
	}
	b, err = fs.ReadFile(filepath.Join(root, "cgroup.subtree_control"))
	if err != nil {
		return nil, err
	}
	if !strings.Contains(strings.ReplaceAll(string(b), "+", ""), "memory") {
		return nil, errors.New("memory delegation did not take effect")
	}
	return &memoryCeilings{fs: fs, root: root}, nil
}
func (w *Worker) initMemoryCeilings() {
	if w.memoryCeilings != nil {
		w.Machine.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
		return
	}
	reason := "requires Linux cgroup v2"
	if runtime.GOOS == "linux" {
		reason = "requires a delegated worker service; foreground join is admission-only"
		if ValidWorkerUnit(w.Unit) {
			b, err := os.ReadFile("/proc/self/cgroup")
			if err == nil {
				reason = "cgroup v2 service membership unavailable"
				for _, line := range strings.Split(string(b), "\n") {
					if !strings.HasPrefix(line, "0::/") {
						continue
					}
					p := filepath.Clean(strings.TrimPrefix(line, "0::"))
					if filepath.Base(p) == "worker" {
						p = filepath.Dir(p)
					}
					if filepath.Base(p) != w.Unit {
						continue
					}
					w.memoryCeilings, err = delegateMemory(osMemoryFS{}, filepath.Join("/sys/fs/cgroup", p), os.Getpid())
					if err == nil {
						w.Machine.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
						return
					}
					break
				}
			}
			if err != nil {
				reason = "memory delegation setup failed; inspect worker log"
				fmt.Fprintf(os.Stderr, "memory ceiling delegation: %v\n", err)
			}
		}
	}
	w.Machine.MemoryEnforcement = "memory ceiling not enforced on this host (" + reason + ")"
	if w.processMemory == nil && workerProcessMemory != nil {
		w.processMemory = workerProcessMemory()
	}
}
func (w Worker) requireMemoryCeilings() error {
	if !w.RequireMemoryCeilings {
		return nil
	}
	if w.memoryCeilings == nil {
		return fmt.Errorf("%w: %s; delegate memory to the worker service before enabling it", ErrMemoryCeilingsRequired, w.Machine.MemoryEnforcement)
	}
	// An empty child tests the exact per-attempt controls before any queue contact.
	probe, err := w.memoryCeilings.create("myce-ceiling-preflight", 128<<20, 0)
	if err != nil {
		return fmt.Errorf("%w: cgroup preflight: %v", ErrMemoryCeilingsRequired, err)
	}
	if err = probe.fs.Remove(probe.path); err != nil {
		return fmt.Errorf("%w: remove preflight child: %v", ErrMemoryCeilingsRequired, err)
	}
	return nil
}

func memoryBytes(gb float64) (int64, error) {
	if !validMemoryValue(gb) || gb >= float64(math.MaxInt64)/(1<<30) {
		return 0, errors.New("invalid memory ceiling")
	}
	return int64(math.Floor(gb * (1 << 30))), nil
}

// Round positive limits down to whole kernel pages so enforced <= reserved.
// Sub-page requests get one page; hostNeed reserves at least the largest
// supported page size. Zero stays zero, preserving disabled swap.
func pageAlignedMemory(bytes int64, pageSize int) (int64, error) {
	if bytes < 0 || pageSize <= 0 {
		return 0, errors.New("invalid memory ceiling or page size")
	}
	if bytes == 0 {
		return 0, nil
	}
	page := int64(pageSize)
	return max(page, bytes/page*page), nil
}
func (c *memoryCeilings) create(id string, bytes, swap int64) (*memoryAttempt, error) {
	if c == nil || bytes == 0 {
		return nil, nil
	}
	if id == "" || filepath.Base(id) != id || id == "." || id == ".." {
		return nil, errors.New("invalid memory cgroup identity")
	}
	pageSize := c.pageSize
	if pageSize == 0 {
		pageSize = os.Getpagesize()
	}
	bytes, err := pageAlignedMemory(bytes, pageSize)
	if err != nil {
		return nil, err
	}
	swap, err = pageAlignedMemory(swap, pageSize)
	if err != nil {
		return nil, err
	}
	a := &memoryAttempt{fs: c.fs, path: filepath.Join(c.root, id), bytes: bytes, pageSize: pageSize}
	a.hostBaseline, _ = readGPUHostState(c.fs)
	if err := c.fs.Mkdir(a.path); err != nil {
		return nil, err
	}
	for _, v := range []struct{ name, value string }{{"memory.max", strconv.FormatInt(bytes, 10)}, {"memory.swap.max", strconv.FormatInt(swap, 10)}, {"memory.oom.group", "1"}} {
		if err := c.fs.WriteFile(filepath.Join(a.path, v.name), []byte(v.value)); err != nil {
			_ = a.cleanup(RealClock{})
			return nil, err
		}
		b, err := c.fs.ReadFile(filepath.Join(a.path, v.name))
		if err != nil || strings.TrimSpace(string(b)) != v.value {
			_ = a.cleanup(RealClock{})
			return nil, errors.New("memory ceiling write did not take effect")
		}
	}
	return a, nil
}
func (a *memoryAttempt) attach(pid int) error {
	if a == nil {
		return nil
	}
	if err := a.fs.WriteFile(filepath.Join(a.path, "cgroup.procs"), []byte(strconv.Itoa(pid))); err != nil {
		return err
	}
	b, err := a.fs.ReadFile(filepath.Join(a.path, "cgroup.procs"))
	if err != nil {
		return err
	}
	for _, member := range strings.Fields(string(b)) {
		if member == strconv.Itoa(pid) {
			return nil
		}
	}
	return errors.New("memory ceiling process membership not confirmed")
}
func (a *memoryAttempt) failure() error {
	if a == nil {
		return nil
	}
	b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.events"))
	if err != nil {
		return fmt.Errorf("read memory ceiling OOM status: %w", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == "oom_kill" {
			n, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				return err
			}
			if n > 0 {
				return fmt.Errorf("exceeded its %g GiB memory ceiling", float64(a.bytes)/(1<<30))
			}
			return nil
		}
	}
	return errors.New("memory ceiling OOM status missing")
}

// Hierarchical oom_kill includes victims of a descendant's own limit. Require
// this cgroup's local OOM trigger before attributing the failure to its cap.
func (a *memoryAttempt) localLimitFailure() error {
	if a == nil {
		return nil
	}
	b, err := a.fs.ReadFile(filepath.Join(a.path, "memory.events.local"))
	if err != nil {
		return fmt.Errorf("read local memory ceiling OOM status: %w", err)
	}
	events := memoryCounters(b)
	if events["oom"] == 0 && events["oom_group_kill"] == 0 {
		return nil
	}
	return a.failure()
}

func (a *memoryAttempt) setOOMGroup(whole bool) error {
	if a == nil {
		return nil
	}
	value := "0"
	if whole {
		value = "1"
	}
	path := filepath.Join(a.path, "memory.oom.group")
	if err := a.fs.WriteFile(path, []byte(value)); err != nil {
		return err
	}
	b, err := a.fs.ReadFile(path)
	if err != nil || strings.TrimSpace(string(b)) != value {
		return errors.New("memory OOM group mode not confirmed")
	}
	return nil
}

// Kill the entire attempt subtree, including descendants that changed sessions.
func (a *memoryAttempt) cleanup(clock Clock) error {
	if err := a.stop(clock); err != nil {
		return err
	}
	if a == nil {
		return nil
	}
	return a.removeEmptyTree()
}

// Enable child controllers while the parent has no internal processes.
func (a *memoryAttempt) children() (*memoryCeilings, error) {
	if a == nil {
		return nil, nil
	}
	b, err := a.fs.ReadFile(filepath.Join(a.path, "cgroup.procs"))
	if err != nil || strings.TrimSpace(string(b)) != "" {
		return nil, errors.New("memory parent must be empty before delegation")
	}
	if err := a.setOOMGroup(false); err != nil {
		return nil, err
	}
	for _, v := range []struct{ name, value string }{{"cgroup.subtree_control", "+memory"}} {
		if err := a.fs.WriteFile(filepath.Join(a.path, v.name), []byte(v.value)); err != nil {
			return nil, err
		}
		b, err = a.fs.ReadFile(filepath.Join(a.path, v.name))
		if err != nil || strings.TrimPrefix(strings.TrimSpace(string(b)), "+") != strings.TrimPrefix(v.value, "+") {
			return nil, errors.New("child memory delegation not confirmed")
		}
	}
	return &memoryCeilings{fs: a.fs, root: a.path}, nil
}

// cgroup.kill empties descendants but leaves directories. Recovery uses the
// same bottom-up removal without needing an in-memory child list.
func (a *memoryAttempt) removeEmptyTree() error {
	entries, err := a.fs.ReadDir(a.path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			child := &memoryAttempt{fs: a.fs, path: filepath.Join(a.path, entry.Name())}
			if err := child.removeEmptyTree(); err != nil {
				return err
			}
		}
	}
	return a.fs.Remove(a.path)
}

// A descendant can leave its PGID while remaining in the attempt cgroup.
// Give those processes TERM too, and retain the full grace while any remain.
func (a *memoryAttempt) termOutsideGroups(groups map[int]bool) error {
	_, err := a.outsideGroups(groups, true)
	return err
}

func (a *memoryAttempt) outsideGroups(groups map[int]bool, term bool) (bool, error) {
	if a == nil {
		return false, nil
	}
	data, err := a.fs.ReadFile(filepath.Join(a.path, "cgroup.procs"))
	if err != nil {
		return false, err
	}
	alive := false
	for _, value := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(value)
		if err != nil || pid <= 0 {
			return false, errors.New("invalid cgroup process inventory")
		}
		group, err := syscall.Getpgid(pid)
		if errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return false, err
		}
		if !groups[group] {
			alive = true
			if term {
				if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
					return false, err
				}
			}
		}
	}
	entries, err := a.fs.ReadDir(a.path)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			childAlive, err := (&memoryAttempt{fs: a.fs, path: filepath.Join(a.path, entry.Name())}).outsideGroups(groups, term)
			if err != nil {
				return false, err
			}
			alive = alive || childAlive
		}
	}
	return alive, nil
}

func (a *memoryAttempt) populated() (bool, error) {
	if a == nil {
		return false, nil
	}
	data, err := a.fs.ReadFile(filepath.Join(a.path, "cgroup.events"))
	if err != nil {
		return false, err
	}
	fields := strings.Fields(string(data))
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == "populated" {
			return fields[i+1] != "0", nil
		}
	}
	return false, errors.New("cgroup population unconfirmed")
}

// Stop and verify the subtree while retaining its reservation for a group retry.
func (a *memoryAttempt) stop(clock Clock) error {
	if a == nil {
		return nil
	}
	if err := a.fs.WriteFile(filepath.Join(a.path, "cgroup.kill"), []byte("1")); err != nil {
		return err
	}
	if clock == nil {
		clock = RealClock{}
	}
	deadline := clock.Now().Add(5 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		b, err := a.fs.ReadFile(filepath.Join(a.path, "cgroup.events"))
		if err != nil {
			return err
		}
		fields := strings.Fields(string(b))
		empty := false
		for i := 0; i+1 < len(fields); i += 2 {
			if fields[i] == "populated" {
				empty = fields[i+1] == "0"
			}
		}
		if empty {
			return nil
		}
		if !clock.Now().Before(deadline) {
			return errors.New("memory attempt cgroup release unconfirmed")
		}
		<-tick.C
	}
}
func (w Worker) ceilingBytes(gpu, host float64) (int64, error) {
	if host == 0 {
		if w.RequireMemoryCeilings || w.Machine.HostMemoryBudgetGB > 0 {
			return 0, errors.New("worker requires positive host_memory_gb")
		}
		return 0, nil
	}
	bytes, err := memoryBytes(hostNeed(&w.Machine, gpu, host))
	if err != nil || w.MemorySwapMaxBytes < 0 || w.MemorySwapMaxBytes > math.MaxInt64-bytes {
		return 0, errors.New("invalid memory ceiling or swap limit")
	}
	return bytes, nil
}

// Only a canonical numeric ceiling message may pass the public privacy boundary.
func ceilingDiagnostic(value, fallback string) string {
	const prefix = "exceeded its "
	const suffix = " GiB memory ceiling"
	text := strings.TrimPrefix(value, "command failed: ")
	if text == "Docker memory ceiling not enforced" {
		return text
	}
	if strings.HasPrefix(text, prefix) && strings.HasSuffix(text, suffix) {
		n, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimPrefix(text, prefix), suffix), 64)
		if err == nil && n > 0 && validMemoryValue(n) && text == fmt.Sprintf("exceeded its %g GiB memory ceiling", n) {
			return text
		}
	}
	return diagnostic(value, fallback)
}

// MemoryCeilingConfig validates CLI and installed configuration before startup.
func MemoryCeilingConfig(budget, slack float64, swap int64) (int64, error) {
	if !validMemoryValue(budget) || !validMemoryValue(slack) || swap < 0 {
		return 0, errors.New("memory budget/slack/swap must be finite and nonnegative")
	}
	bytes, err := memoryBytes(budget + slack)
	if err != nil || swap > math.MaxInt64-bytes {
		return 0, errors.New("memory ceiling configuration overflows bytes")
	}
	return bytes, nil
}

// Inspect the worker's actual service subtree; installer never moves its own PID.
func WorkerMemoryDelegationStatus(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) string {
	no := func(reason string) string { return "memory ceiling not enforced on this host (" + reason + ")" }
	if c.Detection.OS != "linux" {
		return no("requires Linux cgroup v2")
	}
	if command == nil {
		return no("systemd inspection unavailable")
	}
	b, err := command(ctx, "systemctl", "--user", "show", filepath.Base(c.Service), "-p", "Delegate", "-p", "ControlGroup")
	if err != nil {
		return no("service delegation inspection failed")
	}
	props := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok {
			props[k] = v
		}
	}
	if props["Delegate"] != "yes" {
		return no("service memory delegation unavailable")
	}
	group := props["ControlGroup"]
	if group == "" || !strings.HasPrefix(group, "/") || filepath.Base(group) != filepath.Base(c.Service) {
		return no("worker service not running yet; check status after startup")
	}
	enabled, err := os.ReadFile(filepath.Join("/sys/fs/cgroup", group, "cgroup.subtree_control"))
	if err != nil || !strings.Contains(" "+strings.TrimSpace(string(enabled))+" ", " memory ") {
		return no("worker has not enabled delegated memory; check status and journal")
	}
	return "memory ceilings enforced (cgroup v2)"
}

func memoryEnforcementDiagnostic(value string) string {
	if value == "memory ceilings enforced (cgroup v2)" {
		return value
	}
	for _, reason := range []string{"requires Linux cgroup v2", "requires a delegated worker service; foreground join is admission-only", "cgroup v2 service membership unavailable", "memory delegation setup failed; inspect worker log"} {
		if value == "memory ceiling not enforced on this host ("+reason+")" {
			return value
		}
	}
	if value == "" {
		return "memory ceiling not enforced on this host (worker did not report enforcement)"
	}
	return "memory ceiling not enforced on this host (invalid capability report)"
}
