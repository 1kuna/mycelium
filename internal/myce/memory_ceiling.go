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
	"time"
)

// All paths stay below the worker service: disable confirms the entire subtree.
type memoryFS interface {
	ReadFile(string) ([]byte, error)
	WriteFile(string, []byte) error
	Mkdir(string) error
	Remove(string) error
}
type osMemoryFS struct{}

func (osMemoryFS) ReadFile(p string) ([]byte, error)  { return os.ReadFile(p) }
func (osMemoryFS) WriteFile(p string, b []byte) error { return os.WriteFile(p, b, 0600) }
func (osMemoryFS) Mkdir(p string) error               { return os.Mkdir(p, 0700) }
func (osMemoryFS) Remove(p string) error              { return os.Remove(p) }

type memoryCeilings struct {
	fs       memoryFS
	root     string
	pageSize int // Zero uses the host kernel page size.
}
type memoryAttempt struct {
	fs    memoryFS
	path  string
	bytes int64
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
	return int64(math.Ceil(gb * (1 << 30))), nil
}

// Round up at the cgroup boundary: Linux stores byte limits in whole pages.
// Reject overflow rather than granting less memory than requested.
func pageAlignedMemory(bytes int64, pageSize int) (int64, error) {
	if bytes < 0 || pageSize <= 0 {
		return 0, errors.New("invalid memory ceiling or page size")
	}
	page := int64(pageSize)
	if remainder := bytes % page; remainder != 0 {
		extra := page - remainder
		if bytes > math.MaxInt64-extra {
			return 0, errors.New("page-aligned memory ceiling overflows bytes")
		}
		bytes += extra
	}
	return bytes, nil
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
	a := &memoryAttempt{fs: c.fs, path: filepath.Join(c.root, id), bytes: bytes}
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

// Kill the entire attempt subtree, including descendants that changed sessions.
func (a *memoryAttempt) cleanup(clock Clock) error {
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
			return a.fs.Remove(a.path)
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
