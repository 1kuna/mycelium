package myce

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const dockerEnvelopeUnavailable = "envelope-only Docker needs a shared cgroup parent; declare per-member caps or use a native recipe"

func dockerNeedsEnvelope(s Spec) bool {
	if !s.Run.Docker || len(s.Run.DockerSidecars) == 0 {
		return false
	}
	if s.Run.DockerMemoryGB == 0 {
		return true
	}
	for _, side := range s.Run.DockerSidecars {
		if side.MemoryGB == 0 {
			return true
		}
	}
	return false
}

// Systemd Docker cannot join an arbitrary delegated worker service subtree.
// Remote/Desktop daemons cannot share this kernel's cgroup either. Recheck
// the daemon before launch, then verify container membership in this kernel.
func (w Worker) dockerEnvelopeSupported() bool {
	if w.memoryCeilings == nil || !strings.HasPrefix(w.DockerEndpoint, "unix:///") {
		return false
	}
	if _, err := os.Stat(strings.TrimPrefix(w.DockerEndpoint, "unix://")); err != nil {
		return false
	}
	root, err := filepath.Rel("/sys/fs/cgroup", w.memoryCeilings.root)
	if err != nil || root == ".." || strings.HasPrefix(root, "../") {
		return false
	}
	d := dockerAttempt{binary: "docker", state: dockerExecution{Endpoint: w.DockerEndpoint}, timeout: w.dockerCallTimeout, command: w.dockerCommand, env: dockerClientEnvironment()}
	if d.timeout <= 0 {
		d.timeout = 2 * time.Second
	}
	b, err := d.call("info", "--format", "{{json .}}")
	if err != nil {
		return false
	}
	var info struct{ CgroupDriver, CgroupVersion, Name, OperatingSystem string }
	if json.Unmarshal(b, &info) != nil {
		return false
	}
	hostname, err := os.Hostname()
	return err == nil && info.CgroupDriver == "cgroupfs" && info.CgroupVersion == "2" && info.Name == hostname && !strings.Contains(strings.ToLower(info.OperatingSystem), "docker desktop")
}

func (d *dockerAttempt) envelope() *memoryAttempt {
	if d.memory != nil {
		return d.memory
	}
	if d.state.CgroupParent == "" {
		return nil
	}
	return &memoryAttempt{fs: osMemoryFS{}, path: filepath.Join("/sys/fs/cgroup", d.state.CgroupParent), bytes: d.state.MemoryLimitBytes}
}

// Persist OOM and peaks before removing counters, also during restart cleanup.
// The daemon must first prove every labeled member stopped.
func (d *dockerAttempt) releaseEnvelope() error {
	a := d.envelope()
	if a == nil {
		return nil
	}
	if _, err := a.fs.ReadFile(filepath.Join(a.path, "memory.max")); errors.Is(err, os.ErrNotExist) {
		if d.state.EnvelopeReleased {
			return nil
		}
		// Called only after successful daemon inventory proves all members
		// stopped. The delegated unit and its counters can vanish on restart.
		d.state.EnvelopeReleased, d.state.EnvelopeEvidenceLost = true, true
		d.state.MemoryPeak = nil
		return d.saveLocked()
	} else if err != nil {
		return err
	}
	if err := a.failure(); err != nil {
		if !strings.HasPrefix(ceilingDiagnostic(err.Error(), ""), "exceeded its ") {
			return err
		}
		d.state.OOMKilled = true
	}
	d.state.MemoryPeak = a.peak()
	if peak := d.state.MemoryPeak; d.memoryWorker == nil && peak != nil && (peak.HostGB != nil || peak.CgroupGB != nil || peak.DeviceGB != nil || peak.Constrained) {
		// Recovery has no running sampler's resident high-water mark. The
		// surviving cgroup only supplies a post-exit reading, not a job peak.
		d.state.MemoryPeak.Constrained = true
	}
	d.state.EnvelopeReleased = true
	if err := d.saveLocked(); err != nil {
		return err
	}
	return a.cleanup(d.clock)
}

func verifyDockerCgroup(pid int, parent string) error {
	if pid <= 0 {
		return errors.New("Docker shared parent process membership unavailable")
	}
	b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "cgroup"))
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "0::"+parent+"/") {
			return nil
		}
	}
	return errors.New("Docker shared parent process membership not confirmed")
}
