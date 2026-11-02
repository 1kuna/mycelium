package myce

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Keep daemon and filesystem observations off the actor's control path. A slow
// probe drops credit; its original clock timestamp survives late completion.
func (d *dockerAttempt) currentHostMemory(clock Clock, fs memoryFS) *CurrentHostMemory {
	if a := d.envelope(); a != nil {
		return a.currentHostMemory(clock)
	}
	sample, err := d.hostMemoryProbe.run("host-memory", func() (*CurrentHostMemory, error) {
		return d.sampleHostMemory(clock, fs)
	})
	if err != nil {
		return nil
	}
	return sample
}

// The exact label inventory plus the full container ID in local /proc membership
// prove ownership in this kernel. Remote/Desktop PIDs alone cannot grant credit.
func (d *dockerAttempt) sampleHostMemory(clock Clock, fs memoryFS) (*CurrentHostMemory, error) {
	d.mu.Lock()
	endpoint, identity := d.state.Endpoint, d.state.Identity
	probe := &dockerAttempt{ctx: context.Background(), binary: d.binary, command: d.command, env: d.env, workdir: d.workdir, timeout: d.timeout, state: dockerExecution{Endpoint: endpoint}}
	d.mu.Unlock()
	if identity == "" || !strings.HasPrefix(endpoint, "unix:///") {
		return nil, errors.New("local Docker identity unavailable")
	}
	at := clock.Now()
	containers, err := inspectOwnedDocker(probe.ctx, endpoint, identity, func(_ context.Context, _ string, args ...string) ([]byte, error) {
		return probe.callWithTimeout(probe.timeout, args...)
	})
	if err != nil || len(containers) == 0 {
		return nil, errors.New("Docker memory inventory unavailable")
	}
	seen := map[string]bool{}
	total := 0.0
	for _, c := range containers {
		if !dockerLive(c) {
			continue
		}
		if c.State.Pid <= 0 {
			return nil, errors.New("Docker memory membership unavailable")
		}
		proc := filepath.Join("/proc", strconv.Itoa(c.State.Pid), "cgroup")
		membership, err := fs.ReadFile(proc)
		if err != nil {
			return nil, err
		}
		path := ""
		for _, line := range strings.Split(string(membership), "\n") {
			if !strings.HasPrefix(line, "0::/") {
				continue
			}
			rel := strings.TrimPrefix(line, "0::")
			leaf := filepath.Base(rel)
			if filepath.Clean(rel) == rel && rel != "/" && (leaf == c.ID || leaf == "docker-"+c.ID+".scope") {
				path = filepath.Join("/sys/fs/cgroup", rel)
			}
		}
		if path == "" || seen[path] {
			return nil, errors.New("Docker memory membership unconfirmed")
		}
		seen[path] = true
		a := &memoryAttempt{fs: fs, path: path}
		sample := a.currentHostMemory(clock)
		after, err := fs.ReadFile(proc)
		if err != nil || !bytes.Equal(membership, after) || sample == nil {
			return nil, errors.New("Docker memory sample unavailable")
		}
		total += sample.NonreclaimableGB
	}
	if len(seen) == 0 {
		return nil, errors.New("Docker running memory unavailable")
	}
	return &CurrentHostMemory{NonreclaimableGB: total, sampledAt: at}, nil
}

// Docker payloads are daemon descendants, outside the worker RSS tree. Missing
// current evidence makes this observation unknown instead of recording that
// managed memory as an external peak. Reservations/peaks are never substituted.
func creditDockerObservation(o *HostMemoryObservation, jobs []Job, reports []AttemptReport, now time.Time) *HostMemoryObservation {
	if o == nil {
		return nil
	}
	samples := map[attemptKey]*CurrentHostMemory{}
	for _, r := range reports {
		if r.Started {
			samples[attemptKey{r.ID, r.Attempt, r.Session}] = r.CurrentHostMemory
		}
	}
	managed := 0.0
	for _, j := range jobs {
		if !j.Spec.Run.Docker {
			continue
		}
		s, started := samples[jobKey(j)]
		if !started {
			continue
		}
		if s == nil || s.sampledAt.IsZero() || now.Before(s.sampledAt) || now.Sub(s.sampledAt) >= hostMemorySampleFreshFor || !validMemoryValue(s.NonreclaimableGB) {
			o.AttributionUnknown = true
			return o
		}
		managed += s.NonreclaimableGB
	}
	// Host and cgroup observations are not atomic; bound the sampled estimate.
	o.MyceliumGB = min(o.MyceliumGB+managed, o.UsedGB)
	return o
}
