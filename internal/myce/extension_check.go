package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ExtensionCheckRequest selects a frozen check command and a local scratch
// directory. It never grants qualification or bypasses launch authorization.
type ExtensionCheckRequest struct {
	Work      string `json:"work" yaml:"work"`
	Directory string `json:"directory" yaml:"directory"`
}

// CheckExtension checks a frozen local variant on this host. Host is the local
// worker's configured physical-host identity, not a remote execution request.
func CheckExtension(ctx context.Context, root string, e Extension, platform, device, host string, log io.Writer, cache *EnvironmentCache, c Client) (reports []ExtensionReport, err error) {
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if platform == "cpu" {
		device = ""
	}
	if host == "" || platform != "cpu" && device == "" || cache == nil || cache.Root == "" {
		return nil, errors.New("runtime check requires --host, --device and --environment-cache (start myce dev for automatic local defaults)")
	}
	fingerprint, err := ExtensionFingerprint(ctx, platform, device)
	if err != nil {
		return nil, err
	}
	m := Machine{HostID: host, GPU: platform, Device: device, ExtensionFingerprint: fingerprint, CapabilityProfile: &CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH}}
	if reason := e.targetUnavailableReason(machineTarget(&m)); reason != "" {
		return nil, errors.New(reason)
	}
	actual, err := ReadExtension(extensionFolder(root, e))
	if err != nil {
		return nil, err
	}
	if actual.Digest != e.Digest {
		return nil, errors.New("extension content changed")
	}
	if err = cache.init(); err != nil {
		return nil, err
	}
	unlock, err := extensionLock(root, e, platform+"-check", false)
	if err != nil {
		return nil, err
	}
	defer unlock()
	// Check work uses the configured queue and only this enrolled local worker.
	// Absolute prepared paths are valid only on this exact target/host/device.
	var snapshot Snapshot
	if err = c.Call(ctx, "GET", "/api/status", nil, &snapshot); err != nil {
		return reports, err
	}
	var selected *Machine
	for _, owner := range snapshot.Machines {
		candidates := []*Machine{owner}
		if owner.Devices != nil {
			candidates = nil
			for _, d := range owner.Devices {
				target := d.machine(*owner)
				candidates = append(candidates, &target)
			}
			if platform == "cpu" {
				cpu := *owner
				cpu.GPU, cpu.Device = "cpu", ""
				cpu.ExtensionFingerprint = owner.CPUExtensionFingerprint
				candidates = append(candidates, &cpu)
			}
		}
		for _, candidate := range candidates {
			if candidate.GPU == "cpu" {
				cpu := *candidate
				cpu.Device = ""
				candidate = &cpu
			}
			if hostID(candidate) == host && candidate.GPU == platform && candidate.Device == device && candidate.ExtensionFingerprint == fingerprint && machineTarget(candidate) == machineTarget(&m) && !candidate.Gone && !candidate.Leaving {
				selected = candidate
				break
			}
		}
	}
	if selected == nil {
		return reports, errors.New("no enrolled local worker matches host, target, device and fingerprint")
	}
	if err = c.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
		return nil, err
	}
	m = *selected
	if err = os.MkdirAll(extensionCache(root, e, platform), 0700); err != nil {
		return nil, err
	}
	commandLog, err := os.CreateTemp(extensionCache(root, e, platform), "check-*.log")
	if err != nil {
		return nil, err
	}
	defer commandLog.Close()
	log = io.MultiWriter(log, commandLog)
	// Starting a check revokes qualification, not successful local setup. In
	// particular, a repaired local setup must remain usable when an earlier
	// automatic preparation is negative-cached. The queue still checks every
	// required prefix and setup marker before assigning the check.
	view, _ := e.forMachine(&m)
	previous := readExtensionReports(root, e, platform)
	localHost, _ := os.Hostname()
	for _, work := range extensionWorks(e) {
		r := newExtensionReport(e, &m, work)
		// Frozen files are already installed. Without setup steps or assets,
		// only ready environment prefixes remain, checked by the queue. This
		// also repairs records cleared by an older check invocation.
		r.Setup = len(view.Setup) == 0 && len(e.Assets) == 0
		for _, old := range previous {
			if old.Setup && old.Work == work && old.Digest == r.Digest && old.Platform == platform &&
				extensionReportDevice(platform, old.Device) == extensionReportDevice(platform, device) &&
				old.Fingerprint == r.Fingerprint && old.EnvironmentIdentity == r.EnvironmentIdentity &&
				(old.Host == r.Host || old.Host == localHost) &&
				(old.Target == nil && r.Target == nil || old.Target != nil && r.Target != nil && *old.Target == *r.Target) {
				r.Setup = true
				break
			}
		}
		r.CheckID = ID()
		reports = append(reports, r)
	}
	if err = writeExtensionReports(root, e, platform, reports); err != nil {
		return reports, err
	}
	defer func() {
		if err != nil {
			fmt.Fprintln(commandLog, err)
			for i := range reports {
				reports[i].Qualified = false
				reports[i].Reason = "check failed; command log: " + commandLog.Name()
			}
		}
		err = errors.Join(err, writeExtensionReports(root, e, platform, reports))
	}()

	scratch, err := os.MkdirTemp(extensionCache(root, e, platform), "check-")
	if err != nil {
		return reports, err
	}
	defer os.RemoveAll(scratch)
	for i := range reports {
		r := &reports[i]
		b, hasBaseline := view.baseline(r.Work, platform)
		selection := ResourceNeeds{}
		if v := e.variant(machineTarget(&m)); v != nil {
			selection.Memory = v.Needs.Memory
		}
		if platform != "cpu" {
			selection.GPU = &GPUSelection{Devices: []string{m.Device}}
		}
		job, e2 := c.SubmitAndWait(ctx, Spec{Name: "extension-check", Project: "extension-check", SubmissionKey: r.CheckID,
			Extension: extensionKey(e), ExtensionDigest: e.Digest, AllowedMachines: []string{m.ID},
			Needs: selection, ExtensionCheck: &ExtensionCheckRequest{Work: r.Work, Directory: scratch}})
		// Fence the exact check key and confirm cleanup, even when a terminal
		// result reports a failure with release still pending.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cleanupErr := waitExtensionCheckRelease(cleanupCtx, c, r.CheckID)
		cancel()
		if e2 != nil || cleanupErr != nil {
			return reports, errors.Join(e2, cleanupErr)
		}
		var chunks []LogChunk
		if e2 = c.Call(ctx, "GET", "/api/jobs/"+job.ID+"/logs?after=0", nil, &chunks); e2 == nil {
			for _, chunk := range chunks {
				fmt.Fprint(log, chunk.Text)
			}
		}
		if job.State != "succeeded" {
			if hasBaseline {
				if actual, readErr := readExtensionCheckOutput(scratch); readErr == nil {
					if mismatch := compareExtensionOutput(actual, e.Files[b.Output], b); mismatch != nil {
						return reports, mismatch
					}
				}
			}
			return reports, fmt.Errorf("runtime %s: %s", job.State, job.Reason)
		}
		if hasBaseline {
			actual, e2 := readExtensionCheckOutput(scratch)
			if e2 != nil {
				return reports, e2
			}
			if e2 = compareExtensionOutput(actual, e.Files[b.Output], b); e2 != nil {
				return reports, e2
			}
			if e2 = os.Remove(filepath.Join(scratch, "actual")); e2 != nil {
				return reports, e2
			}
		}
		r.Setup = true
		r.Qualified = true
		r.Reason = extensionQualificationReason(e, *r, &m)
		if r.Reason != "" {
			r.Qualified = false
			r.Reason = "ran OK but not qualified: " + r.Reason
		}
	}
	return reports, nil
}

// Expand all declared environment references from the prepared worker cache.
// The ordinary runtime still binds each member's environment and owns launch.
func expandExecutionReference(s *Spec, r *strings.Replacer) {
	s.Run.Command = expandArgs(s.Run.Command, r)
	s.Run.Workdir = r.Replace(s.Run.Workdir)
	for k, v := range s.Run.Env {
		s.Run.Env[k] = r.Replace(v)
	}
	for i := range s.Run.Sidecars {
		side := &s.Run.Sidecars[i]
		side.Command = expandArgs(side.Command, r)
		for k, v := range side.Env {
			side.Env[k] = r.Replace(v)
		}
	}
}

func waitExtensionCheckRelease(ctx context.Context, c Client, key string) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		ack, err := c.CancelSubmission(ctx, "extension-check", key)
		if err != nil {
			return err
		}
		if ack.Release != nil && *ack.Release != "pending" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// Static authoring checks supplement schema validation without executing code.
func StaticCheckExtension(e Extension) error {
	for _, v := range e.Variants {
		for _, side := range v.Execution.Sidecars {
			if side.Port != nil && *side.Port != 0 {
				return errors.New("sidecar port must be 0; use {port} for an allocated loopback port")
			}
		}
		if len(v.Check) > 0 && (v.Check[0] == "python3" || v.Check[0] == "python") {
			return errors.New("check uses bare Python; declare an environment and use {env.NAME.python}")
		}
	}
	return nil
}
