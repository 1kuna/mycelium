package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"
)

// CheckExtension checks a frozen local variant on this host. Host is the local
// worker's configured physical-host identity, not a remote execution request.
// Legacy extensions retain their baseline/setup contract in PrepareExtension.
func CheckExtension(ctx context.Context, root string, e Extension, platform, device, host string, log io.Writer, cache *EnvironmentCache, c Client) (reports []ExtensionReport, err error) {
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if len(e.Variants) == 0 {
		if _, err = PrepareExtension(ctx, root, e, platform, device, false, log, cache); err != nil {
			return nil, err
		}
		return PrepareExtension(ctx, root, e, platform, device, true, log, cache)
	}
	if host == "" {
		host, err = os.Hostname()
		if err != nil {
			return nil, err
		}
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
	if cache == nil {
		return nil, errors.New("environment cache required")
	}
	if err = cache.init(); err != nil {
		return nil, err
	}
	unlock, err := extensionLock(root, e, platform, false)
	if err != nil {
		return nil, err
	}
	defer unlock()
	for _, work := range extensionWorks(e) {
		r := newExtensionReport(e, &m, work)
		r.CheckID = ID()
		reports = append(reports, r)
	}
	if err = writeExtensionReports(root, e, platform, reports); err != nil {
		return reports, err
	}
	defer func() {
		if err != nil {
			for i := range reports {
				reports[i].Qualified = false
				reports[i].Reason = "check failed; see local command log"
			}
		}
		err = errors.Join(err, writeExtensionReports(root, e, platform, reports))
	}()
	// Check work uses the configured queue and only this enrolled local worker.
	// Absolute prepared paths are valid only on this exact target/host/device.
	var snapshot Snapshot
	if err = c.Call(ctx, "GET", "/api/status", nil, &snapshot); err != nil {
		return reports, err
	}
	var selected *Machine
	for _, candidate := range snapshot.Machines {
		if hostID(candidate) == host && candidate.GPU == platform && candidate.Device == device && candidate.ExtensionFingerprint == fingerprint && machineTarget(candidate) == machineTarget(&m) && !candidate.Gone && !candidate.Leaving {
			selected = candidate
			break
		}
	}
	if selected == nil {
		return reports, errors.New("no enrolled local worker matches host, target, device and fingerprint")
	}
	m = *selected
	scratch, err := os.MkdirTemp(extensionCache(root, e, platform), "check-")
	if err != nil {
		return reports, err
	}
	defer os.RemoveAll(scratch)
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	if err = c.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
		return reports, err
	}
	w := Worker{ExtensionRoot: root, Machine: m, Client: c, Clock: RealClock{}, environmentCache: cache}
	// The caller holds the extension lock; E2 owns exact folder/environment/asset preparation.
	plan := EnvironmentRecipe{Key: extensionPreparationKey(e, &m), Preparation: &ExtensionSelection{Name: e.Name, Version: e.Version, Digest: e.Digest}}
	if err = w.prepareRegisteredExtension(ctx, plan); err != nil {
		return reports, err
	}
	if len(e.Assets) > 0 {
		_, release, e2 := cache.Acquire(ctx, EnvironmentRecipe{Key: plan.Key, Assets: e.Assets, readyOnly: true})
		if e2 != nil {
			return reports, e2
		}
		releases = append(releases, release)
	}
	v := e.variant(machineTarget(&m))
	roots := map[string]string{}
	for name, r := range v.Environments {
		prefix, release, e2 := cache.Acquire(ctx, r)
		if e2 != nil {
			return reports, e2
		}
		releases = append(releases, release)
		roots[name] = prefix
	}
	base, err := extensionEnvironment(root, e, platform, device)
	if err != nil {
		return reports, err
	}
	replace := func(s string) string {
		s = strings.NewReplacer("{extension}", base["MYCE_EXTENSION_DIR"], "{cache}", base["MYCE_EXTENSION_CACHE"]).Replace(s)
		s = extensionAssetReplacer(e, filepath.Join(cache.Root, plan.Key)).Replace(s)
		s = executionReplacer(scratch, "", "", machineTarget(&m)).Replace(s)
		for name, prefix := range roots {
			s = executionReplacer(scratch, prefix, name, machineTarget(&m)).Replace(s)
		}
		return s
	}
	for i := range reports {
		r := &reports[i]
		r.Setup = true
		execution := v.Execution
		execution.Main.Env = maps.Clone(execution.Main.Env)
		execution.Sidecars = slices.Clone(execution.Sidecars)
		command := v.Check
		b, hasBaseline := v.Baselines[r.Work]
		if len(command) == 0 {
			command = v.Execution.Main.Command
		}
		for k, value := range execution.Main.Env {
			execution.Main.Env[k] = replace(value)
		}
		for n := range execution.Sidecars {
			side := &execution.Sidecars[n]
			side.Command = slices.Clone(side.Command)
			side.Env = maps.Clone(side.Env)
			for n, arg := range side.Command {
				side.Command[n] = replace(arg)
			}
			for k, value := range side.Env {
				side.Env[k] = replace(value)
			}
			ref := side.Environment
			if ref == "" {
				ref = v.Execution.Main.Environment
			}
			side.Command = extensionCheckLauncher(side.Command, base, roots[ref])
			side.Environment = ""
		}
		// env is the standard native launcher; control values are supplied by us,
		// never accepted from extension-authored execution.env.
		mainEnv := maps.Clone(base)
		if hasBaseline {
			mainEnv["MYCE_CHECK_INPUT"] = filepath.Join(base["MYCE_EXTENSION_DIR"], b.Input)
			mainEnv["MYCE_CHECK_OUTPUT"] = filepath.Join(scratch, "actual")
			mainEnv["MYCE_CHECK_WORK"] = r.Work
		}
		argv := make([]string, len(command))
		for n, arg := range command {
			argv[n] = replace(arg)
		}
		argv = extensionCheckLauncher(argv, mainEnv, roots[v.Execution.Main.Environment])
		execution.Main.Command = argv
		execution.Main.Environment = ""
		if len(v.Check) > 0 {
			execution.Main.Workdir = base["MYCE_EXTENSION_DIR"]
		} else if execution.Main.Workdir != "" {
			execution.Main.Workdir = replace(execution.Main.Workdir)
		} else {
			execution.Main.Workdir = scratch
		}
		needs := v.Needs
		needs.Platform = Platform{Required: []string{platform}}
		job, e2 := c.SubmitAndWait(ctx, Spec{Name: "extension-check", Project: "extension-check", SubmissionKey: r.CheckID, AllowedMachines: []string{m.ID}, Needs: needs, Execution: &execution})
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
			return reports, fmt.Errorf("runtime %s: %s", job.State, job.Reason)
		}
		if hasBaseline {
			output, e2 := os.Open(mainEnv["MYCE_CHECK_OUTPUT"])
			if e2 != nil {
				return reports, e2
			}
			actual, e2 := io.ReadAll(io.LimitReader(output, extensionMaxBytes+1))
			e2 = errors.Join(e2, output.Close())
			if e2 != nil {
				return reports, e2
			}
			if len(actual) > extensionMaxBytes {
				return reports, errors.New("check output too large")
			}
			if e2 = compareExtensionOutput(actual, e.Files[b.Output], b); e2 != nil {
				return reports, e2
			}
			if e2 = os.Remove(mainEnv["MYCE_CHECK_OUTPUT"]); e2 != nil {
				return reports, e2
			}
		}
		r.Qualified = true
		r.Reason = ""
	}
	return reports, nil
}

// Use the OS env launcher to bind already-prepared member environments. The
// ordinary native runtime still owns every member, port, readiness and stop.
func extensionCheckLauncher(command []string, base map[string]string, root string) []string {
	env := maps.Clone(base)
	if root != "" {
		bindEnvironment(env, root)
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	argv := []string{"env"}
	for _, key := range keys {
		argv = append(argv, key+"="+env[key])
	}
	return append(argv, command...)
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
