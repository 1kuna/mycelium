package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
)

func identityHash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func extensionEnvironmentIdentity(e Extension, m *Machine) string {
	keys := map[string]string{}
	if v := e.variant(machineTarget(m)); v != nil {
		for n, r := range v.Environments {
			keys[n] = r.Key
		}
	}
	return identityHash(keys)
}
func extensionCheckIdentity(e Extension, m *Machine) string {
	view, ok := e.forMachine(m)
	if !ok {
		return ""
	}
	return identityHash(struct {
		Digest    string
		Target    ExecutionTarget
		Check     []string
		Baselines map[string]ExtensionBaseline
	}{e.Digest, machineTarget(m), view.Check, view.Baselines})
}
func extensionPreparationKey(e Extension, m *Machine) string {
	return identityHash(struct {
		Digest      string
		Target      ExecutionTarget
		Fingerprint string
	}{e.Digest, machineTarget(m), m.ExtensionFingerprint})
}
func (q *Queue) extensionPreparationPlan(m *Machine) []EnvironmentRecipe {
	if !q.hostLaunchAllowed(m, m.Session) {
		return nil
	}
	var selected *Job
	var extension *Extension
	for _, j := range q.liveJobs() {
		if j.State != "queued" || !j.Spec.extensionJob() || !q.placementAllowed(j, m) {
			continue
		}
		for i := range j.Extensions {
			e := &j.Extensions[i]
			if !slices.Contains(e.platforms(), m.GPU) || e.targetUnavailableReason(machineTarget(m)) != "" {
				continue
			}
			prepared := false
			for _, r := range m.ExtensionReports {
				if r.Digest == e.Digest && r.Setup && (len(e.Variants) == 0 || r.Host == hostID(m) && r.Fingerprint == m.ExtensionFingerprint && r.EnvironmentIdentity == extensionEnvironmentIdentity(*e, m)) {
					prepared = true
				}
			}
			if prepared {
				if v := e.variant(machineTarget(m)); v != nil {
					for _, r := range v.Environments {
						if !slices.Contains(m.Environments, r.Key) {
							prepared = false
						}
					}
				}
				if len(e.Assets) > 0 && !slices.Contains(m.Environments, extensionPreparationKey(*e, m)) {
					prepared = false
				}
				if prepared {
					continue
				}
			}
			key := extensionPreparationKey(*e, m)
			failed := false
			for _, r := range m.EnvironmentReports {
				if r.Key == key && (r.State == "failed" || r.State == "uploading") {
					failed = true
				}
			}
			if failed {
				continue
			} // negative cache: new content/fingerprint permits another attempt
			if selected == nil || q.before(j, selected, m) {
				selected, extension = j, e
			}
		}
	}
	if extension == nil {
		return nil
	}
	return q.extensionPreparationOffer(*extension, m)
}

func (q *Queue) extensionPreparationOffer(e Extension, m *Machine) []EnvironmentRecipe {
	key := extensionPreparationKey(e, m)
	for _, r := range m.EnvironmentReports {
		if r.Key == key && (r.State == "failed" || r.State == "uploading") {
			return nil
		}
	}
	launch := q.issuePreparation(m, "environment", key)
	return []EnvironmentRecipe{{Key: key, Platform: m.GPU, Preparation: &ExtensionSelection{Name: e.Name, Version: e.Version, Digest: e.Digest}, Launch: &launch}}
}

// Registry bytes are fetched by their frozen digest, then reparsed and verified.
// All subprocesses and downloads share the existing preparation launch fence.
func (w Worker) prepareRegisteredExtension(ctx context.Context, plan EnvironmentRecipe) (err error) {
	if plan.Platform == "cpu" {
		w, err = w.forAttempt(Job{Spec: Spec{Needs: ResourceNeeds{Platform: Platform{Required: []string{"cpu"}}}}})
		if err != nil {
			return err
		}
	}
	if plan.Launch != nil && plan.Launch.Device != "" {
		w, err = w.forAttempt(Job{AllocatedDevices: []string{plan.Launch.Device}})
		if err != nil {
			return err
		}
	}
	if plan.Preparation == nil {
		return errors.New("missing extension preparation identity")
	}
	if err = checkLaunchGate(ctx); err != nil {
		return err
	}
	var e Extension
	if err = w.Client.Call(ctx, "GET", "/api/extensions/"+plan.Preparation.Digest, nil, &e); err != nil {
		return err
	}
	e, err = extensionFromFiles(e.Files)
	if err != nil {
		return err
	}
	if e.Digest != plan.Preparation.Digest || e.Name != plan.Preparation.Name || e.Version != plan.Preparation.Version || extensionPreparationKey(e, &w.Machine) != plan.Key {
		return errors.New("registry digest or preparation identity mismatch")
	}
	if err = w.environmentCache.init(); err != nil {
		return err
	}
	flight, err := w.environmentCache.lock(ctx, "extension-"+e.Digest, syscall.LOCK_EX)
	if err != nil {
		return err
	}
	defer unlockEnvironment(flight)
	if err = InstallExtension(w.ExtensionRoot, e); err != nil {
		return err
	}
	// A delayed/repeated offer is not a new environment identity. Retain the
	// exact check only while every prepared prefix is still ready and use-locked.
	if v := e.variant(machineTarget(&w.Machine)); v != nil {
		checked := true
		old := readExtensionReports(w.ExtensionRoot, e, w.Machine.GPU)
		for _, work := range extensionWorks(e) {
			found := false
			for _, r := range old {
				if r.Work == work && extensionQualificationReason(e, r, &w.Machine) == "" {
					found = true
				}
			}
			checked = checked && found
		}
		if checked {
			recipes := []EnvironmentRecipe{}
			for _, recipe := range v.Environments {
				recipes = append(recipes, recipe)
			}
			if len(v.Setup) > 0 || len(e.Assets) > 0 {
				recipes = append(recipes, EnvironmentRecipe{Key: plan.Key})
			}
			for _, recipe := range recipes {
				recipe.readyOnly = true
				_, release, readyErr := w.environmentCache.Acquire(ctx, recipe)
				if errors.Is(readyErr, errExtensionUnavailable) {
					checked = false
					break
				}
				if readyErr != nil {
					return readyErr
				}
				defer release()
			}
			if checked {
				return nil
			}
		}
	}
	reports := []ExtensionReport{}
	for _, work := range extensionWorks(e) {
		reports = append(reports, newExtensionReport(e, &w.Machine, work))
	}
	defer func() {
		for i := range reports {
			reports[i].Setup = err == nil
			reports[i].Qualified = false
			reports[i].Reason = "check required"
			if err != nil {
				reports[i].Reason = "environment build failed: " + err.Error()
			}
		}
		err = errors.Join(err, writeExtensionReports(w.ExtensionRoot, e, w.Machine.GPU, reports))
	}()
	if err = writeExtensionReports(w.ExtensionRoot, e, w.Machine.GPU, reports); err != nil {
		return err
	}
	// Each environment retains a use lock until setup and asset staging complete.
	roots := map[string]string{}
	if v := e.variant(machineTarget(&w.Machine)); v != nil {
		names := make([]string, 0, len(v.Environments))
		for n := range v.Environments {
			names = append(names, n)
		}
		slices.Sort(names)
		for _, n := range names {
			r := v.Environments[n]
			var release func()
			roots[n], release, err = w.environmentCache.Acquire(ctx, r)
			if err != nil {
				return err
			}
			defer release()
		}
	}
	view, ok := e.forMachine(&w.Machine)
	if !ok {
		return errors.New("extension target unavailable")
	}
	// No synthetic environment for preparation that has nothing left to do.
	if len(view.Setup) == 0 && len(e.Assets) == 0 {
		return nil
	}
	root := filepath.Join(w.environmentCache.Root, plan.Key)
	env, err := extensionEnvironment(w.ExtensionRoot, e, w.Machine.GPU, w.Machine.Device)
	if err == nil {
		var controls map[string]string
		controls, err = w.deviceEnvironment()
		for k, v := range controls {
			env[k] = v
		}
	}
	if err != nil {
		return err
	}
	setup := view.Setup
	if v := e.variant(machineTarget(&w.Machine)); v != nil {
		main := v.Execution.Main.Environment
		if roots[main] != "" {
			bindEnvironment(env, roots[main])
		}
		for name, prefix := range roots {
			setup = expandArgs(setup, executionReplacer(env["MYCE_EXTENSION_DIR"], prefix, name, machineTarget(&w.Machine)))
		}
	}
	setup = expandArgs(setup, extensionAssetReplacer(e, root))
	setup = expandArgs(setup, strings.NewReplacer("{extension}", env["MYCE_EXTENSION_DIR"], "{cache}", env["MYCE_EXTENSION_CACHE"]))
	recipe := EnvironmentRecipe{Key: plan.Key, Assets: e.Assets}
	recipe.prepare = func(ctx context.Context, root string, out io.Writer) error {
		if len(setup) == 0 {
			return nil
		}
		return runRecordedCommand(ctx, setup, env["MYCE_EXTENSION_DIR"], cleanExtensionEnv(env), out, commandScope{StateDir: root, Clock: w.Clock}, "build-state.json")
	}
	_, release, err := w.environmentCache.Acquire(ctx, recipe)
	if release != nil {
		defer release()
	}
	return err
}
func extensionAssetReplacer(e Extension, root string) *strings.Replacer {
	pairs := []string{}
	for name := range e.Assets {
		pairs = append(pairs, "{assets."+name+"}", filepath.Join(root, "assets", name))
	}
	return strings.NewReplacer(pairs...)
}
func newExtensionReport(e Extension, m *Machine, work string) ExtensionReport {
	r := ExtensionReport{Device: extensionReportDevice(m.GPU, m.Device), Name: e.Name, Version: e.Version, Digest: e.Digest, Platform: m.GPU, Work: work, Fingerprint: m.ExtensionFingerprint, Host: hostID(m), EnvironmentIdentity: extensionEnvironmentIdentity(e, m), CheckDigest: extensionCheckIdentity(e, m), Reason: "check required"}
	if len(e.Variants) > 0 {
		t := machineTarget(m)
		r.Target = &t
	}
	return r
}

// extensionQualificationReason is the single qualification predicate for check
// verdicts, cached reports and placement. Empty means qualified for this host.
// A successful command alone is only smoke evidence, never qualification.
func extensionQualificationReason(e Extension, r ExtensionReport, m *Machine) string {
	view, ok := e.forMachine(m)
	if m.Devices != nil && (extensionReportDevice(r.Platform, r.Device) != extensionReportDevice(m.GPU, m.Device) || r.Fingerprint == "" || r.Fingerprint != m.ExtensionFingerprint) {
		return "device identity changed; check required"
	}
	if !ok || !e.matchesTarget(m) || !slices.Contains(e.platforms(), m.GPU) {
		return "extension target does not match this host"
	}
	if len(view.Check) == 0 {
		return "declare a check command and a native baseline in baselines[" + r.Work + "]"
	}
	if _, ok := view.baseline(r.Work, m.GPU); !ok {
		return "declare a native baseline in baselines[" + r.Work + "] for the check command"
	}
	if r.Digest != e.Digest || r.Platform != m.GPU || !r.Qualified {
		return "successful check required for this extension version and platform"
	}
	if len(e.Variants) == 0 {
		return ""
	} // existing legacy checks retain their wire contract
	if r.Target == nil || *r.Target != machineTarget(m) || r.Host == "" || r.Host != hostID(m) || r.Fingerprint == "" || r.Fingerprint != m.ExtensionFingerprint || r.EnvironmentIdentity != extensionEnvironmentIdentity(e, m) || r.CheckDigest != extensionCheckIdentity(e, m) || r.CheckID == "" || !r.Setup {
		return "host, environment or check identity changed; check required"
	}
	return ""
}

func (q *Queue) extensionByDigest(digest string) (Extension, error) {
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	q.mu.RLock()
	for _, e := range q.s.Extensions {
		if e.Digest == digest {
			store := q.files
			// Detach maps before hydrating outside the queue mutex.
			b, _ := json.Marshal(e)
			var out Extension
			_ = json.Unmarshal(b, &out)
			q.mu.RUnlock()
			err := store.hydrateExtension(&out)
			return out, err
		}
	}
	q.mu.RUnlock()
	return Extension{}, fmt.Errorf("extension digest is not registered")
}

// The legacy single fake GPU and canonical fake device 0 are the same fixture.
func extensionReportDevice(platform, device string) string {
	if platform == "cpu" {
		return ""
	}
	if platform == "fake" && device == "" {
		return "0"
	}
	return device
}
