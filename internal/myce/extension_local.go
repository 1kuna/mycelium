package myce

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"
)

var errExtensionUnavailable = errors.New("extension qualification unavailable")

const (
	extensionUncheckedReason          = "not yet checked on this device; run the extension check"
	extensionDriverChangedReason      = "hardware/driver fingerprint changed; setup and check required"
	extensionContentChangedReason     = "extension fingerprint (digest) changed; setup and check required"
	extensionFingerprintMissingReason = "hardware/driver fingerprint unavailable; setup and check required"
	extensionLibraryUnreadableReason  = "extension library unreadable"
	extensionLibraryPartialReason     = "some local extension registrations are unreadable"
)

// Keep only fixed report diagnostics in public status; arbitrary check output
// can contain private command details.
func extensionReportDiagnostic(reason string) string {
	switch reason {
	case extensionUncheckedReason, extensionDriverChangedReason, extensionContentChangedReason, extensionFingerprintMissingReason,
		extensionLibraryUnreadableReason, extensionLibraryPartialReason:
		return reason
	default:
		return diagnostic(reason, "setup and check required")
	}
}

type ExtensionReport struct {
	Device              string           `json:"device,omitempty"`
	Host                string           `json:"host,omitempty"`
	EnvironmentIdentity string           `json:"environment_identity,omitempty"`
	CheckDigest         string           `json:"check_digest,omitempty"`
	CheckID             string           `json:"check_id,omitempty"`
	CheckState          string           `json:"check_state,omitempty"`
	Target              *ExecutionTarget `json:"target,omitempty"`
	Name                string           `json:"name"`
	Version             string           `json:"version"`
	Digest              string           `json:"digest"`
	Platform            string           `json:"platform"`
	Work                string           `json:"work"`
	Fingerprint         string           `json:"fingerprint"`
	Setup               bool             `json:"setup"`
	Qualified           bool             `json:"qualified"`
	Reason              string           `json:"reason,omitempty"`
}

func DefaultExtensionRoot() string {
	d, e := os.UserCacheDir()
	if e != nil {
		return ""
	}
	return filepath.Join(d, "myce", "extensions")
}
func extensionFolder(root string, e Extension) string {
	return filepath.Join(root, "library", e.Name, e.Version)
}
func extensionCache(root string, e Extension, platform string) string {
	return filepath.Join(root, "cache", e.Name, e.Version, platform)
}
func extensionRecords(root string, e Extension, platform string) string {
	return filepath.Join(extensionCache(root, e, platform), "qualification.json")
}

func extensionBytecodePath(name string) bool {
	return strings.HasSuffix(name, ".pyc") || strings.Contains("/"+name, "/__pycache__/")
}

func extensionBytecodeManifest(root string, e Extension) string {
	return filepath.Join(root, "cache", e.Name, e.Version, "authored-bytecode.json")
}

func rememberAuthoredBytecode(root string, e Extension) error {
	var paths []string
	for name := range e.Files {
		if extensionBytecodePath(name) {
			paths = append(paths, name)
		}
	}
	if len(paths) == 0 {
		return nil
	}
	slices.Sort(paths)
	data, err := json.Marshal(paths)
	if err != nil {
		return err
	}
	path := extensionBytecodeManifest(root, e)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return writeFileAtomic(path, data)
}

func readInstalledExtension(root string, expected Extension) (Extension, error) {
	folder := extensionFolder(root, expected)
	var authored []string
	data, err := os.ReadFile(extensionBytecodeManifest(root, expected))
	if err == nil {
		if err := json.Unmarshal(data, &authored); err != nil {
			return Extension{}, err
		}
	} else if !os.IsNotExist(err) {
		return Extension{}, err
	}
	excluded := false
	clean, err := readExtension(folder, func(name string) bool {
		if !extensionBytecodePath(name) {
			return true
		}
		if slices.Contains(authored, name) {
			return true
		}
		if _, ok := expected.Files[name]; authored == nil && ok {
			return true
		}
		excluded = true
		return false
	})
	if !excluded || authored != nil || err == nil && clean.Digest == expected.Digest {
		return clean, err
	}

	// Older installs have no authored-path manifest. Preserve an existing
	// registered or qualified full-tree digest when it proves those files
	// were actually bundled. Subsequent scans can then ignore new bytecode.
	known := func(e Extension) bool {
		if expected.Digest != "" {
			return e.Digest == expected.Digest
		}
		for _, platform := range e.platforms() {
			for _, report := range readExtensionReports(root, expected, platform) {
				if report.Digest == e.Digest {
					return true
				}
			}
		}
		return false
	}
	if err == nil && known(clean) {
		return clean, nil
	}
	if full, fullErr := ReadExtension(folder); fullErr == nil && known(full) {
		_ = rememberAuthoredBytecode(root, full)
		return full, nil
	}
	return clean, err
}

func installedExtensionIdentity(folder string) Extension {
	return Extension{Name: filepath.Base(filepath.Dir(folder)), Version: filepath.Base(folder)}
}

func InstallExtension(root string, e Extension) error {
	checked, err := extensionFromFiles(e.Files)
	if err != nil {
		return err
	}
	if root == "" {
		return errors.New("extension root required")
	}
	dir := extensionFolder(root, checked)
	if _, err := os.Stat(dir); err == nil {
		old, err := readInstalledExtension(root, checked)
		if err != nil {
			return err
		}
		if old.Digest != checked.Digest {
			return frozenExtensionConflict(extensionKey(checked), old.Digest, checked.Digest)
		}
		return rememberAuthoredBytecode(root, checked)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(filepath.Dir(dir), ".add-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for n, b := range checked.Files {
		p := filepath.Join(tmp, n)
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(p, b, 0600); err != nil {
			return err
		}
	}
	if err := renameStoragePath(context.Background(), tmp, dir); err != nil {
		return err
	}
	return rememberAuthoredBytecode(root, checked)
}
func LocalExtensions(root string) ([]Extension, error) {
	return readLocalExtensions(root, false)
}

// Worker reports keep evidence for independently validated registrations even
// when a different version in the local cache fails to load.
func readLocalExtensions(root string, keepValid bool) ([]Extension, error) {
	return readLocalExtensionsWith(root, keepValid, func(folder string) (Extension, error) {
		return readInstalledExtension(root, installedExtensionIdentity(folder))
	})
}

func readLocalExtensionsWith(root string, keepValid bool, read func(string) (Extension, error)) ([]Extension, error) {
	out := []Extension{}
	var firstErr error
	names, err := os.ReadDir(filepath.Join(root, "library"))
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		if !n.IsDir() || strings.HasPrefix(n.Name(), ".") {
			continue
		}
		versions, err := os.ReadDir(filepath.Join(root, "library", n.Name()))
		if err != nil {
			if !keepValid {
				return nil, err
			}
			firstErr = cmp.Or(firstErr, err)
			continue
		}
		for _, v := range versions {
			if !v.IsDir() || strings.HasPrefix(v.Name(), ".") {
				continue
			}
			e, err := read(filepath.Join(root, "library", n.Name(), v.Name()))
			if err == nil && (e.Name != n.Name() || e.Version != v.Name()) {
				err = errors.New("extension folder identity mismatch")
			}
			if err != nil {
				if !keepValid {
					return nil, err
				}
				firstErr = cmp.Or(firstErr, err)
				continue
			}
			out = append(out, e)
		}
	}
	return out, firstErr
}

// Fingerprints are only hashes. Rejoining after a hardware/driver/OS change
// invalidates cached qualification. A failed identity probe fails closed.
func ExtensionFingerprint(ctx context.Context, platform, device string) (string, error) {
	return extensionFingerprintFrom(ctx, platform, device, extensionIdentityProbe)
}

func extensionFingerprintFrom(ctx context.Context, platform, device string, identity func() MachineProbe) (string, error) {
	return extensionFingerprintWithInventory(ctx, platform, device, identity, nil)
}

func extensionFingerprintWithInventory(ctx context.Context, platform, device string, identity func() MachineProbe, inventory func(context.Context) ([]MachineDevice, error)) (string, error) {
	if backendFor(platform).cpu {
		device = ""
	}
	if !validGPU(platform) {
		return "", errors.New("invalid extension platform")
	}
	// The default fake runtime has always meant its single device. Canonical
	// machine inventory calls that same simulated component "0".
	if backendFor(platform).simulated && device == "" {
		device = "0"
	}
	probe := identity()
	data := []byte(runtime.GOOS + "/" + runtime.GOARCH + "/" + platform + "/" + device)
	operation := backendFor(platform).fingerprint
	if operation == nil {
		return "", errors.New("hardware/driver identity unavailable")
	}
	data, commands, err := operation(ctx, probe, device, data, inventory)
	if err != nil {
		return "", err
	}
	for _, args := range commands {
		b, err := probe.Command(ctx, args[0], args[1:]...)
		if err != nil {
			return "", errors.New("hardware/driver identity unavailable")
		}
		data = append(data, b...)
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}
func cleanExtensionEnv(extra map[string]string) []string {
	env := jobBaseEnvironment()
	for k, v := range extra {
		if k != "MYCE_TOKEN" {
			env[k] = v
		}
	}
	out := []string{}
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	slices.Sort(out)
	return out
}
func ExtensionDevice(platform string) string {
	return backendFor(platform).extensionDevice
}
func extensionEnvironment(root string, e Extension, platform, device string) (map[string]string, error) {
	folder, err := filepath.Abs(extensionFolder(root, e))
	if err != nil {
		return nil, err
	}
	cache, err := filepath.Abs(extensionCache(root, e, platform))
	if err != nil {
		return nil, err
	}
	// Keep interpreter-generated bytecode out of the frozen library. Apply this
	// to preparation, qualification and execution, without changing content identity.
	out := map[string]string{"PYTHONDONTWRITEBYTECODE": "1", "MYCE_EXTENSION_DIR": folder, "MYCE_EXTENSION_CACHE": cache, "MYCE_DEVICE": ExtensionDevice(platform), "MYCE_PLATFORM": platform, "MYCE_DEVICES": device, "PATH": filepath.Join(cache, "venv", "bin") + string(os.PathListSeparator) + filepath.Join(cache, "bin") + string(os.PathListSeparator) + os.Getenv("PATH")}
	if device != "" || platform == "cpu" {
		model, err := WithModelDevice(Model{}, platform, device)
		if err != nil {
			return nil, err
		}
		for k, v := range model.DeviceEnv {
			out[k] = v
		}
	}
	return out, nil
}
func extensionLock(root string, e Extension, platform string, shared bool) (func(), error) {
	dir := extensionCache(root, e, platform)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "use.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	op := syscall.LOCK_EX
	if shared {
		op = syscall.LOCK_SH
	}
	if err := syscall.Flock(int(f.Fd()), op|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("extension cache in use")
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}
func writeExtensionReports(root string, e Extension, platform string, reports []ExtensionReport) error {
	if err := os.MkdirAll(extensionCache(root, e, platform), 0700); err != nil {
		return err
	}
	devices := map[string]bool{}
	for _, r := range reports {
		devices[r.Device] = true
	}
	for _, r := range readExtensionReports(root, e, platform) {
		if !devices[r.Device] {
			reports = append(reports, r)
		}
	}
	return saveWorkerJSON(extensionCache(root, e, platform), "qualification.json", reports)
}
func readExtensionReports(root string, e Extension, platform string) []ExtensionReport {
	var r []ExtensionReport
	b, err := os.ReadFile(extensionRecords(root, e, platform))
	if err == nil {
		_ = json.Unmarshal(b, &r)
	}
	return r
}
func extensionWorks(e Extension) []string {
	out := []string{}
	if len(e.Variants) > 0 {
		out = append(out, "extension:"+e.Name)
	}
	for _, k := range e.Kinds {
		out = append(out, "kind:"+k)
	}
	for _, p := range e.ProgramTypes {
		out = append(out, "program:"+p)
	}
	return out
}
func LocalExtensionReports(root, platform, fingerprint string, machines ...*Machine) []ExtensionReport {
	extensions, err := readLocalExtensions(root, true)
	return localExtensionReports(root, platform, fingerprint, extensions, err, machines...)
}

type extensionReportRegistration struct {
	extension Extension
	signature string
	bytes     int64
}

type extensionReportLibrary map[string]extensionReportRegistration

// Reuse validated report metadata only while every entry in the registration
// tree has the same inode, change time, size and mode. A full read still
// applies on changes and on filesystems without the required file metadata.
func (library *extensionReportLibrary) read(root string) ([]Extension, error) {
	next := extensionReportLibrary{}
	extensions, err := readLocalExtensionsWith(root, true, func(folder string) (Extension, error) {
		identity := installedExtensionIdentity(folder)
		signature, signatureErr := extensionLibrarySignature(root, folder, identity)
		if cached, ok := (*library)[folder]; ok && signatureErr == nil && signature == cached.signature {
			limit, err := extensionByteLimit()
			if err != nil {
				return Extension{}, err
			}
			if cached.bytes > int64(limit) {
				return Extension{}, budgetRefusal(int64(limit))
			}
			next[folder] = cached
			return cached.extension, nil
		}
		e, err := readInstalledExtension(root, identity)
		if err != nil {
			return Extension{}, err
		}
		var size int64
		for _, payload := range e.Files {
			size += int64(len(payload))
		}
		e.Files = nil // Reports use the parsed declaration and digest.
		if signatureErr == nil {
			if after, err := extensionLibrarySignature(root, folder, identity); err == nil && after == signature {
				next[folder] = extensionReportRegistration{extension: e, signature: signature, bytes: size}
			}
		}
		return e, nil
	})
	*library = next
	return extensions, err
}

func extensionLibrarySignature(root, folder string, e Extension) (string, error) {
	signature, err := sharedSignature(folder)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(extensionBytecodeManifest(root, e))
	if os.IsNotExist(err) {
		return signature, nil
	}
	if err != nil {
		return "", err
	}
	return signature + "/" + identityHash(info.Sys()), nil
}

func collectWorkerExtensionReports(root string, now time.Time, retention time.Duration, m *Machine, library *extensionReportLibrary) {
	extensions, err := library.read(root)
	if err == nil {
		removed := pruneUnusedExtensions(root, now, retention, extensions)
		if len(removed) > 0 {
			extensions = slices.DeleteFunc(extensions, func(e Extension) bool {
				if !slices.Contains(removed, extensionKey(e)) {
					return false
				}
				delete(*library, extensionFolder(root, e))
				return true
			})
		}
	}
	cpu := *m
	cpu.GPU, cpu.Device = "cpu", ""
	cpu.ExtensionFingerprint = m.CPUExtensionFingerprint
	m.ExtensionReports = localExtensionReports(root, "cpu", cpu.ExtensionFingerprint, extensions, err, &cpu)
	for i := range m.Devices {
		d := &m.Devices[i]
		target := d.machine(*m)
		d.ExtensionReports = localExtensionReports(root, target.GPU, target.ExtensionFingerprint, extensions, err, &target)
	}
	for _, d := range m.Devices {
		if d.ID == m.Device {
			m.ExtensionReports = append(m.ExtensionReports, d.ExtensionReports...)
			m.ExtensionFingerprint = d.ExtensionFingerprint
		}
	}
	removeVanishedExtensionReports(root, m, extensions, os.Lstat)
}

func removeVanishedExtensionReports(root string, m *Machine, extensions []Extension, lstat func(string) (os.FileInfo, error)) {
	// Qualification receipts can outlive a registration removed after the
	// shared scan. Recheck each manifest once before advertising any target.
	// An inconclusive stat keeps the scanned report. A removal after the stat
	// can appear in this heartbeat; launch revalidates content and qualification.
	var missing map[string]bool
	for _, e := range extensions {
		info, err := lstat(filepath.Join(extensionFolder(root, e), "extension.yaml"))
		if err == nil && !info.Mode().IsRegular() || errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			if missing == nil {
				missing = map[string]bool{}
			}
			missing[extensionKey(e)] = true
		}
	}
	if len(missing) == 0 {
		return
	}
	vanished := func(r ExtensionReport) bool {
		return missing[extensionKey(Extension{Name: r.Name, Version: r.Version})]
	}
	m.ExtensionReports = slices.DeleteFunc(m.ExtensionReports, vanished)
	for i := range m.Devices {
		d := &m.Devices[i]
		d.ExtensionReports = slices.DeleteFunc(d.ExtensionReports, vanished)
	}
}

func localExtensionReports(root, platform, fingerprint string, extensions []Extension, err error, machines ...*Machine) []ExtensionReport {
	out := []ExtensionReport{}
	if err != nil {
		reason := extensionLibraryPartialReason
		if len(extensions) == 0 {
			reason = extensionLibraryUnreadableReason
		}
		out = append(out, ExtensionReport{Platform: platform, Reason: reason})
	}
	for _, e := range extensions {
		if len(e.Variants) == 0 && !slices.Contains(e.platforms(), platform) {
			continue
		}
		records := readExtensionReports(root, e, platform)
		for _, work := range extensionWorks(e) {
			r := ExtensionReport{Name: e.Name, Version: e.Version, Digest: e.Digest, Platform: platform, Work: work, Reason: extensionUncheckedReason}
			if len(e.Variants) > 0 {
				t := ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: platform}
				r.Target = &t
				if reason := e.targetUnavailableReason(t); reason != "" {
					r.Reason = reason
					out = append(out, r)
					continue
				}
			}
			found, changedExtension := false, false
			for _, old := range records {
				if old.Work != work || len(machines) > 0 && extensionReportDevice(platform, old.Device) != extensionReportDevice(platform, machines[0].Device) {
					continue
				}
				if old.Digest != e.Digest {
					changedExtension = true
					continue
				}
				r, found = old, true
				break
			}
			if !found {
				if changedExtension {
					r.Reason = extensionContentChangedReason
				}
			} else if fingerprint == "" || r.Fingerprint == "" || r.Fingerprint != fingerprint {
				r.Setup = false
				r.Qualified = false
				r.Reason = extensionDriverChangedReason
				if fingerprint == "" || r.Fingerprint == "" {
					r.Reason = extensionFingerprintMissingReason
				}
			}
			if len(e.Variants) > 0 && r.Qualified {
				local := Machine{HostID: r.Host, GPU: platform, ExtensionFingerprint: fingerprint, CapabilityProfile: &CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH}}
				if len(machines) > 0 {
					local = *machines[0]
				}
				if reason := extensionQualificationReason(e, r, &local); reason != "" {
					r.Qualified = false
					r.Setup = false
					r.Reason = reason
				}
			}
			out = append(out, r)
		}
	}
	return out
}
func runExtensionCommand(ctx context.Context, args []string, dir string, env map[string]string, log io.Writer, scopes ...commandScope) error {
	if len(args) == 0 {
		return nil
	}
	command := args[0]
	for _, dir := range filepath.SplitList(env["PATH"]) {
		if strings.Contains(command, "/") {
			break
		}
		candidate := filepath.Join(dir, command)
		if st, e := os.Stat(candidate); e == nil && st.Mode().IsRegular() && st.Mode()&0111 != 0 {
			command = candidate
			break
		}
	}
	scope := commandScope{StateDir: filepath.Join(dir, ".myce-commands")}
	if len(scopes) > 0 {
		scope = scopes[0]
	}
	if err := scope.prepare(); err != nil {
		return err
	}
	argv := append([]string{command}, args[1:]...)
	return runRecordedCommand(ctx, argv, dir, cleanExtensionEnv(env), log, scope, "attempt-command-"+ID()+".json")
}

// Setup/check are explicit local operations, never background services. Their
// detailed logs stay local; only fixed reasons enter reports/status.
func PrepareExtension(ctx context.Context, root string, e Extension, platform, device string, check bool, log io.Writer, caches ...*EnvironmentCache) ([]ExtensionReport, error) {
	host, _ := os.Hostname()
	local := Machine{HostID: host, GPU: platform, Device: device, CapabilityProfile: &CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH}}
	if reason := e.targetUnavailableReason(machineTarget(&local)); reason != "" {
		return nil, errors.New(reason)
	}
	if !slices.Contains(e.platforms(), platform) {
		return nil, errors.New("extension does not support this platform; no CPU fallback")
	}
	actual, err := readInstalledExtension(root, e)
	if err != nil {
		return nil, err
	}
	if actual.Digest != e.Digest {
		return nil, errors.New("extension content changed")
	}
	unlock, err := extensionLock(root, e, platform, false)
	if err != nil {
		return nil, err
	}
	defer unlock()
	fingerprint, err := ExtensionFingerprint(ctx, platform, device)
	if err != nil {
		reports := []ExtensionReport{}
		for _, work := range extensionWorks(e) {
			reports = append(reports, ExtensionReport{Name: e.Name, Version: e.Version, Digest: e.Digest, Platform: platform, Work: work, Reason: "hardware/driver identity unavailable"})
		}
		return reports, errors.Join(err, writeExtensionReports(root, e, platform, reports))
	}
	env, err := extensionEnvironment(root, e, platform, device)
	if err != nil {
		return nil, err
	}
	local.ExtensionFingerprint = fingerprint
	view, ok := e.forMachine(&local)
	if !ok {
		return nil, errors.New("extension target unsupported; native groups require D4")
	}
	e = view
	dir := env["MYCE_EXTENSION_DIR"]
	old := readExtensionReports(root, e, platform)
	reports := []ExtensionReport{}
	for _, work := range extensionWorks(e) {
		r := newExtensionReport(e, &local, work)
		if check {
			r.CheckID = ID()
			for _, o := range old {
				if o.Work == work && o.Digest == e.Digest && o.Fingerprint == fingerprint {
					r.Setup = o.Setup
				}
			}
		}
		reports = append(reports, r)
	}
	// Clear qualified before any command, including a repeat that later fails.
	if err := writeExtensionReports(root, e, platform, reports); err != nil {
		return nil, err
	}
	if r, ok := extensionRecipe(e, &local); ok {
		if len(caches) == 0 || caches[0] == nil {
			return nil, errors.New("variant environments require an explicit environment cache")
		}
		envRoot, release, err := caches[0].Acquire(ctx, r)
		if err != nil {
			return nil, err
		}
		defer release()
		bindEnvironment(env, envRoot)
		ref := e.variant(machineTarget(&local)).Execution.Main.Environment
		expand := executionReplacer(dir, envRoot, ref, machineTarget(&local))
		e.Setup = expandArgs(e.Setup, expand)
		e.Check = expandArgs(e.Check, expand)
	}
	replacePaths := strings.NewReplacer("{extension}", dir, "{cache}", env["MYCE_EXTENSION_CACHE"])
	e.Setup = expandArgs(e.Setup, replacePaths)
	e.Check = expandArgs(e.Check, replacePaths)
	if !check {
		err = runExtensionCommand(ctx, e.Setup, dir, env, log)
		for i := range reports {
			reports[i].Setup = err == nil
			reports[i].Reason = "check required"
			if err != nil {
				reports[i].Reason = "setup failed"
			}
		}
	} else {
		for i := range reports {
			r := &reports[i]
			if !r.Setup {
				r.Reason = "setup required"
				err = errors.Join(err, errors.New(r.Reason))
				continue
			}
			b, ok := e.baseline(r.Work, platform)
			if !ok {
				r.Reason = "no native baseline"
				err = errors.Join(err, errors.New(r.Reason))
				continue
			}
			tmp, e2 := os.MkdirTemp(env["MYCE_EXTENSION_CACHE"], "check-")
			if e2 != nil {
				return reports, e2
			}
			env["MYCE_CHECK_INPUT"] = filepath.Join(dir, b.Input)
			env["MYCE_CHECK_OUTPUT"] = filepath.Join(tmp, "actual")
			env["MYCE_CHECK_WORK"] = r.Work
			e2 = runExtensionCommand(ctx, e.Check, dir, env, log)
			if e2 != nil {
				r.Reason = "check command failed"
			} else {
				limit, e3 := extensionByteLimit()
				if e3 != nil {
					_ = os.RemoveAll(tmp)
					return reports, e3
				}
				output, e3 := readCapacityFile(env["MYCE_CHECK_OUTPUT"], int64(limit))
				if e3 != nil || len(output) > limit {
					r.Reason = "check output missing or too large"
				} else if e3 = compareExtensionOutput(output, e.Files[b.Output], b); e3 != nil {
					r.Reason = e3.Error()
				} else {
					r.Qualified = true
					r.Reason = extensionQualificationReason(e, *r, &local)
					r.Qualified = r.Reason == ""
				}
			}
			_ = os.RemoveAll(tmp) // Only this invocation's reproducible check output.
			if !r.Qualified {
				err = errors.Join(err, errors.New(r.Reason), e2)
			}
		}
	}
	return reports, errors.Join(err, writeExtensionReports(root, e, platform, reports))
}

// prepareExtensionExecution only expands a qualified plain command. The
// existing worker still owns memory admission, start gates, signals and resume.
func (w Worker) prepareExtensionExecution(j *Job) (envOut map[string]string, releaseOut func(), errOut error) {
	defer func() {
		if errOut != nil {
			errOut = fmt.Errorf("%w: %w", errExtensionUnavailable, errOut)
		}
	}()
	if !j.Spec.extensionJob() {
		return nil, nil, nil
	}
	e := qualifiedExtension(j, &w.Machine)
	if e == nil || j.Extension == nil || j.Extension.Digest != e.Digest {
		return nil, nil, errors.New("extension is not qualified for this attempt")
	}
	actual, err := readInstalledExtension(w.ExtensionRoot, *e)
	if err != nil {
		return nil, nil, err
	}
	if actual.Digest != e.Digest {
		return nil, nil, errors.New("frozen extension content changed")
	}
	unlock, err := extensionLock(w.ExtensionRoot, *e, w.Machine.GPU, true)
	if err != nil {
		return nil, nil, err
	}
	touchExtensionUse(w.ExtensionRoot, *e, workerNow(w.Clock))
	reports := LocalExtensionReports(w.ExtensionRoot, w.Machine.GPU, w.extensionFingerprint, &w.Machine)
	m := w.Machine
	m.ExtensionReports = reports
	if qualifiedExtension(j, &m) == nil {
		unlock()
		return nil, nil, errors.New("extension qualification changed")
	}
	env, err := extensionEnvironment(w.ExtensionRoot, *e, w.Machine.GPU, w.Machine.Device)
	if err == nil {
		var controls map[string]string
		controls, err = w.deviceEnvironment()
		for k, v := range controls {
			env[k] = v
		}
	}
	if err != nil {
		unlock()
		return nil, nil, err
	}
	if err := e.lower(&j.Spec, &w.Machine); err != nil {
		unlock()
		return nil, nil, err
	}
	if err := validateEphemeralExecution(j.Spec); err != nil {
		unlock()
		return nil, nil, reject("job_worker_data_unsupported", err)
	}
	if check := j.Spec.ExtensionCheck; check != nil {
		if w.environmentCache == nil {
			unlock()
			return nil, nil, errors.New("extension check requires prepared environments")
		}
		// Use worker-owned stable prefixes. The CLI never builds an environment
		// or launches setup, and its cache argument cannot redirect preparation.
		if v := e.variant(machineTarget(&w.Machine)); v != nil {
			for name, recipe := range v.Environments {
				expandExecutionReference(&j.Spec, executionReplacer(check.Directory, filepath.Join(w.environmentCache.Root, recipe.Key), name, machineTarget(&w.Machine)))
			}
		}
		expandExecutionReference(&j.Spec, executionReplacer(check.Directory, "", "", machineTarget(&w.Machine)))
		view, _ := e.forMachine(&w.Machine)
		if b, ok := view.baseline(check.Work, w.Machine.GPU); ok {
			env["MYCE_CHECK_INPUT"] = filepath.Join(env["MYCE_EXTENSION_DIR"], b.Input)
			env["MYCE_CHECK_OUTPUT"] = filepath.Join(check.Directory, "actual")
			env["MYCE_CHECK_WORK"] = check.Work
		}
	}
	if r, ok := extensionRecipe(*e, &w.Machine); ok {
		j.EnvironmentRecipes = map[string]EnvironmentRecipe{w.Machine.GPU: r}
	}
	argv := slices.Clone(j.Spec.Run.Command)
	for i, arg := range argv {
		arg = strings.ReplaceAll(arg, "{extension}", env["MYCE_EXTENSION_DIR"])
		arg = strings.ReplaceAll(arg, "{cache}", env["MYCE_EXTENSION_CACHE"])
		argv[i] = arg
	}
	if j.Spec.Program != nil {
		argv = append(argv, j.Spec.Program.File)
		argv = append(argv, j.Spec.Program.Args...)
	} else {
		argv = append(argv, j.Spec.Args...)
	}
	if len(e.Variants) == 0 && !strings.Contains(argv[0], "/") {
		for _, bin := range []string{filepath.Join(env["MYCE_EXTENSION_CACHE"], "venv", "bin"), filepath.Join(env["MYCE_EXTENSION_CACHE"], "bin")} {
			candidate := filepath.Join(bin, argv[0])
			if st, err := os.Stat(candidate); err == nil && st.Mode().IsRegular() && st.Mode()&0111 != 0 {
				argv[0] = candidate
				break
			}
		}
	}
	j.Spec.Run.Command = argv
	expandNativeExtension(&j.Spec, env)
	for k, v := range j.Spec.Run.Env {
		j.Spec.Run.Env[k] = strings.ReplaceAll(strings.ReplaceAll(v, "{extension}", env["MYCE_EXTENSION_DIR"]), "{cache}", env["MYCE_EXTENSION_CACHE"])
	}
	j.Spec.Run.Workdir = strings.ReplaceAll(strings.ReplaceAll(j.Spec.Run.Workdir, "{extension}", env["MYCE_EXTENSION_DIR"]), "{cache}", env["MYCE_EXTENSION_CACHE"])
	if len(e.Assets) > 0 {
		key := extensionPreparationKey(*e, &w.Machine)
		if w.environmentCache == nil || !w.environmentCache.ready(key) {
			unlock()
			return nil, nil, errors.New("extension assets are not prepared")
		}
		if j.ExecutionEvidence != nil {
			j.ExecutionEvidence.EnvironmentPreparation = executionMayHaveRun
		}
		root, release, err := w.environmentCache.Acquire(withLaunchGate(w.launchContext(), func(ctx context.Context) error {
			return w.authorizeLaunchContext(ctx, HostLaunch{Machine: w.Machine.ID, Session: w.Machine.Session, Kind: "batch", Key: j.ID, Attempt: fmt.Sprint(j.Attempt)})
		}), EnvironmentRecipe{Key: key, readyOnly: true})
		if err != nil {
			unlock()
			return nil, nil, err
		}
		prior := unlock
		unlock = func() { release(); prior() }
		r := extensionAssetReplacer(*e, root)
		j.Spec.Run.Command = expandArgs(j.Spec.Run.Command, r)
		j.Spec.Run.Workdir = r.Replace(j.Spec.Run.Workdir)
		for k, v := range j.Spec.Run.Env {
			j.Spec.Run.Env[k] = r.Replace(v)
		}
		for i := range j.Spec.Run.Sidecars {
			side := &j.Spec.Run.Sidecars[i]
			side.Command = expandArgs(side.Command, r)
			for k, v := range side.Env {
				side.Env[k] = r.Replace(v)
			}
		}
	}
	env["MYCE_WORK"] = j.Spec.workType()
	env["MYCE_EXTENSION_NAME"] = e.Name
	env["MYCE_EXTENSION_VERSION"] = e.Version
	return env, unlock, nil
}

// Keep this formatted identity useful for local CLI output without machine paths.
func FormatExtensionReport(r ExtensionReport) string {
	state := "not qualified"
	if r.Qualified {
		state = "qualified"
	}
	return fmt.Sprintf("%s@%s %s %s: %s %s", r.Name, r.Version, r.Platform, r.Work, state, r.Reason)
}

// Live MHz, topology order and idle counters are not CPU identity. Hash only
// stable implementation/feature fields, deduplicated across logical CPUs.
func cpuIdentity(facts []byte) string {
	fields := map[string]bool{}
	for _, line := range strings.Split(string(facts), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "vendor_id", "model name", "cpu family", "model", "stepping", "flags", "hardware", "cpu implementer", "cpu architecture", "cpu variant", "cpu part", "cpu revision", "features":
			fields[strings.TrimSpace(key)+":"+strings.TrimSpace(value)] = true
		}
	}
	values := make([]string, 0, len(fields))
	for value := range fields {
		values = append(values, value)
	}
	slices.Sort(values)
	return strings.Join(values, "\n")
}

var extensionIdentityProbe = localExtensionIdentityProbe

func localExtensionIdentityProbe() MachineProbe {
	p := LocalMachineProbe()
	p.Info = nil
	return p
}
