package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
)

type ExtensionReport struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Digest      string `json:"digest"`
	Platform    string `json:"platform"`
	Work        string `json:"work"`
	Fingerprint string `json:"fingerprint"`
	Setup       bool   `json:"setup"`
	Qualified   bool   `json:"qualified"`
	Reason      string `json:"reason,omitempty"`
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
		old, err := ReadExtension(dir)
		if err != nil {
			return err
		}
		if old.Digest != checked.Digest {
			return errors.New("extension version is frozen; publish a new version")
		}
		return nil
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
	if err := os.Rename(tmp, dir); err != nil {
		return err
	}
	return nil
}
func LocalExtensions(root string) ([]Extension, error) {
	out := []Extension{}
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
			return nil, err
		}
		for _, v := range versions {
			if !v.IsDir() || strings.HasPrefix(v.Name(), ".") {
				continue
			}
			e, err := ReadExtension(filepath.Join(root, "library", n.Name(), v.Name()))
			if err != nil {
				return nil, err
			}
			if e.Name != n.Name() || e.Version != v.Name() {
				return nil, errors.New("extension folder identity mismatch")
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// Fingerprints are only hashes. Rejoining after a hardware/driver/OS change
// invalidates cached qualification. A failed identity probe fails closed.
func ExtensionFingerprint(ctx context.Context, platform, device string) (string, error) {
	if !validGPU(platform) {
		return "", errors.New("invalid extension platform")
	}
	data := []byte(runtime.GOOS + "/" + runtime.GOARCH + "/" + platform + "/" + device)
	commands := [][]string{}
	switch platform {
	case "fake":
		data = append(data, []byte("/fake-v1")...)
	case "metal":
		commands = [][]string{{"sw_vers", "-productVersion"}, {"system_profiler", "-json", "SPHardwareDataType", "SPDisplaysDataType"}}
	case "cuda":
		commands = [][]string{{"nvidia-smi", "--query-gpu=uuid,name,driver_version", "--format=csv,noheader"}, {"uname", "-r"}}
	case "xpu":
		commands = [][]string{{"sycl-ls"}, {"uname", "-r"}}
	}
	for _, args := range commands {
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Env = cleanExtensionEnv(nil)
		b, err := cmd.Output()
		if err != nil {
			return "", errors.New("hardware/driver identity unavailable")
		}
		data = append(data, b...)
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}
func cleanExtensionEnv(extra map[string]string) []string {
	env := map[string]string{}
	for _, entry := range os.Environ() {
		k, v, ok := strings.Cut(entry, "=")
		if ok && k != "MYCE_TOKEN" {
			env[k] = v
		}
	}
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
	switch platform {
	case "metal":
		return "mps"
	case "xpu":
		return "xpu:0"
	case "cuda":
		return "cuda:0"
	case "fake":
		return "cpu"
	}
	return ""
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
	out := map[string]string{"MYCE_EXTENSION_DIR": folder, "MYCE_EXTENSION_CACHE": cache, "MYCE_DEVICE": ExtensionDevice(platform), "MYCE_PLATFORM": platform, "MYCE_DEVICES": device, "PATH": filepath.Join(cache, "venv", "bin") + string(os.PathListSeparator) + filepath.Join(cache, "bin") + string(os.PathListSeparator) + os.Getenv("PATH")}
	if device != "" {
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
	for _, k := range e.Kinds {
		out = append(out, "kind:"+k)
	}
	for _, p := range e.ProgramTypes {
		out = append(out, "program:"+p)
	}
	return out
}
func LocalExtensionReports(root, platform, fingerprint string) []ExtensionReport {
	extensions, err := LocalExtensions(root)
	if err != nil {
		return []ExtensionReport{{Platform: platform, Reason: "extension library unreadable"}}
	}
	out := []ExtensionReport{}
	for _, e := range extensions {
		if !slices.Contains(e.Platforms, platform) {
			continue
		}
		records := readExtensionReports(root, e, platform)
		for _, work := range extensionWorks(e) {
			r := ExtensionReport{Name: e.Name, Version: e.Version, Digest: e.Digest, Platform: platform, Work: work, Reason: "setup and check required"}
			for _, old := range records {
				if old.Work == work && old.Digest == e.Digest {
					r = old
					break
				}
			}
			if fingerprint == "" || r.Fingerprint != fingerprint {
				r.Setup = false
				r.Qualified = false
				r.Reason = "hardware/driver identity changed or unavailable; setup and check required"
			}
			out = append(out, r)
		}
	}
	return out
}
func runExtensionCommand(ctx context.Context, args []string, dir string, env map[string]string, log io.Writer) error {
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
	cmd := exec.Command(command, args[1:]...)
	cmd.Dir = dir
	cmd.Env = cleanExtensionEnv(env)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		return err
	case <-ctx.Done():
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
		return ctx.Err()
	}
}

// Setup/check are explicit local operations, never background services. Their
// detailed logs stay local; only fixed reasons enter reports/status.
func PrepareExtension(ctx context.Context, root string, e Extension, platform, device string, check bool, log io.Writer) ([]ExtensionReport, error) {
	if !slices.Contains(e.Platforms, platform) {
		return nil, errors.New("extension does not support this platform; no CPU fallback")
	}
	actual, err := ReadExtension(extensionFolder(root, e))
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
	dir := env["MYCE_EXTENSION_DIR"]
	old := readExtensionReports(root, e, platform)
	reports := []ExtensionReport{}
	for _, work := range extensionWorks(e) {
		r := ExtensionReport{Name: e.Name, Version: e.Version, Digest: e.Digest, Platform: platform, Work: work, Fingerprint: fingerprint, Reason: "setup required"}
		if check {
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
				err = errors.New(r.Reason)
				continue
			}
			b, ok := e.Baselines[r.Work]
			if !ok {
				r.Reason = "no native baseline"
				err = errors.New(r.Reason)
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
				output, e3 := os.ReadFile(env["MYCE_CHECK_OUTPUT"])
				if e3 != nil || len(output) > extensionMaxBytes {
					r.Reason = "check output missing or too large"
				} else if e3 = compareExtensionOutput(output, e.Files[b.Output], b); e3 != nil {
					r.Reason = e3.Error()
				} else {
					r.Qualified = true
					r.Reason = ""
				}
			}
			_ = os.RemoveAll(tmp) // Only this invocation's reproducible check output.
			if !r.Qualified {
				err = errors.New(r.Reason)
			}
		}
	}
	return reports, errors.Join(err, writeExtensionReports(root, e, platform, reports))
}

// prepareExtensionExecution only expands a qualified plain command. The
// existing worker still owns memory admission, start gates, signals and resume.
func (w Worker) prepareExtensionExecution(j *Job) (map[string]string, func(), error) {
	if !j.Spec.extensionJob() {
		return nil, nil, nil
	}
	e := qualifiedExtension(j, &w.Machine)
	if e == nil || j.Extension == nil || j.Extension.Digest != e.Digest {
		return nil, nil, errors.New("extension is not qualified for this attempt")
	}
	actual, err := ReadExtension(extensionFolder(w.ExtensionRoot, *e))
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
	reports := LocalExtensionReports(w.ExtensionRoot, w.Machine.GPU, w.extensionFingerprint)
	m := w.Machine
	m.ExtensionReports = reports
	if qualifiedExtension(j, &m) == nil {
		unlock()
		return nil, nil, errors.New("extension qualification changed")
	}
	env, err := extensionEnvironment(w.ExtensionRoot, *e, w.Machine.GPU, w.Machine.Device)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	argv := slices.Clone(e.Run)
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
	if !strings.Contains(argv[0], "/") {
		for _, bin := range []string{filepath.Join(env["MYCE_EXTENSION_CACHE"], "venv", "bin"), filepath.Join(env["MYCE_EXTENSION_CACHE"], "bin")} {
			candidate := filepath.Join(bin, argv[0])
			if st, err := os.Stat(candidate); err == nil && st.Mode().IsRegular() && st.Mode()&0111 != 0 {
				argv[0] = candidate
				break
			}
		}
	}
	j.Spec.Run.Command = argv
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
