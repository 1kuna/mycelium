package myce

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Fleet files deliberately accept only literal values, never shell expansion.
func ReadFleetEnvironment(path string) (token, queue string, err error) {
	st, err := os.Stat(path)
	if err != nil {
		return "", "", err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return "", "", errors.New("fleet EnvironmentFile must be a private regular file (0600)")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		line := strings.TrimSpace(scan.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || (key != "MYCE_TOKEN" && key != "MYCE_QUEUE") {
			return "", "", errors.New("fleet EnvironmentFile accepts only MYCE_TOKEN and MYCE_QUEUE literal assignments")
		}
		value = strings.TrimSpace(value)
		if strings.HasPrefix(value, "'") {
			if len(value) < 2 || !strings.HasSuffix(value, "'") || strings.Contains(value[1:len(value)-1], "'") {
				return "", "", errors.New("invalid fleet EnvironmentFile quoting")
			}
			value = value[1 : len(value)-1]
		} else if strings.HasPrefix(value, `"`) {
			value, err = strconv.Unquote(value)
			if err != nil {
				return "", "", errors.New("invalid fleet EnvironmentFile quoting")
			}
		} else if strings.ContainsAny(value, " \t'\"\\") {
			return "", "", errors.New("quote fleet EnvironmentFile values containing spaces")
		}
		if value == "" || strings.ContainsAny(value, "\x00\n\r") {
			return "", "", errors.New("fleet EnvironmentFile values must be nonempty single-line literals")
		}
		if key == "MYCE_TOKEN" {
			token = value
		} else {
			queue = value
		}
	}
	if err = scan.Err(); err != nil {
		return "", "", errors.New("cannot read fleet EnvironmentFile")
	}
	if token == "" || queue == "" {
		return "", "", errors.New("fleet EnvironmentFile requires MYCE_TOKEN and MYCE_QUEUE")
	}
	u, err := url.Parse(queue)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", errors.New("MYCE_QUEUE must be an HTTP URL without credentials, query or fragment")
	}
	return token, queue, nil
}

func prepareInstallNAS(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	if c.RequireMountedNAS {
		if c.Detection.OS != "linux" {
			return errors.New("--require-mounted-nas requires Linux /proc/mounts")
		}
		mounts, err := command(ctx, "cat", "/proc/mounts")
		if err != nil {
			return fmt.Errorf("inspect NAS mount: %w", err)
		}
		mounted := false
		for _, line := range strings.Split(string(mounts), "\n") {
			f := strings.Fields(line)
			if len(f) < 4 {
				continue
			}
			path := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\134`, `\`).Replace(f[1])
			if path == filepath.Clean(c.NAS) && path != "/" {
				mounted = true
			}
		}
		if !mounted {
			return errors.New("NAS root is not a mounted filesystem in /proc/mounts; refusing local fallback")
		}
	}
	// Refuse symlinks so creation and probes cannot escape the Mycelium namespace.
	for _, dir := range []string{c.NAS, filepath.Join(c.NAS, "mycelium"), filepath.Join(c.NAS, "mycelium", "checkpoints"), filepath.Join(c.NAS, "mycelium", "models"), filepath.Join(c.NAS, "mycelium", "environments")} {
		st, err := os.Lstat(dir)
		if errors.Is(err, os.ErrNotExist) {
			if err = os.Mkdir(dir, 0700); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("NAS namespace must contain real directories, not symlinks")
		}
	}
	for _, name := range []string{"checkpoints", "models", "environments"} {
		dir := filepath.Join(c.NAS, "mycelium", name)
		f, err := os.CreateTemp(dir, ".myce-install-probe-")
		if err != nil {
			return err
		}
		path := f.Name()
		renamed := path + ".renamed"
		_, writeErr := f.WriteString("myce NAS write and rename probe\n")
		err = errors.Join(writeErr, f.Sync(), f.Close())
		if err != nil {
			_ = os.Remove(path)
			return err
		}
		if err = os.Rename(path, renamed); err != nil {
			_ = os.Remove(path)
			return err
		}
		if err = os.Remove(renamed); err != nil {
			return err
		}
	}
	return nil
}

func installConfigDiff(old, new ServiceConfig) string {
	old.Force, new.Force = false, false
	a, _ := json.Marshal(old)
	b, _ := json.Marshal(new)
	var before, after map[string]json.RawMessage
	json.Unmarshal(a, &before)
	json.Unmarshal(b, &after)
	keys := map[string]bool{}
	for key := range before {
		keys[key] = true
	}
	for key := range after {
		keys[key] = true
	}
	fields := make([]string, 0, len(keys))
	for key := range keys {
		fields = append(fields, key)
	}
	sort.Strings(fields)
	out := []string{}
	for _, key := range fields {
		if string(before[key]) != string(after[key]) {
			out = append(out, fmt.Sprintf("%s: %s -> %s", key, before[key], after[key]))
		}
	}
	return strings.Join(out, "\n")
}

func installDisabled(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error), installed bool) error {
	unit := filepath.Base(c.Service)
	dir, err := WorkerUnitDirectory(c.HostStateDir, unit)
	if err != nil {
		return err
	}
	if _, e := os.Stat(filepath.Join(dir, hostStateFile)); errors.Is(e, os.ErrNotExist) {
		if _, e = InitializeHostIntent(dir, c.HostID, RealClock{}); e != nil {
			return e
		}
	}
	w := HostWorker{Config: c.Config, Unit: unit, State: c.State, Host: c.HostID, Name: c.Name, Queue: c.Queue, Environments: c.EnvironmentCache}
	if err = AdoptHostWorker(c.HostStateDir, w); err != nil {
		return err
	}
	if err = AdoptHostWorker(dir, w); err != nil {
		return err
	}
	h := HostControl{Dir: dir, Docker: true}
	if installed {
		h.Services = map[string]HostService{unit: &SystemdHostService{Unit: unit, Command: command}}
	}
	// Installation preserves the whole-host revision and its original stop deadline.
	_, err = h.Disable(ctx, "installed with --start-disabled", 0)
	return err
}

func replaceInstallBinary(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".myce-binary-")
	if err != nil {
		return err
	}
	if err = f.Chmod(0700); err != nil {
		f.Close()
		return err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncWorkerDir(filepath.Dir(path))
}

func (c ServiceConfig) ValidateInstall() error {
	if c.Role != "queue" && c.Role != "worker" {
		return errors.New("install requires worker or queue")
	}
	for _, p := range []string{c.JoiningFile, c.NAS, c.State, c.Checkpoints, c.ModelRoot, c.EnvironmentRoot, c.ModelCache, c.EnvironmentCache, c.Config, c.Binary, c.Service, c.Credentials, c.HostStateDir, c.DB, c.UploadSpoolDir, c.ProjectConfig, c.HostConfig, c.ModelCatalog, c.EnvironmentCatalog, c.EnvironmentFile} {
		if p != "" && (!filepath.IsAbs(p) || strings.ContainsAny(p, "\x00\n\r")) {
			return errors.New("installed paths must be absolute and single-line")
		}
	}
	if c.Detection.OS == "linux" {
		// These paths occur in directives that do not support ExecStart quoting.
		paths := []string{c.EnvironmentFile}
		if c.Role == "worker" {
			paths = append(paths, c.GPUEnvironmentFile(), c.HostStateDir)
		}
		for _, p := range paths {
			if strings.ContainsAny(p, "'\"\\") || strings.IndexFunc(p, unicode.IsSpace) >= 0 {
				return errors.New("systemd directive paths cannot contain whitespace, quotes or backslashes (EnvironmentFile and ConditionPathExists)")
			}
		}
		if c.Role == "queue" && c.PrivateDevices {
			return errors.New("PrivateDevices is unsupported for portable systemd user queues; use --private-devices=false (queue checks for inherited GPU descriptors)")
		}
	}
	if strings.ContainsAny(c.Path, "\x00\n\r\"\\") {
		return errors.New("PATH cannot contain newlines, quotes or backslashes for service capture")
	}
	if c.ContainerRetention != nil && *c.ContainerRetention < 0 {
		return errors.New("container-retention must be nonnegative")
	}
	if c.Heartbeat <= 0 || c.LogRetention <= 0 || c.RestartGrace < 0 || c.ProductionLossTimeout <= 0 || c.DeadlineLossTimeout <= 0 || c.ExperimentLossTimeout <= 0 {
		return errors.New("heartbeat, retention and loss timeouts must be positive; restart grace must be nonnegative")
	}
	if !validMemoryValue(c.ReserveHostMemoryGB) || c.ModelCacheBytes <= 0 || c.EnvironmentCacheBytes <= 0 || c.UploadSpoolBytes <= 0 {
		return errors.New("reserve and cache/spool limits must be finite and nonnegative, with positive byte limits")
	}
	if _, err := MemoryCeilingConfig(c.HostMemoryBudgetGB, c.MemoryCeilingSlackGB, c.MemorySwapMaxBytes); err != nil {
		return err
	}
	if c.HostID == "" || strings.ContainsAny(c.HostID, "\x00\n\r") || c.Name == "" || strings.ContainsAny(c.Name, "\x00\n\r") {
		return errors.New("host ID and worker name must be nonempty single-line values")
	}
	if c.LocalDisableReceipts && (c.Role != "queue" || c.HostID == "" || c.HostStateDir == "") {
		return errors.New("local disable receipts require queue, host ID and host-state directory")
	}
	if c.StartDisabled && (c.Role != "worker" || c.Detection.OS != "linux") {
		return errors.New("--start-disabled requires a Linux worker")
	}
	if c.RequireMemoryCeilings && c.Role != "worker" {
		return errors.New("--require-memory-ceilings requires a worker")
	}
	if c.Nice < -20 || c.Nice > 19 {
		return errors.New("nice must be between -20 and 19")
	}
	quota, err := strconv.ParseFloat(strings.TrimSuffix(c.CPUQuota, "%"), 64)
	if err != nil || !strings.HasSuffix(c.CPUQuota, "%") || quota <= 0 || math.IsNaN(quota) || math.IsInf(quota, 0) {
		return errors.New("cpu-quota must be a positive percentage")
	}
	for _, limit := range []struct{ name, value string }{{"memory-max", c.MemoryMax}, {"tasks-max", c.TasksMax}} {
		if limit.value == "infinity" {
			continue
		}
		value := limit.value
		if limit.name == "memory-max" {
			value = strings.TrimRight(value, "KMGTPE")
		}
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil || n == 0 || (limit.name == "memory-max" && len(limit.value)-len(value) > 1) {
			return fmt.Errorf("%s must be positive bytes with an optional K/M/G/T/P/E suffix, a positive task count, or infinity", limit.name)
		}
	}
	return nil
}
