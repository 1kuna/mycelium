package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type ServiceSessionReport struct{ Manager, Lingering, Limitation, MemoryEnforcement string }

// EnsureOnboardingServiceSession performs the narrow user lingering action,
// never sudo. On macOS it verifies the actual GUI domain before any install.
func EnsureOnboardingServiceSession(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) (ServiceSessionReport, error) {
	report := ServiceSessionReport{MemoryEnforcement: "admission-only until Linux cgroup delegation is verified"}
	if command == nil {
		return report, preflightError("service_session_unavailable", "service", "service manager runner missing")
	}
	uid := strconv.Itoa(os.Getuid())
	if c.RequireMemoryCeilings && c.Detection.OS != "linux" {
		return report, preflightError("memory_ceilings_unsupported", "service", "required memory ceilings are unsupported on macOS; RAM and CPU budgets are admission-only")
	}
	switch c.Detection.OS {
	case "linux":
		report.Manager = "systemd user"
		b, err := command(ctx, "loginctl", "show-user", uid, "--property=Linger", "--value")
		if err != nil {
			return report, preflightError("lingering_unavailable", "service", "cannot inspect lingering; administrator must enable it for this user")
		}
		switch strings.TrimSpace(string(b)) {
		case "yes":
			report.Lingering = "already enabled"
		case "no":
			if _, err = command(ctx, "loginctl", "enable-linger", uid); err != nil {
				return report, preflightError("lingering_permission_required", "service", "could not enable lingering; administrator must run loginctl enable-linger for this user")
			}
			b, err = command(ctx, "loginctl", "show-user", uid, "--property=Linger", "--value")
			if err != nil || strings.TrimSpace(string(b)) != "yes" {
				return report, preflightError("lingering_unavailable", "service", "lingering action did not verify; administrator must enable it for this user")
			}
			report.Lingering = "enabled and verified"
		default:
			return report, preflightError("lingering_unavailable", "service", "unknown lingering state; administrator must inspect loginctl")
		}
		if _, err = command(ctx, "systemctl", "--user", "show-environment"); err != nil {
			return report, preflightError("service_session_unavailable", "service", "systemd user manager is unavailable; log in as the service user")
		}
	case "darwin":
		report.Manager = "launchd gui/" + uid
		report.Limitation = "LaunchAgent starts at GUI login and does not run before login or survive GUI logout; Local Network approval may require the user"
		report.MemoryEnforcement = "admission-only; macOS has no supported CPU-time or RAM ceiling"
		if _, err := command(ctx, "launchctl", "print", "gui/"+uid); err != nil {
			return report, preflightError("gui_session_required", "service", "log in to the macOS GUI as the service user; a LaunchAgent cannot provide pre-login service")
		}
	default:
		return report, preflightError("service_platform_unsupported", "service", "only Linux user systemd and macOS GUI launchd are supported")
	}
	return report, nil
}

// ValidateWorkerServiceEnvironment probes through the real manager, never a
// foreground substitute. The transient launchd job gets only saved PATH/runtime
// settings and reads no credentials. Its private artifacts are removed afterward.
func ValidateWorkerServiceEnvironment(ctx context.Context, c ServiceConfig, clock Clock, command func(context.Context, string, ...string) ([]byte, error)) (result error) {
	if c.Role != "worker" || c.AllowDeviceless {
		return nil
	}
	if command == nil {
		return preflightError("service_session_unavailable", "service", "service manager runner missing")
	}
	if c.Detection.OS == "linux" {
		return validateWorkerService(ctx, c, command)
	}
	if c.Detection.OS != "darwin" || clock == nil {
		return preflightError("service_platform_unsupported", "service", "supported service manager and Clock required")
	}
	if c.RequireMemoryCeilings {
		return preflightError("memory_ceilings_unsupported", "service", "macOS RAM enforcement is admission-only")
	}
	dir, err := os.MkdirTemp(filepath.Dir(c.Config), ".myce-service-probe-")
	if err != nil {
		return err
	}
	label := "local.mycelium.preflight." + filepath.Base(dir)
	output := filepath.Join(dir, "result.json")
	stderr := filepath.Join(dir, "stderr")
	plist := filepath.Join(dir, "probe.plist")
	args := ""
	for _, arg := range []string{"/usr/bin/env", "-u", "MYCE_TOKEN", c.Binary, "detect", "--config", c.Config} {
		args += "<string>" + html.EscapeString(arg) + "</string>"
	}
	text := `<?xml version="1.0"?><plist version="1.0"><dict><key>Label</key><string>` + label + `</string><key>ProgramArguments</key><array>` + args + `</array><key>EnvironmentVariables</key><dict><key>PATH</key><string>` + html.EscapeString(c.Path) + `</string></dict><key>RunAtLoad</key><true/><key>StandardOutPath</key><string>` + html.EscapeString(output) + `</string><key>StandardErrorPath</key><string>` + html.EscapeString(stderr) + `</string><key>SoftResourceLimits</key><dict><key>FileSize</key><integer>1048576</integer></dict></dict></plist>`
	if err = os.WriteFile(plist, []byte(text), 0600); err != nil {
		return err
	}
	domain := "gui/" + strconv.Itoa(os.Getuid())
	target := domain + "/" + label
	// Even a failed bootstrap may have registered the job.
	defer func() {
		cleanup, cancel := SetupTimeout(context.WithoutCancel(ctx))
		defer cancel()
		_, err := command(cleanup, "launchctl", "bootout", target)
		if err == nil || installExit(err, 113) {
			_ = os.RemoveAll(dir)
		} else {
			result = errors.Join(result, preflightError("service_probe_cleanup_failed", "service", "launchd probe cleanup is unconfirmed; retained private probe artifacts for recovery"))
		}
	}()
	if _, err = command(ctx, "launchctl", "bootstrap", domain, plist); err != nil {
		return preflightError("service_environment_invalid", "service", "cannot bootstrap the launchd GUI detection probe")
	}
	// Clock owns the probe deadline. The real ticker only paces local I/O;
	// it does not advance scheduling time (Clock deliberately exposes only Now).
	deadline := clock.Now().Add(10 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if typed := serviceProbeFailure(readProbeOutput(stderr, 4096), nil); typed != nil {
			return typed
		}
		b := readProbeOutput(output, 1<<20)
		if len(b) > 0 {
			var got HardwarePreflight
			if json.Unmarshal(b, &got) == nil {
				if err = validateServiceHardware(c, got); err != nil {
					return err
				}
				return nil
			}
		}
		if !clock.Now().Before(deadline) {
			return preflightError("service_environment_invalid", "service", "launchd could not validate configured components and PATH; inspect runtime permissions")
		}
		select {
		case <-ctx.Done():
			return preflightError("service_environment_invalid", "service", "launchd detection did not complete before the setup deadline")
		case <-ticker.C:
		}
	}
}

func (r ServiceSessionReport) String() string {
	return fmt.Sprintf("%s; lingering: %s; %s; %s", r.Manager, r.Lingering, r.Limitation, r.MemoryEnforcement)
}

// boundedProbeOutput caps both stdout and stderr while still draining the
// subprocess pipes. Version/inventory output and private stderr never escape
// through operator-facing errors.
type boundedProbeOutput struct {
	buffer bytes.Buffer
	limit  int
}

func (b *boundedProbeOutput) Bytes() []byte { return b.buffer.Bytes() }

func (b *boundedProbeOutput) Write(p []byte) (int, error) {
	n := len(p)
	if remaining := b.limit - b.buffer.Len(); remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = b.buffer.Write(p)
	}
	return n, nil
}
func readProbeOutput(path string, limit int64) []byte {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	b, _ := io.ReadAll(io.LimitReader(f, limit))
	return b
}

// Only stable B2 reasons are accepted. Messages/components from command output
// are discarded, including paths, device names and arbitrary private stderr.
func serviceProbeFailure(output []byte, err error) error {
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		output = exit.Stderr
	}
	if len(output) > 4096 {
		output = output[:4096]
	}
	messages := map[string]string{
		"gpu_driver_missing":           "supported GPU has no driver inventory; install its runtime and re-detect",
		"gpu_driver_unavailable":       "GPU driver probe failed in the service session; repair runtime access",
		"gpu_inventory_unavailable":    "service GPU inventory is unavailable; restore configured components",
		"gpu_inventory_invalid":        "service GPU inventory is invalid; repair the runtime and re-detect",
		"gpu_memory_unavailable":       "service GPU memory inventory is unavailable; supply a measured device budget",
		"gpu_permission_denied":        "service cannot access the configured GPU nodes; repair driver permissions",
		"hardware_unsupported":         "service OS/architecture is unsupported",
		"device_selector_unmatched":    "service cannot observe the explicit device selectors",
		"service_inventory_incomplete": "service cannot observe every configured component and budget",
	}
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			if _, err := time.Parse("2006/01/02 15:04:05", strings.Join(fields[:2], " ")); err == nil {
				line = strings.Join(fields[2:], " ")
			}
		}
		code, _, _ := strings.Cut(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "myce: ")), ":")
		if message, ok := messages[code]; ok {
			return preflightError(code, "service", message)
		}
	}
	return nil
}
