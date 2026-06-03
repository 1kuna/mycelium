package main

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type serviceScope string

const (
	serviceScopeUser   serviceScope = "user"
	serviceScopeSystem serviceScope = "system"
)

type serviceSpec struct {
	Name       string
	Label      string
	Scope      serviceScope
	ConfigPath string
	BinaryPath string
	Home       string
	LogDir     string
	UnitPath   string
}

type serviceStatus struct {
	Manager string
	Health  string
}

type serviceManager interface {
	Install(context.Context, serviceSpec) error
	Status(context.Context, serviceSpec) (string, error)
	Uninstall(context.Context, serviceSpec) error
}

type serviceCommandRunner func(context.Context, string, ...string) error

func runService(ctx context.Context, args []string) error {
	return runServiceWithManager(ctx, args, nil, runtime.GOOS)
}

func runServiceWithManager(ctx context.Context, args []string, manager serviceManager, goos string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mycelium service <install|status|uninstall>")
	}
	spec, err := parseServiceSpec(args[0], args[1:], goos)
	if err != nil {
		return err
	}
	if manager == nil {
		manager, err = serviceManagerForGOOS(goos, runServiceCommand)
		if err != nil {
			return err
		}
	}
	switch args[0] {
	case "install":
		if err := manager.Install(ctx, spec); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "installed\t%s\t%s\n", spec.Label, spec.UnitPath)
		return nil
	case "status":
		managerStatus, err := manager.Status(ctx, spec)
		if err != nil {
			return err
		}
		health, err := servicePeerHealth(ctx, spec.ConfigPath)
		if err != nil {
			return err
		}
		seeds, err := servicePeerDiagnostics(ctx, spec.ConfigPath)
		if err != nil {
			return err
		}
		status := serviceStatus{Manager: managerStatus, Health: health}
		if seeds == "" {
			fmt.Fprintf(os.Stdout, "service\t%s\npeer\t%s\n", status.Manager, status.Health)
			return nil
		}
		fmt.Fprintf(os.Stdout, "service\t%s\npeer\t%s\nseeds\t%s\n", status.Manager, status.Health, seeds)
		return nil
	case "uninstall":
		if err := manager.Uninstall(ctx, spec); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "uninstalled\t%s\n", spec.Label)
		return nil
	default:
		return fmt.Errorf("unknown service command %q", args[0])
	}
}

func parseServiceSpec(command string, args []string, goos string) (serviceSpec, error) {
	fs := flag.NewFlagSet("service "+command, flag.ContinueOnError)
	configPath := fs.String("config", "", "peer config JSON path")
	name := fs.String("name", "default", "service name suffix")
	user := fs.Bool("user", false, "install as a user service")
	system := fs.Bool("system", false, "install as a system service")
	if err := fs.Parse(args); err != nil {
		return serviceSpec{}, err
	}
	if *configPath == "" {
		return serviceSpec{}, fmt.Errorf("--config is required")
	}
	scope, err := parseServiceScope(*user, *system)
	if err != nil {
		return serviceSpec{}, err
	}
	binary, err := os.Executable()
	if err != nil {
		return serviceSpec{}, err
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return serviceSpec{}, fmt.Errorf("user home is required")
	}
	spec := serviceSpec{
		Name:       sanitizeServiceName(*name),
		Scope:      scope,
		ConfigPath: *configPath,
		BinaryPath: binary,
		Home:       home,
		LogDir:     filepath.Join(home, ".mycelium", "logs"),
	}
	if spec.Name == "" {
		return serviceSpec{}, fmt.Errorf("service name must contain a letter, digit, dot, underscore, or dash")
	}
	spec.Label = "com.mycelium." + spec.Name
	spec.UnitPath, err = serviceUnitPath(spec, goos)
	if err != nil {
		return serviceSpec{}, err
	}
	return spec, nil
}

func parseServiceScope(user, system bool) (serviceScope, error) {
	if user && system {
		return "", fmt.Errorf("--user and --system are mutually exclusive")
	}
	if system {
		return serviceScopeSystem, nil
	}
	return serviceScopeUser, nil
}

func sanitizeServiceName(raw string) string {
	raw = strings.TrimSpace(raw)
	var out strings.Builder
	for _, r := range raw {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-' {
			out.WriteRune(r)
		}
	}
	return out.String()
}

func serviceUnitPath(spec serviceSpec, goos string) (string, error) {
	switch goos {
	case "darwin":
		if spec.Scope == serviceScopeSystem {
			return filepath.Join("/Library/LaunchDaemons", spec.Label+".plist"), nil
		}
		return filepath.Join(spec.Home, "Library", "LaunchAgents", spec.Label+".plist"), nil
	case "linux":
		unit := "mycelium-" + spec.Name + ".service"
		if spec.Scope == serviceScopeSystem {
			return filepath.Join("/etc/systemd/system", unit), nil
		}
		return filepath.Join(spec.Home, ".config", "systemd", "user", unit), nil
	default:
		return "", fmt.Errorf("service install is unsupported on %s", goos)
	}
}

func serviceManagerForGOOS(goos string, runner serviceCommandRunner) (serviceManager, error) {
	switch goos {
	case "darwin":
		return launchdManager{Run: runner}, nil
	case "linux":
		return systemdManager{Run: runner}, nil
	default:
		return nil, fmt.Errorf("service install is unsupported on %s", goos)
	}
}

func runServiceCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

type launchdManager struct {
	Run serviceCommandRunner
}

func (m launchdManager) Install(ctx context.Context, spec serviceSpec) error {
	if err := writeServiceFile(spec.UnitPath, spec.LogDir, renderLaunchdPlist(spec)); err != nil {
		return err
	}
	domain := launchdDomain(spec)
	_ = m.run(ctx, "launchctl", "bootout", domain, spec.UnitPath)
	if err := m.run(ctx, "launchctl", "bootstrap", domain, spec.UnitPath); err != nil {
		return err
	}
	if err := m.run(ctx, "launchctl", "enable", domain+"/"+spec.Label); err != nil {
		return err
	}
	return m.run(ctx, "launchctl", "kickstart", "-k", domain+"/"+spec.Label)
}

func (m launchdManager) Status(ctx context.Context, spec serviceSpec) (string, error) {
	if err := m.run(ctx, "launchctl", "print", launchdDomain(spec)+"/"+spec.Label); err != nil {
		return "", err
	}
	return "active", nil
}

func (m launchdManager) Uninstall(ctx context.Context, spec serviceSpec) error {
	_ = m.run(ctx, "launchctl", "bootout", launchdDomain(spec), spec.UnitPath)
	return os.RemoveAll(spec.UnitPath)
}

func (m launchdManager) run(ctx context.Context, name string, args ...string) error {
	if m.Run == nil {
		m.Run = runServiceCommand
	}
	return m.Run(ctx, name, args...)
}

func launchdDomain(spec serviceSpec) string {
	if spec.Scope == serviceScopeSystem {
		return "system"
	}
	return "gui/" + strconv.Itoa(os.Getuid())
}

type systemdManager struct {
	Run serviceCommandRunner
}

func (m systemdManager) Install(ctx context.Context, spec serviceSpec) error {
	if err := writeServiceFile(spec.UnitPath, spec.LogDir, renderSystemdUnit(spec)); err != nil {
		return err
	}
	if err := m.systemctl(ctx, spec, "daemon-reload"); err != nil {
		return err
	}
	return m.systemctl(ctx, spec, "enable", "--now", filepath.Base(spec.UnitPath))
}

func (m systemdManager) Status(ctx context.Context, spec serviceSpec) (string, error) {
	if err := m.systemctl(ctx, spec, "is-active", filepath.Base(spec.UnitPath)); err != nil {
		return "", err
	}
	return "active", nil
}

func (m systemdManager) Uninstall(ctx context.Context, spec serviceSpec) error {
	_ = m.systemctl(ctx, spec, "disable", "--now", filepath.Base(spec.UnitPath))
	if err := os.RemoveAll(spec.UnitPath); err != nil {
		return err
	}
	return m.systemctl(ctx, spec, "daemon-reload")
}

func (m systemdManager) systemctl(ctx context.Context, spec serviceSpec, args ...string) error {
	if m.Run == nil {
		m.Run = runServiceCommand
	}
	command := []string{}
	if spec.Scope == serviceScopeUser {
		command = append(command, "--user")
	}
	command = append(command, args...)
	return m.Run(ctx, "systemctl", command...)
}

func writeServiceFile(path, logDir, body string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(body), 0644)
}

func renderLaunchdPlist(spec serviceSpec) string {
	var out strings.Builder
	out.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	out.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	out.WriteString("<plist version=\"1.0\">\n<dict>\n")
	writePlistString(&out, "Label", spec.Label)
	out.WriteString("<key>ProgramArguments</key>\n<array>\n")
	for _, arg := range []string{spec.BinaryPath, "run", "--config", spec.ConfigPath} {
		writePlistValue(&out, arg)
	}
	out.WriteString("</array>\n")
	out.WriteString("<key>RunAtLoad</key>\n<true/>\n")
	out.WriteString("<key>KeepAlive</key>\n<dict>\n<key>NetworkState</key>\n<true/>\n<key>SuccessfulExit</key>\n<false/>\n</dict>\n")
	writePlistString(&out, "WorkingDirectory", spec.Home)
	writePlistString(&out, "StandardOutPath", filepath.Join(spec.LogDir, spec.Name+".out.log"))
	writePlistString(&out, "StandardErrorPath", filepath.Join(spec.LogDir, spec.Name+".err.log"))
	out.WriteString("</dict>\n</plist>\n")
	return out.String()
}

func writePlistString(out *strings.Builder, key, value string) {
	out.WriteString("<key>")
	out.WriteString(xmlEscape(key))
	out.WriteString("</key>\n")
	writePlistValue(out, value)
}

func writePlistValue(out *strings.Builder, value string) {
	out.WriteString("<string>")
	out.WriteString(xmlEscape(value))
	out.WriteString("</string>\n")
}

func xmlEscape(value string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(value))
	return buf.String()
}

func renderSystemdUnit(spec serviceSpec) string {
	return strings.Join([]string{
		"[Unit]",
		"Description=Mycelium peer (" + spec.Name + ")",
		"After=network-online.target",
		"Wants=network-online.target",
		"",
		"[Service]",
		"Type=simple",
		"ExecStart=" + quoteSystemdArg(spec.BinaryPath) + " run --config " + quoteSystemdArg(spec.ConfigPath),
		"Restart=always",
		"RestartSec=2",
		"StandardOutput=append:" + filepath.Join(spec.LogDir, spec.Name+".out.log"),
		"StandardError=append:" + filepath.Join(spec.LogDir, spec.Name+".err.log"),
		"",
		"[Install]",
		"WantedBy=default.target",
		"",
	}, "\n")
}

func quoteSystemdArg(value string) string {
	return strconv.Quote(value)
}

func servicePeerHealth(ctx context.Context, configPath string) (string, error) {
	cfg, err := loadPeerConfig(configPath)
	if err != nil {
		return "", err
	}
	base, err := servicePeerBaseURL(cfg.Listen)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/peer/health", nil)
	if err != nil {
		return "", err
	}
	if cfg.JoinToken != "" {
		req.Header.Set("X-Myc-Join-Token", cfg.JoinToken)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("peer health failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("peer health returned HTTP %d", resp.StatusCode)
	}
	return "ready", nil
}

func servicePeerDiagnostics(ctx context.Context, configPath string) (string, error) {
	cfg, err := loadPeerConfig(configPath)
	if err != nil {
		return "", err
	}
	if len(cfg.SeedPeers) == 0 {
		return "", nil
	}
	base, err := servicePeerBaseURL(cfg.Listen)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/peer/diagnostics", nil)
	if err != nil {
		return "", err
	}
	if cfg.RPCToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.RPCToken)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("peer diagnostics failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("peer diagnostics returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var report peerDiagnosticsReport
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		return "", err
	}
	ready := 0
	var failures []string
	for _, seed := range report.Seeds {
		if seed.Ready {
			ready++
			continue
		}
		failures = append(failures, seed.Address+": "+seed.Error)
	}
	if len(failures) > 0 {
		return "", fmt.Errorf("peer diagnostics seed failures: %s", strings.Join(failures, "; "))
	}
	return fmt.Sprintf("ready %d/%d", ready, len(report.Seeds)), nil
}

func servicePeerBaseURL(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port), nil
}
