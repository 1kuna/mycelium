package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	osuser "os/user"
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
	RunUser    string
	TargetPath string
	BackupPath string
	DryRun     bool
}

type serviceStatus struct {
	Manager string
	Health  string
}

type serviceUpgradePlan struct {
	Spec          serviceSpec
	GOOS          string
	BackupPath    string
	CurrentDigest string
	CurrentBytes  int64
	TargetDigest  string
	TargetBytes   int64
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
		return fmt.Errorf("usage: mycelium service <plan|install|status|uninstall|upgrade>")
	}
	spec, err := parseServiceSpec(args[0], args[1:], goos)
	if err != nil {
		return err
	}
	if args[0] == "plan" {
		fmt.Fprint(os.Stdout, renderServicePlan(spec, goos))
		return nil
	}
	if args[0] == "upgrade" {
		plan, err := buildServiceUpgradePlan(spec, goos)
		if err != nil {
			return err
		}
		fmt.Fprint(os.Stdout, renderServiceUpgradePlan(plan))
		return nil
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
	binaryPath := fs.String("binary", "", "service binary path override")
	name := fs.String("name", "default", "service name suffix")
	user := fs.Bool("user", false, "install as a user service")
	system := fs.Bool("system", false, "install as a system service")
	runUser := fs.String("run-user", "", "user account for a system LaunchDaemon")
	targetPath := fs.String("target-binary", "", "candidate binary for a dry-run service upgrade plan")
	backupPath := fs.String("backup", "", "backup path for the current service binary during upgrade")
	dryRun := fs.Bool("dry-run", false, "print an upgrade plan without changing the host")
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
	binary := strings.TrimSpace(*binaryPath)
	if binary == "" {
		binary, err = os.Executable()
		if err != nil {
			return serviceSpec{}, err
		}
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return serviceSpec{}, fmt.Errorf("user home is required")
	}
	serviceRunUser := strings.TrimSpace(*runUser)
	if serviceRunUser == "" {
		serviceRunUser = defaultServiceRunUser(scope, goos)
	}
	if serviceRunUser != "" && scope == serviceScopeSystem && goos == "darwin" {
		resolvedHome, err := userHome(serviceRunUser)
		if err != nil {
			return serviceSpec{}, err
		}
		home = resolvedHome
	}
	spec := serviceSpec{
		Name:       sanitizeServiceName(*name),
		Scope:      scope,
		ConfigPath: *configPath,
		BinaryPath: binary,
		Home:       home,
		LogDir:     filepath.Join(home, ".mycelium", "logs"),
		RunUser:    serviceRunUser,
		TargetPath: strings.TrimSpace(*targetPath),
		BackupPath: strings.TrimSpace(*backupPath),
		DryRun:     *dryRun,
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

func defaultServiceRunUser(scope serviceScope, goos string) string {
	if scope != serviceScopeSystem || goos != "darwin" {
		return ""
	}
	if sudoUser := strings.TrimSpace(os.Getenv("SUDO_USER")); sudoUser != "" && sudoUser != "root" {
		return sudoUser
	}
	if envUser := strings.TrimSpace(os.Getenv("USER")); envUser != "" && envUser != "root" {
		return envUser
	}
	current, err := osuser.Current()
	if err != nil {
		return ""
	}
	name := strings.TrimSpace(current.Username)
	if i := strings.LastIndexAny(name, `\`); i >= 0 {
		name = name[i+1:]
	}
	if name == "root" {
		return ""
	}
	return name
}

func userHome(name string) (string, error) {
	user, err := osuser.Lookup(name)
	if err != nil {
		return "", fmt.Errorf("lookup run user %q: %w", name, err)
	}
	if user.HomeDir == "" {
		return "", fmt.Errorf("run user %q has no home directory", name)
	}
	return user.HomeDir, nil
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
	if spec.RunUser != "" && spec.Scope == serviceScopeSystem {
		writePlistString(&out, "UserName", spec.RunUser)
	}
	out.WriteString("<key>RunAtLoad</key>\n<true/>\n")
	out.WriteString("<key>KeepAlive</key>\n<dict>\n<key>NetworkState</key>\n<true/>\n<key>SuccessfulExit</key>\n<false/>\n</dict>\n")
	writePlistString(&out, "WorkingDirectory", spec.Home)
	writePlistString(&out, "StandardOutPath", filepath.Join(spec.LogDir, spec.Name+".out.log"))
	writePlistString(&out, "StandardErrorPath", filepath.Join(spec.LogDir, spec.Name+".err.log"))
	out.WriteString("</dict>\n</plist>\n")
	return out.String()
}

func renderServicePlan(spec serviceSpec, goos string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "service\t%s\n", spec.Label)
	fmt.Fprintf(&out, "scope\t%s\n", spec.Scope)
	fmt.Fprintf(&out, "binary\t%s\n", spec.BinaryPath)
	fmt.Fprintf(&out, "config\t%s\n", spec.ConfigPath)
	fmt.Fprintf(&out, "unit\t%s\n", spec.UnitPath)
	fmt.Fprintf(&out, "logs\t%s\n", spec.LogDir)
	if spec.RunUser != "" {
		fmt.Fprintf(&out, "run_user\t%s\n", spec.RunUser)
	}
	out.WriteString("commands\n")
	switch goos {
	case "darwin":
		domain := launchdDomain(spec)
		prefix := ""
		if spec.Scope == serviceScopeSystem {
			prefix = "sudo "
		}
		fmt.Fprintf(&out, "  install: %s%s service install --config %s --name %s --%s\n", prefix, shellQuote(spec.BinaryPath), shellQuote(spec.ConfigPath), shellQuote(spec.Name), spec.Scope)
		fmt.Fprintf(&out, "  status:  %s%s service status --config %s --name %s --%s\n", prefix, shellQuote(spec.BinaryPath), shellQuote(spec.ConfigPath), shellQuote(spec.Name), spec.Scope)
		fmt.Fprintf(&out, "  unload:  %slaunchctl bootout %s %s\n", prefix, domain, shellQuote(spec.UnitPath))
		fmt.Fprintf(&out, "  reload:  %slaunchctl bootstrap %s %s && %slaunchctl enable %s/%s && %slaunchctl kickstart -k %s/%s\n", prefix, domain, shellQuote(spec.UnitPath), prefix, domain, spec.Label, prefix, domain, spec.Label)
		fmt.Fprintf(&out, "  rollback: restore the prior binary, then run %slaunchctl kickstart -k %s/%s and service status\n", prefix, domain, spec.Label)
		out.WriteString("unit_body\n")
		out.WriteString(renderLaunchdPlist(spec))
	case "linux":
		prefix := ""
		systemctl := "systemctl --user"
		if spec.Scope == serviceScopeSystem {
			prefix = "sudo "
			systemctl = "systemctl"
		}
		unit := filepath.Base(spec.UnitPath)
		fmt.Fprintf(&out, "  install: %s%s service install --config %s --name %s --%s\n", prefix, shellQuote(spec.BinaryPath), shellQuote(spec.ConfigPath), shellQuote(spec.Name), spec.Scope)
		fmt.Fprintf(&out, "  status:  %s%s service status --config %s --name %s --%s\n", prefix, shellQuote(spec.BinaryPath), shellQuote(spec.ConfigPath), shellQuote(spec.Name), spec.Scope)
		fmt.Fprintf(&out, "  restart: %s%s restart %s\n", prefix, systemctl, shellQuote(unit))
		fmt.Fprintf(&out, "  rollback: restore the prior binary, then run %s%s restart %s and service status\n", prefix, systemctl, shellQuote(unit))
		out.WriteString("unit_body\n")
		out.WriteString(renderSystemdUnit(spec))
	default:
		fmt.Fprintf(&out, "unsupported\t%s\n", goos)
	}
	if !strings.HasSuffix(out.String(), "\n") {
		out.WriteString("\n")
	}
	return out.String()
}

func buildServiceUpgradePlan(spec serviceSpec, goos string) (serviceUpgradePlan, error) {
	if !spec.DryRun {
		return serviceUpgradePlan{}, fmt.Errorf("service upgrade is dry-run only; pass --dry-run and follow the printed operator commands")
	}
	if spec.TargetPath == "" {
		return serviceUpgradePlan{}, fmt.Errorf("--target-binary is required for service upgrade")
	}
	if spec.BackupPath == "" {
		spec.BackupPath = spec.BinaryPath + ".pre-upgrade.bak"
	}
	currentDigest, currentBytes, err := fileSHA256(spec.BinaryPath)
	if err != nil {
		return serviceUpgradePlan{}, fmt.Errorf("current service binary: %w", err)
	}
	targetDigest, targetBytes, err := fileSHA256(spec.TargetPath)
	if err != nil {
		return serviceUpgradePlan{}, fmt.Errorf("target service binary: %w", err)
	}
	return serviceUpgradePlan{
		Spec:          spec,
		GOOS:          goos,
		BackupPath:    spec.BackupPath,
		CurrentDigest: currentDigest,
		CurrentBytes:  currentBytes,
		TargetDigest:  targetDigest,
		TargetBytes:   targetBytes,
	}, nil
}

func fileSHA256(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return "", 0, err
	}
	if info.IsDir() {
		return "", 0, fmt.Errorf("%s is a directory", path)
	}
	return hex.EncodeToString(hash.Sum(nil)), n, nil
}

func renderServiceUpgradePlan(plan serviceUpgradePlan) string {
	spec := plan.Spec
	var out strings.Builder
	fmt.Fprintf(&out, "upgrade_dry_run\ttrue\n")
	fmt.Fprintf(&out, "service\t%s\n", spec.Label)
	fmt.Fprintf(&out, "scope\t%s\n", spec.Scope)
	fmt.Fprintf(&out, "stable_binary\t%s\n", spec.BinaryPath)
	fmt.Fprintf(&out, "target_binary\t%s\n", spec.TargetPath)
	fmt.Fprintf(&out, "backup\t%s\n", plan.BackupPath)
	fmt.Fprintf(&out, "current_sha256\t%s\n", plan.CurrentDigest)
	fmt.Fprintf(&out, "current_bytes\t%d\n", plan.CurrentBytes)
	fmt.Fprintf(&out, "target_sha256\t%s\n", plan.TargetDigest)
	fmt.Fprintf(&out, "target_bytes\t%d\n", plan.TargetBytes)
	out.WriteString("commands\n")
	statusCmd := serviceStatusCommand(spec)
	fmt.Fprintf(&out, "  preflight_status: %s\n", statusCmd)
	fmt.Fprintf(&out, "  backup: %s\n", servicePrivilegedCommand(spec, "cp", spec.BinaryPath, plan.BackupPath))
	fmt.Fprintf(&out, "  replace: %s\n", servicePrivilegedCommand(spec, "install", "-m", "0755", spec.TargetPath, spec.BinaryPath))
	fmt.Fprintf(&out, "  restart: %s\n", serviceRestartCommand(spec, plan.GOOS))
	fmt.Fprintf(&out, "  verify: %s\n", statusCmd)
	fmt.Fprintf(&out, "  rollback: %s && %s && %s\n",
		servicePrivilegedCommand(spec, "install", "-m", "0755", plan.BackupPath, spec.BinaryPath),
		serviceRestartCommand(spec, plan.GOOS),
		statusCmd,
	)
	out.WriteString("notes\n")
	out.WriteString("  no live change was made\n")
	out.WriteString("  run preflight_status before backup and verify after restart\n")
	out.WriteString("  rollback restores the backed-up stable binary and reruns service status\n")
	return out.String()
}

func serviceStatusCommand(spec serviceSpec) string {
	prefix := ""
	if spec.Scope == serviceScopeSystem {
		prefix = "sudo "
	}
	return fmt.Sprintf("%s%s service status --config %s --name %s --%s", prefix, shellQuote(spec.BinaryPath), shellQuote(spec.ConfigPath), shellQuote(spec.Name), spec.Scope)
}

func servicePrivilegedCommand(spec serviceSpec, name string, args ...string) string {
	parts := []string{name}
	for _, arg := range args {
		parts = append(parts, shellQuote(arg))
	}
	command := strings.Join(parts, " ")
	if spec.Scope == serviceScopeSystem {
		return "sudo " + command
	}
	return command
}

func serviceRestartCommand(spec serviceSpec, goos string) string {
	switch goos {
	case "darwin":
		prefix := ""
		if spec.Scope == serviceScopeSystem {
			prefix = "sudo "
		}
		return fmt.Sprintf("%slaunchctl kickstart -k %s/%s", prefix, launchdDomain(spec), spec.Label)
	case "linux":
		systemctl := "systemctl --user"
		prefix := ""
		if spec.Scope == serviceScopeSystem {
			systemctl = "systemctl"
			prefix = "sudo "
		}
		return fmt.Sprintf("%s%s restart %s", prefix, systemctl, shellQuote(filepath.Base(spec.UnitPath)))
	default:
		return "unsupported service restart for " + goos
	}
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	if strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '/' || r == '.' || r == '_' || r == '-' || r == ':')
	}) == -1 {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
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
		body, _ := readLimitedPeerRPCBody(resp.Body)
		return "", fmt.Errorf("peer diagnostics returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var report peerDiagnosticsReport
	if err := decodePeerRPCResponse(resp.Body, &report); err != nil {
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
