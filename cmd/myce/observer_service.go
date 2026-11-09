package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"html"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

// The shared owner is user infrastructure, not part of its starter's job.
// An allowlist also excludes future CI tracking variables and unrelated secrets.
// Notifiers retain their separately saved caller environment/absolute executable.
func observerEnvironment() []string {
	var env []string
	for _, key := range []string{"PATH", "LANG", "LANGUAGE", "LC_ALL", "LC_CTYPE", "LC_COLLATE", "LC_MESSAGES", "LC_MONETARY", "LC_NUMERIC", "LC_TIME", "HOME", "USER", "LOGNAME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	if _, ok := os.LookupEnv("PATH"); !ok {
		env = append(env, "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
	}
	// These are explicit internal custody contracts, never job identity. Preserve
	// them across upgrade handoff; a managed attempt must not escape cleanup.
	for _, key := range []string{"MYCE_DEV_SESSION_ROOT", "MYCE_PROCESS_SESSION", "MYCE_SUPERVISED_OBSERVER"} {
		if value := os.Getenv(key); value != "" {
			env = append(env, key+"="+value)
		}
	}
	return env
}

const observerServiceLabel = "local.mycelium.observer"

type observerServiceRunner func(context.Context, string, ...string) error

func ensureInstalledObserver(ctx context.Context, c myce.ServiceConfig) error {
	if os.Getenv("MYCE_DEV_SESSION_ROOT") != "" || os.Getenv("MYCE_PROCESS_SESSION") != "" {
		return nil // These observers belong to attempt/dev cleanup, never a user service.
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	root, err := sharedProfileRoot()
	if err != nil {
		return err
	}
	return installObserverService(ctx, runtime.GOOS, home, c.Binary, root, observerEnvironment(), observerManagerCommand)
}

func observerManagerCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	// Manager output may contain private paths or environment values.
	cmd.Env = notifierEnvironment()
	return cmd.Run()
}

func observerServiceCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: myce service observer install|run")
	}
	f := flag.NewFlagSet("service observer", flag.ContinueOnError)
	rootFlag := f.String("root", "", "private observer registry (default: user cache)")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected observer service arguments")
	}
	root := *rootFlag
	var err error
	if root == "" {
		root, err = sharedProfileRoot()
	}
	if err != nil {
		return err
	}
	if !filepath.IsAbs(root) {
		return errors.New("observer registry must be absolute")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "install":
		if os.Getenv("MYCE_DEV_SESSION_ROOT") != "" || os.Getenv("MYCE_PROCESS_SESSION") != "" {
			return errors.New("observer supervision cannot be installed from a managed attempt or dev session")
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		return installObserverService(ctx, runtime.GOOS, home, executable, root, observerEnvironment(), observerManagerCommand)
	case "run":
		// User managers can themselves inherit CI/session variables. Re-exec is
		// essential: Clearenv alone leaves Linux's initial /proc environment intact.
		var env []string
		for _, entry := range observerEnvironment() {
			if !strings.HasPrefix(entry, "MYCE_") {
				env = append(env, entry)
			}
		}
		env = append(env, "MYCE_SUPERVISED_OBSERVER=1")
		if err := os.Chdir("/"); err != nil {
			return err
		}
		return syscall.Exec(executable, []string{executable, "service", "observer", "own", "--root", root}, env)
	case "own":
		if os.Getenv("MYCE_SUPERVISED_OBSERVER") != "1" {
			return errors.New("observer owner requires service startup")
		}
		if err := os.MkdirAll(root, 0700); err != nil {
			return err
		}
		log.SetOutput(&observerLogWriter{root: root})
		err := runSupervisedObserver(ctx, root)
		if err != nil && ctx.Err() == nil {
			// Never overwrite a live fallback owner's status from a contender.
			// A safe bounded event also survives systemd's start-limit suppression.
			_ = (&observerLogWriter{root: root}).event(struct {
				At    time.Time `json:"at"`
				Event string    `json:"event"`
			}{time.Now(), "supervisor_start_failed"})
		}
		return err
	default:
		return errors.New("usage: myce service observer install|run")
	}
}

func runSupervisedObserver(ctx context.Context, root string) error {
	lock, err := supervisedObserverLock(ctx, filepath.Join(root, "observer.lock"))
	if err != nil {
		return err
	}
	defer lock.Close()
	// Never wait for lifetime ownership while holding startup.lock: idle exit
	// needs startup.lock to release that ownership. Once acquired, serialize
	// socket replacement with the old owner's complete identity-check/unlink.
	startup, err := sharedProfileLock(ctx, filepath.Join(root, "startup.lock"))
	if err != nil {
		return err
	}
	defer startup.Close()
	return serveSharedProfiles(ctx, root, lock, startup.Close)
}

// Wait in the kernel for an existing fallback/handoff owner. Its death releases
// the lifetime lock, so recovery needs neither polling nor another keyed call.
func supervisedObserverLock(ctx context.Context, path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	acquired := make(chan error)
	defer close(done)
	go func() {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
		select {
		case acquired <- err:
		case <-done:
			f.Close()
		}
	}()
	select {
	case err := <-acquired:
		if err != nil {
			f.Close()
			return nil, err
		}
		return f, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func installObserverService(ctx context.Context, platform, home, executable, root string, env []string, command observerServiceRunner) error {
	if !filepath.IsAbs(home) || !filepath.IsAbs(executable) || !filepath.IsAbs(root) {
		return errors.New("observer service requires absolute home, executable and registry paths")
	}
	var path, definition string
	switch platform {
	case "darwin":
		if err := command(ctx, "launchctl", "print", "gui/"+strconv.Itoa(os.Getuid())); err != nil {
			return errors.New("observer_supervision_unavailable: launchd GUI user domain unavailable; keyed commands retain on-demand recovery")
		}
		path = filepath.Join(home, "Library", "LaunchAgents", observerServiceLabel+".plist")
		args := ""
		for _, arg := range append(append([]string{"/usr/bin/env", "-i"}, env...), executable, "service", "observer", "run", "--root", root) {
			args += "<string>" + html.EscapeString(arg) + "</string>"
		}
		// Retain native crash-spawn admission (MY-34): protects the shared user
		// manager from broken-start storms, at the cost of delayed/withheld restart.
		// In-process failures also emit a bounded safe event; pre-exec failures
		// go to the manager log below (systemd uses its native journal).
		definition = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><plist version=\"1.0\"><dict><key>Label</key><string>" + observerServiceLabel + "</string><key>ProgramArguments</key><array>" + args + "</array><key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>AbandonProcessGroup</key><true/><key>WorkingDirectory</key><string>/</string><key>StandardOutPath</key><string>" + html.EscapeString(filepath.Join(root, "observer-supervisor.log")) + "</string><key>StandardErrorPath</key><string>" + html.EscapeString(filepath.Join(root, "observer-supervisor.log")) + "</string></dict></plist>\n"
	case "linux":
		if err := command(ctx, "systemctl", "--user", "show-environment"); err != nil {
			return errors.New("observer_supervision_unavailable: systemd user manager unavailable; keyed commands retain on-demand recovery")
		}
		config := os.Getenv("XDG_CONFIG_HOME")
		if config == "" {
			config = filepath.Join(home, ".config")
		}
		path = filepath.Join(config, "systemd", "user", "myce-observer.service")
		var argv []string
		for _, arg := range append(append([]string{"/usr/bin/env", "-i"}, env...), executable, "service", "observer", "run", "--root", root) {
			argv = append(argv, strconv.Quote(strings.ReplaceAll(arg, "%", "%%")))
		}
		// ':' disables systemd's dollar expansion; argv is literal, not a shell.
		// KillMode=process/AbandonProcessGroup preserve cooperative lock handoff.
		definition = "[Unit]\nDescription=Mycelium completion observer\n\n[Service]\nExecStart=:" + strings.Join(argv, " ") + "\nWorkingDirectory=/\nRestart=always\nKillMode=process\nStandardOutput=journal\nStandardError=journal\nUMask=0077\n\n[Install]\nWantedBy=default.target\n"
	default:
		return errors.New("observer_supervision_unavailable: supported managers are launchd and systemd user")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(definition), 0600); err != nil {
		return err
	}
	if platform == "linux" {
		if err := command(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
			return fmt.Errorf("observer_supervision_unavailable: systemd reload failed: %w", err)
		}
		if err := command(ctx, "systemctl", "--user", "enable", "--now", "myce-observer.service"); err != nil {
			return fmt.Errorf("observer_supervision_unavailable: systemd enable/start failed: %w", err)
		}
		return nil
	}
	target := "gui/" + strconv.Itoa(os.Getuid()) + "/" + observerServiceLabel
	// An installed/running owner must not be killed by reinstall. Definition
	// updates apply at the next user-domain load; image upgrades use handoff.
	if err := command(ctx, "launchctl", "print", target); err == nil {
		return nil
	}
	if err := command(ctx, "launchctl", "bootstrap", "gui/"+strconv.Itoa(os.Getuid()), path); err != nil {
		return fmt.Errorf("observer_supervision_unavailable: launchd bootstrap failed: %w", err)
	}
	return nil
}
