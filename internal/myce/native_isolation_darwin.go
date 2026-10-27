package myce

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Seatbelt restrictions survive exec/fork and apply to same-UID jobs. The
// worker retains process custody, signaling and memory probes.
// sandbox-exec is deprecated: failure to install its profile fails the attempt.
func (w Worker) isolateNativeCommand(cmd *exec.Cmd, spool, work string, env map[string]string, ports []string, network bool) error {
	canonical := func(path string) string {
		if path == "" {
			return ""
		}
		path, _ = filepath.Abs(path)
		// Missing leaf paths still need their existing symlinked ancestors
		// resolved (/var is /private/var on macOS).
		ancestor, suffix := path, ""
		for {
			if resolved, err := filepath.EvalSymlinks(ancestor); err == nil {
				return filepath.Join(resolved, suffix)
			}
			parent := filepath.Dir(ancestor)
			if parent == ancestor {
				break
			}
			suffix = filepath.Join(filepath.Base(ancestor), suffix)
			ancestor = parent
		}
		return path
	}
	work = canonical(work)
	dir := canonical(cmd.Dir)
	if rel, err := filepath.Rel(work, dir); err != nil || rel != "." && !filepath.IsLocal(rel) {
		return fmt.Errorf("native isolation requires workdir beneath {work}; stage source with inputs or a profile")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	read := []string{work, canonical(env["MYCE_CHECKPOINT_DIR"])}
	write := slices.Clone(read)
	if path := env["MYCE_USAGE_FILE"]; path != "" {
		write = append(write, canonical(path))
		read = append(read, canonical(path))
	}
	// These roots are worker configuration, never paths supplied by run.env.
	for _, path := range []string{filepath.Join(spool, "environment-cache"), w.EnvironmentRoot, w.ExtensionRoot} {
		if path != "" {
			read = append(read, canonical(path))
		}
	}
	if w.environmentCache != nil {
		read = append(read, canonical(w.environmentCache.Root))
	}
	protected := []string{spool, w.CheckpointRoot, w.JobFileRoot, w.SecretRoot,
		filepath.Join(home, ".local/share/mycelium"), filepath.Join(home, ".config"), filepath.Join(home, ".ssh"), filepath.Join(home, "Library/Keychains")}
	if w.ServiceConfig != nil {
		c := w.ServiceConfig
		for _, path := range []string{c.Config, c.Credentials} {
			if path != "" {
				protected = append(protected, filepath.Dir(path))
			}
		}
		protected = append(protected, c.EnvironmentFile, c.JoiningFile, c.State)
	}
	var profile strings.Builder
	profile.WriteString(`(version 1)
(allow default)
(deny job-creation)
(deny mach-task-name)
(deny process-info* (require-not (target self)))
(deny signal (require-not (target same-sandbox)))
(deny mach-register)
(deny mach-lookup (require-not (require-any
  (global-name "com.apple.MTLCompilerService")
  (global-name-prefix "com.apple.MTLCompilerService.")
  (global-name "com.apple.cvmsServ")
  (global-name "com.apple.system.opendirectoryd.libinfo")
  (global-name "com.apple.system.configd")
  (global-name "com.apple.mDNSResponder"))))
(deny network-outbound (remote unix-socket))
`)
	except := func(paths []string) string {
		var b strings.Builder
		b.WriteString("(require-not (require-any")
		for _, path := range paths {
			if path != "" {
				fmt.Fprintf(&b, " (subpath %s)", strconv.Quote(path))
			}
		}
		b.WriteString("))")
		return b.String()
	}
	for _, path := range protected {
		if path == "" {
			continue
		}
		path = canonical(path)
		fmt.Fprintf(&profile, "(deny file-read-data (require-all (subpath %s) %s))\n", strconv.Quote(path), except(read))
		fmt.Fprintf(&profile, "(deny file-write* (require-all (subpath %s) %s))\n", strconv.Quote(path), except(write))
	}
	// Prevent poisoning the worker's executables, startup files and shared tools.
	for _, path := range append([]string{home, "/System", "/usr", "/bin", "/sbin", "/Library", "/opt", "/Applications"}, read...) {
		if path != "" {
			fmt.Fprintf(&profile, "(deny file-write* (require-all (subpath %s) %s))\n", strconv.Quote(canonical(path)), except(write))
		}
	}
	// Protect an executable in a nonstandard installation outside the home too.
	if path, err := os.Executable(); err == nil {
		fmt.Fprintf(&profile, "(deny file-write* (literal %s))\n", strconv.Quote(canonical(path)))
	}
	// Offline jobs must not expose an inbound channel either. General egress
	// does not authorize a server; only reserved sidecar loopback may listen.
	if len(ports) == 0 {
		profile.WriteString("(deny network-inbound)\n")
	} else {
		for _, operation := range []string{"network-inbound"} {
			fmt.Fprintf(&profile, "(deny %s (require-not (require-any", operation)
			for _, port := range ports {
				fmt.Fprintf(&profile, " (local tcp %s)", strconv.Quote("localhost:"+port))
			}
			profile.WriteString(")))\n")
		}
	}
	// Seatbelt's public address filter accepts only * or localhost. Withhold
	// outbound access except this attempt's sidecars rather than leaving a
	// non-loopback alias of a local control listener reachable.
	if network {
		profile.WriteString("(deny network-outbound (require-all (remote ip \"localhost:*\")")
		if len(ports) > 0 {
			profile.WriteString(" (require-not (require-any")
			for _, port := range ports {
				fmt.Fprintf(&profile, " (remote tcp %s)", strconv.Quote("localhost:"+port))
			}
			profile.WriteString("))")
		}
		profile.WriteString("))\n")
	} else if len(ports) == 0 {
		profile.WriteString("(deny network-outbound)\n")
	} else {
		profile.WriteString("(deny network-outbound (require-not (require-any")
		// Only the worker's reserved sidecar ports are exceptions. Authored env
		// cannot add a port, and Unix sockets have no exception.
		for _, port := range ports {
			fmt.Fprintf(&profile, " (remote tcp %s)", strconv.Quote("localhost:"+port))
		}
		profile.WriteString(")))\n")
	}
	for _, port := range w.nativeControlPorts() {
		if slices.Contains(ports, port) {
			return fmt.Errorf("native sidecar port conflicts with queue control port")
		}
		// A port-wide deny also covers DNS changes and alternative queue IPs.
		// This intentionally withholds unrelated services on the same port.
		fmt.Fprintf(&profile, "(deny network-outbound (remote tcp %s))\n", strconv.Quote("*:"+port))
	}
	// Apply authored loader/shell variables only after Seatbelt is active.
	args := append([]string{"/usr/bin/sandbox-exec", "-p", profile.String(), "/usr/bin/env", "-i"}, environmentList(env)...)
	cmd.Args = append(args, cmd.Args...)
	cmd.Path = "/usr/bin/sandbox-exec"
	cmd.Env = environmentList(jobBaseEnvironment())
	return nil
}

func (w Worker) nativeIsolationCapabilities() []string {
	if info, err := os.Stat("/usr/bin/sandbox-exec"); err == nil && info.Mode()&0111 != 0 {
		return []string{"native_isolation_seatbelt", "native_network", "native_sidecars"}
	}
	return []string{"native_isolation_unavailable"}
}
