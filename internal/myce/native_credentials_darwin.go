package myce

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

var nativeCredentialSupport = sync.OnceValue(func() bool {
	if info, err := os.Stat("/usr/bin/sandbox-exec"); err == nil && info.Mode()&0111 != 0 {
		return true
	}
	log.Printf("WARNING plain native jobs can read worker credentials: sandbox-exec unavailable")
	return false
})

// An allow-default Seatbelt profile that withholds only the worker's
// credentials and SSH keys from an unsandboxed job (#1309). Everything else,
// including nested sandbox-exec (SwiftPM manifests), behaves as before; a
// program calling sandbox_init itself is refused.
func (w Worker) withholdNativeCredentials(cmd *exec.Cmd, spool, work string, env map[string]string) error {
	if !nativeCredentialSupport() {
		return nil
	}
	deny, allow, err := w.nativeCredentialPaths(spool, []string{work, env["MYCE_CHECKPOINT_DIR"], env["MYCE_USAGE_FILE"]})
	if err != nil || len(deny) == 0 {
		return err
	}
	cmd.Args, cmd.Path = nativeCredentialProfileArgs(deny, allow, cmd), "/usr/bin/sandbox-exec"
	return nil
}

func nativeCredentialProfileArgs(deny, allow []string, cmd *exec.Cmd) []string {
	var except strings.Builder
	if len(allow) > 0 {
		except.WriteString(" (require-not (require-any")
		for _, path := range allow {
			fmt.Fprintf(&except, " (subpath %s)", strconv.Quote(path))
		}
		except.WriteString("))")
	}
	var profile strings.Builder
	profile.WriteString("(version 1)\n(allow default)\n")
	for _, path := range deny {
		for _, operation := range []string{"file-read-data", "file-write*"} {
			// An empty require-any is invalid Seatbelt syntax. With no
			// exceptions, deny the whole credential subtree directly.
			if len(allow) == 0 {
				fmt.Fprintf(&profile, "(deny %s (subpath %s))\n", operation, strconv.Quote(path))
			} else {
				fmt.Fprintf(&profile, "(deny %s (require-all (subpath %s)%s))\n", operation, strconv.Quote(path), except.String())
			}
		}
	}
	// Seatbelt refuses to exec setuid programs, and any non-trivial profile
	// refuses a nested sandbox_apply. Read-only process and network diagnostics
	// and sandbox-exec (SwiftPM manifests and plugins) run outside the profile as
	// before; crontab, at, authopen and the like stay refused, since they could
	// read or replay withheld files.
	profile.WriteString(`(allow process-exec (require-any (literal "/bin/ps") (literal "/usr/bin/top") (literal "/usr/sbin/traceroute") (literal "/usr/sbin/traceroute6") (literal "/usr/bin/sandbox-exec")) (with no-sandbox))` + "\n")
	// sandbox-exec execs its target in place, so process custody is unchanged.
	// The environment passes through: plain jobs keep their loader variables.
	path := cmd.Path
	if path == "" {
		path = cmd.Args[0]
	}
	return append([]string{"/usr/bin/sandbox-exec", "-p", profile.String(), path}, cmd.Args[1:]...)
}

// Darwin services read their token from a file into process memory only; it
// never appears in another process's view of their initial environment.
func protectServiceCredentials() error { return nil }
