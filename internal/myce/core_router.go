package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

const myceCoreRouter = "/usr/local/libexec/myce-core-router"
const myceCorePattern = "|" + myceCoreRouter + " -p%p -s%s -c%c -d%d -P%P -u%u -g%g -F%F -- %E"
const myceCoreProtocol = "myce-core-router ephemeral-v1"

func ephemeralCorePattern(pattern []byte, err error) error {
	p := strings.TrimSpace(string(pattern))
	if err != nil || p == "" || strings.HasPrefix(p, "|") && p != myceCorePattern {
		return errors.New("ephemeral worker data requires a readable file core route or qualified Mycelium core router; other piped handlers ignore RLIMIT_CORE")
	}
	return nil
}

// The executable path alone is insufficient: the original router stores every
// attempt's core. Require an operator-owned handler and its suppression protocol.
func qualifyCoreRouter(ctx context.Context, stateDir string) error {
	for _, path := range []string{myceCoreRouter, "/etc/myce-crash.json"} {
		st, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("core router qualification: %w", err)
		}
		owner, ok := st.Sys().(*syscall.Stat_t)
		if !ok || owner.Uid != 0 || !st.Mode().IsRegular() || st.Mode().Perm()&0022 != 0 {
			return errors.New("core router and configuration must be root-owned and not group/world writable")
		}
	}
	args := []string{"--qualify"}
	if stateDir != "" {
		args = append(args, stateDir)
	}
	cmd := exec.CommandContext(ctx, myceCoreRouter, args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C"} // Never pass the fleet token.
	var output faultOutput
	cmd.Stdout = &output
	if err := cmd.Run(); err != nil || strings.TrimSpace(output.String()) != myceCoreProtocol {
		return errors.New("Mycelium core router suppression protocol or worker route is unqualified")
	}
	return nil
}

func checkEphemeralCoreRoute(pattern []byte, err error, stateDir string, qualify func(context.Context, string) error) error {
	if err := ephemeralCorePattern(pattern, err); err != nil {
		return err
	}
	if strings.TrimSpace(string(pattern)) == myceCorePattern {
		// Same budget as the exec-helper capability probe. A legacy handler
		// must not block qualification by waiting on an unrelated crash reporter.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return qualify(ctx, stateDir)
	}
	return nil
}
