package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const myceCoreRouter = "/usr/local/libexec/myce-core-router"
const myceCorePattern = "|" + myceCoreRouter + " -p%p -s%s -c%c -d%d -P%P -u%u -g%g -F%F -- %E"
const myceCoreProtocol = "myce-core-router ephemeral-v1"
const myceCoreExecutionProtocol = "myce-core-router ephemeral-v2"

type coreRouterProbe func(ctx context.Context, stateDir, executionCgroup string) error

func ephemeralCorePattern(pattern []byte, err error) error {
	p := strings.TrimSpace(string(pattern))
	if err != nil || p == "" || strings.HasPrefix(p, "|") && p != myceCorePattern {
		return errors.New("ephemeral worker data requires a readable file core route or qualified Mycelium core router; other piped handlers ignore RLIMIT_CORE")
	}
	return nil
}

// The executable path alone is insufficient: the original router stores every
// attempt's core. Require an operator-owned handler and its suppression protocol.
func qualifyCoreRouter(ctx context.Context, stateDir, executionCgroup string) error {
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
	protocol := myceCoreProtocol
	if executionCgroup != "" {
		args = append(args, executionCgroup)
		protocol = myceCoreExecutionProtocol
	}
	cmd := exec.CommandContext(ctx, myceCoreRouter, args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C"} // Never pass the fleet token.
	var output faultOutput
	cmd.Stdout = &output
	if err := cmd.Run(); err != nil || strings.TrimSpace(output.String()) != protocol {
		return errors.New("Mycelium core router suppression protocol or worker route is unqualified")
	}
	return nil
}

func checkEphemeralCoreRoute(pattern []byte, err error, stateDir, executionCgroup string, qualify coreRouterProbe) error {
	if err := ephemeralCorePattern(pattern, err); err != nil {
		return err
	}
	if strings.TrimSpace(string(pattern)) == myceCorePattern {
		// Same budget as the exec-helper capability probe. A legacy handler
		// must not block qualification by waiting on an unrelated crash reporter.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return qualify(ctx, stateDir, executionCgroup)
	}
	return nil
}

func (w Worker) qualifyEphemeralExecution(pattern []byte, readErr error, qualify coreRouterProbe) (ephemeralRefusal, error) {
	if err := ephemeralCorePattern(pattern, readErr); err != nil {
		return ephemeralCoreRouteUnqualified, err
	}
	piped := strings.TrimSpace(string(pattern)) == myceCorePattern
	detached := w.detachesAssignedAttempts()
	refusal := ephemeralCoreRouteUnqualified
	if detached || w.Custodian != "" {
		refusal = ephemeralDetachedUnqualified
	} else if !piped {
		return "", nil
	}
	ceilings := w.executionCeilings()
	if ceilings == nil || signalCgroup(ceilings.root) == "" {
		return refusal, errors.New("ephemeral execution requires delegated attempt cgroups for this launch path")
	}
	execution := signalCgroup(ceilings.root)
	if detached {
		parent := filepath.Dir(execution)
		if w.ServiceConfig.validateWorkerSlice() != nil || filepath.Base(parent) != w.ServiceConfig.workerSlice() {
			return refusal, errors.New("installed worker slice differs from delegated service cgroup")
		}
		// Qualify the same slice systemd-run uses for future attempts, before
		// advertising. The custodian checks its exact scope again before staging.
		execution = filepath.Join(parent, w.ServiceConfig.custodianSlice())
	} else if w.Custodian == "" && ceilings == w.memoryCeilings {
		// Older suppression routers can still qualify direct service children.
		execution = ""
	}
	if !piped {
		return "", nil
	}
	roots, err := w.StateRoots()
	if err != nil {
		return refusal, err
	}
	if err := checkEphemeralCoreRoute(pattern, nil, roots[0], execution, qualify); err != nil {
		return refusal, err
	}
	return "", nil
}
