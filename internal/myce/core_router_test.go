package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestCoreRouterQualification(t *testing.T) {
	for _, pattern := range []string{"", "|/usr/share/apport/apport -P%P", "|/usr/lib/systemd/systemd-coredump", "|/tmp/myce-core-router", myceCorePattern + " extra", "|/usr/local/libexec/myce-core-router"} {
		if err := checkEphemeralCoreRoute([]byte(pattern), nil, "state", func(context.Context, string) error {
			t.Fatal("untrusted handler executed")
			return nil
		}); err == nil {
			t.Fatal("unsupported route accepted", pattern)
		}
	}
	if ephemeralCorePattern(nil, os.ErrPermission) == nil {
		t.Fatal("unreadable route accepted")
	}
	for _, pattern := range []string{"core", "core.%p\n"} {
		if err := checkEphemeralCoreRoute([]byte(pattern), nil, "state", func(context.Context, string) error {
			t.Fatal("file route needs no handler qualification")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, qualifyErr := range []error{nil, errors.New("legacy router"), context.DeadlineExceeded} {
		called := false
		err := checkEphemeralCoreRoute([]byte(myceCorePattern+"\n"), nil, "exact-state", func(ctx context.Context, state string) error {
			called = true
			if _, ok := ctx.Deadline(); !ok || state != "exact-state" {
				t.Fatal("qualification lost timeout or worker identity")
			}
			return qualifyErr
		})
		if !called || !errors.Is(err, qualifyErr) {
			t.Fatal("router accepted without suppression qualification", called, err)
		}
	}
}

func TestCoreRouterDeploy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "python3", "../../deploy/test_core_router.py")
	cmd.Env = withoutMyceToken(os.Environ())
	cmd.Env = append(cmd.Env, "PYTHONDONTWRITEBYTECODE=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("deployed router behavior: %v\n%s", err, out)
	}
}

func TestCoreRouterSuppressionEvidence(t *testing.T) {
	at := time.Unix(1800000000, 0)
	attempt := "attempt-" + strings.Repeat("a", 32)
	cgroup := "/service/" + attempt + "/main"
	fields := map[string]string{"COREDUMP_PID": "42", "COREDUMP_SIGNAL": "11", "COREDUMP_TIMESTAMP": "1800000000000000", "COREDUMP_CGROUP": cgroup, "MYCE_ATTEMPT": attempt, "MYCE_CORE_CAPTURE": "suppressed_ephemeral"}
	data, _ := json.Marshal(fields)
	for _, request := range []SignalCapture{
		{PID: 42, StartedAt: at, ObservedAt: at.Add(time.Second)},
		{Cgroup: cgroup, StartedAt: at, ObservedAt: at.Add(time.Second)},
	} {
		e := &SignalEvidence{}
		collectSignalJournal(context.Background(), e, request, func(_ context.Context, args ...string) ([]byte, bool, error) {
			if slices.Contains(args, "-k") {
				return nil, false, nil
			}
			if !strings.Contains(strings.Join(args, " "), "MYCE_CORE_CAPTURE,MYCE_ATTEMPT") {
				t.Fatal("query omits router metadata")
			}
			return data, false, nil
		})
		if e.CoreCapture != "suppressed_ephemeral" || e.Signal != "SIGSEGV" || e.PID != 42 || e.CorePointer != "" || e.CoreBytes != 0 || len(e.Libraries) != 0 {
			t.Fatal("suppression evidence lost or includes core content", e)
		}
	}
	for _, request := range []SignalCapture{
		{PID: 43, StartedAt: at, ObservedAt: at.Add(time.Second)},
		{Cgroup: "/other/" + attempt, StartedAt: at, ObservedAt: at.Add(time.Second)},
		{PID: 42, StartedAt: at.Add(time.Second), ObservedAt: at.Add(2 * time.Second)},
	} {
		e := &SignalEvidence{}
		applyCoreMetadataFor(e, data, request)
		if e.CoreCapture == "suppressed_ephemeral" {
			t.Fatal("stale or foreign attempt evidence accepted", request)
		}
	}
}
