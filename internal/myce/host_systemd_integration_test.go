package myce

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Opt-in: creates a transient worker unit only if no real worker unit is loaded.
func TestSystemdUserCgroupIntegration(t *testing.T) {
	if runtime.GOOS != "linux" || os.Getenv("MYCE_SYSTEMD_INTEGRATION") != "1" {
		t.Skip("requires Linux systemd user session and MYCE_SYSTEMD_INTEGRATION=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	probe := LocalMachineProbe()
	b, e := probe.Command(ctx, "systemctl", "--user", "show", "myce-worker.service", "-p", "LoadState", "--value")
	if e != nil || strings.TrimSpace(string(b)) != "not-found" {
		t.Skip("systemd user session unavailable or worker unit already exists")
	}
	cmd := exec.CommandContext(ctx, "systemd-run", "--user", "--unit=myce-worker.service", "--property=KillMode=control-group", "--property=SendSIGKILL=yes", "--property=TimeoutStopSec=1s", "--property=Restart=no", "/bin/sh", "-c", "setsid /bin/sh -c 'trap \"\" TERM; while :; do sleep 1; done' & wait")
	if b, e = cmd.CombinedOutput(); e != nil {
		t.Fatalf("transient unit: %v %s", e, b)
	}
	defer probe.Command(context.Background(), "systemctl", "--user", "stop", "myce-worker.service")
	h := HostControl{Observe: testHostExclusion, Dir: t.TempDir(), Service: &SystemdHostService{Unit: "myce-worker.service", Command: probe.Command}, Timeout: 10 * time.Second}
	r, e := h.Disable(ctx, "integration", time.Second)
	if e != nil || !r.Confirmed {
		t.Fatalf("report %+v: %v", r, e)
	}
}
