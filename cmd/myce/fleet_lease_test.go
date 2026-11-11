package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Through the real API and CLI: a project declares open production work, a
// planned drain of its only eligible host reports the block, and the drain
// proceeds once the lease closes or expires.
func TestFleetLeaseCLIBlocksDrainOfOnlyEligibleHostUntilClosedOrExpired(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	gpu, other := maintenanceCLIWorker("gpu0"), maintenanceCLIWorker("cpu0")
	gpu.HostID, gpu.GPU, other.HostID = "gpu-host", "cuda", "other-host"
	gpu.CapabilityProfile.Accelerators = []myce.AcceleratorProfile{{Type: "cuda", MemoryBytes: 24 << 30}}
	for _, m := range []myce.Machine{gpu, other} {
		if _, err := currentCLICheckin(q, clock, myce.Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer((myce.API{Queue: q, Token: "test-only"}).Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test-only")
	run := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := fleetCommand(context.Background(), append(args, "--queue", server.URL), &out)
		return out.String(), err
	}
	drain := func(host string) error {
		return hostMaintenanceCommand(context.Background(), []string{"drain", host, "--queue", server.URL}, &bytes.Buffer{})
	}

	if _, err := run("lease", "open", "--project", "render", "--key", "run-1"); err == nil {
		t.Fatal("lease without an expiry accepted")
	}
	out, err := run("lease", "open", "--project", "render", "--key", "run-1", "--for", "1h", "--platform", "cuda", "--memory-gb", "8")
	var lease myce.ProductionLease
	if err != nil || json.Unmarshal([]byte(out), &lease) != nil || lease.Key != "run-1" || len(lease.Stages) != 1 {
		t.Fatal("open", out, err)
	}
	if err := drain("gpu-host"); err == nil || !strings.Contains(err.Error(), "production lease run-1") {
		t.Fatal("drain stranded the lease's only host", err)
	}
	if err := drain("other-host"); err != nil {
		t.Fatal("unrelated host drain refused", err)
	}
	if out, err = run("lease", "list"); err != nil || !strings.Contains(out, `"run-1"`) {
		t.Fatal("list", out, err)
	}
	if _, err = run("lease", "close", "--project", "render", "--key", "run-1"); err != nil {
		t.Fatal(err)
	}
	if err := drain("gpu-host"); err != nil {
		t.Fatal("closed lease still blocks", err)
	}
	if _, err := q.ChangeHostMaintenance("gpu-host", myce.HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}

	// Expiry alone releases the block; no one has to close it.
	if _, err = run("lease", "open", "--project", "render", "--key", "run-2", "--for", "30m", "--platform", "cuda"); err != nil {
		t.Fatal(err)
	}
	if err := drain("gpu-host"); err == nil {
		t.Fatal("open lease did not block")
	}
	clock.advance(30 * time.Minute)
	if err := drain("gpu-host"); err != nil {
		t.Fatal("expired lease still blocks", err)
	}
}
