package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"mycelium/internal/myce"
)

// Cancel the signed-release fixture as an ordinary attempt on a private dev
// queue. Checking the SID independently of release also catches escapes the
// worker's ancestry scan can still find and clean up.
func TestFleetUpgradeAttemptCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	root := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{
		Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"),
		Checkpoints: filepath.Join(root, "checkpoints"), Token: "fleet-custody-test",
		Workers: 1, GPU: "fake", MemoryGB: 8, Interval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := d.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}

	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	buildDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	probePath := filepath.Join(root, "stream")
	spec := myce.Spec{Name: "signed-release-fixture", Project: "fleet-test"}
	spec.Needs.Platform.Any = true
	spec.Needs.MemoryGB = 1
	spec.Run.Command = []string{binary, "-test.run=^TestFleetUpgradeDevSignedReleasesEndToEnd$", "-test.timeout=5m"}
	spec.Run.Env = map[string]string{fleetStreamProbeEnv: probePath, "FLEET_UPGRADE_BUILD_DIR": buildDir}
	// The re-executed test uses the existing private TestMain roots and the
	// same offline Go setup for building its two signed images. The Darwin
	// gate overrides the packaged Go toolchain's default C compiler.
	for _, key := range []string{"MYCE_TEST_ROOT", "MYCE_TEST_ISOLATED_HOME", "GOFLAGS", "GOCACHE", "GOMODCACHE", "GOPATH", "GOPROXY", "GOSUMDB", "GOTOOLCHAIN", "GOTELEMETRY", "GOENV", "GOWORK", "GORACE", "CC", "CGO_ENABLED"} {
		if value, ok := os.LookupEnv(key); ok {
			spec.Run.Env[key] = value
		}
	}
	client := d.Client()
	var submitted myce.Job
	if err := client.Call(ctx, "POST", "/api/jobs", spec, &submitted); err != nil {
		t.Fatal(err)
	}
	readJob := func() *myce.Job {
		t.Helper()
		job, err := client.GetJob(ctx, submitted.ID)
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	var probe fleetStreamProbe
	wait(t, ctx, func() bool {
		data, err := os.ReadFile(probePath)
		if errors.Is(err, os.ErrNotExist) {
			if job := readJob(); job.ReleaseConfirmed {
				t.Fatalf("fixture exited before streaming: %s (%s)\n%s", job.State, job.Reason, readLogs(t, ctx, client, job.ID))
			}
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &probe); err != nil {
			t.Fatal(err)
		}
		return true
	})
	if probe.FixturePID <= 1 || probe.HelperPID <= 1 || probe.FixturePID == probe.HelperPID {
		t.Fatalf("invalid fixture/helper identity: %+v", probe)
	}
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pid", probe.HelperPID)
	if err != nil || len(processes) != 1 {
		t.Fatalf("read live helper birth: processes=%d err=%v", len(processes), err)
	}
	birth := processes[0].Proc.P_starttime
	fixtureSID, err := unix.Getsid(probe.FixturePID)
	if err != nil {
		t.Fatal(err)
	}
	fixtureGroup, err := unix.Getpgid(probe.FixturePID)
	if err != nil {
		t.Fatal(err)
	}
	helperSID, err := unix.Getsid(probe.HelperPID)
	if err != nil {
		t.Fatal(err)
	}
	helperGroup, err := unix.Getpgid(probe.HelperPID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("streaming helper PID=%d birth=%d.%06d SID=%d PGID=%d, fixture SID=%d PGID=%d",
		probe.HelperPID, birth.Sec, birth.Usec, helperSID, helperGroup, fixtureSID, fixtureGroup)
	if os.Getenv("MYCE_PROCESS_SESSION") == "" && fixtureSID != fixtureGroup {
		t.Errorf("ordinary attempt SID=%d does not match its PGID=%d", fixtureSID, fixtureGroup)
	}
	if helperSID != fixtureSID || helperGroup == fixtureGroup {
		t.Errorf("helper SID/PGID=%d/%d, want fixture SID %d and separate PGID", helperSID, helperGroup, fixtureSID)
	}
	conn, err := net.DialTimeout("tcp", probe.Address, time.Second)
	if err != nil {
		t.Fatalf("helper listener before cancel: %v", err)
	}
	_ = conn.Close()
	if job := readJob(); job.State != "running" || job.StartedAt == nil || job.ReleaseConfirmed {
		t.Fatalf("attempt was not running at cancellation: state=%s started=%v release=%v", job.State, job.StartedAt, job.ReleaseConfirmed)
	}
	if _, err := client.Cancel(ctx, submitted.ID); err != nil {
		t.Fatal(err)
	}
	var released *myce.Job
	wait(t, ctx, func() bool {
		released = readJob()
		return released.ReleaseConfirmed
	})
	if released.State != "cancelled" || released.Attempt != 1 {
		t.Errorf("unexpected release: state=%s attempt=%d", released.State, released.Attempt)
	}
	// Inspect immediately on the first confirmed release, with no retry that
	// could let a late helper exit make the assertion pass.
	processes, err = unix.SysctlKinfoProcSlice("kern.proc.pid", probe.HelperPID)
	if err != nil {
		t.Fatal(err)
	}
	for _, process := range processes {
		const zombie = 5 // Darwin SZOMB, an exited process awaiting reaping.
		if process.Proc.P_starttime == birth && process.Proc.P_stat != zombie {
			t.Errorf("release confirmed while helper PID %d with its original birth still lives", probe.HelperPID)
		}
	}
	conn, err = net.DialTimeout("tcp", probe.Address, time.Second)
	if conn != nil {
		_ = conn.Close()
	}
	if !errors.Is(err, unix.ECONNREFUSED) {
		t.Errorf("release confirmed without a closed listener: %v", err)
	}
}
