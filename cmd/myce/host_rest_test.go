package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestHostRestDevExpectations(t *testing.T) {
	r, c := envelopeFixture(t)
	root := filepath.Dir(c.Config)
	c.HostStateDir = filepath.Join(root, "dev-host")
	d, err := myce.StartDev(myce.DevConfig{HostStateDir: c.HostStateDir, HostID: c.HostID, Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "fixture-token", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	t.Setenv("MYCE_TOKEN", "fixture-token")
	c.Queue, c.Name, c.RequiredWorkers = d.URL, "fake-1", []string{"fake-1"}
	c.Credentials = filepath.Join(root, "fixture-credentials.json")
	if err := os.WriteFile(c.Credentials, []byte(`{"token":"fixture-token"}`), 0600); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(c)
	if err := os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	observe := func(ctx context.Context, v myce.HostInvocation) (myce.HostExclusionReport, error) {
		s, err := myce.ReadHostIntent(v.StateDir)
		return envelopeHardware(ctx, s), err
	}
	invoke := func(flags ...string) (myce.HostEnvelope, error) {
		t.Helper()
		args := append([]string{flags[0], "--local", "--config", c.Config, "--json"}, flags[1:]...)
		var out bytes.Buffer
		err := hostEnvelopeCommandWithObservation(args, myce.MachineProbe{OS: "fixture"}, &out, r, nil, hostMutationExecutor(myce.MachineProbe{OS: "fixture"}).Execute, observe)
		if bytes.Contains(out.Bytes(), []byte("fixture-token")) {
			t.Fatal("credential in envelope")
		}
		var e myce.HostEnvelope
		if decodeErr := json.Unmarshal(out.Bytes(), &e); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if e.HostRestVerdict == nil || e.Verdict != "UNKNOWN" {
			validateEnvelopeOutput(t, out.Bytes())
		}
		return e, err
	}
	wait := func(pred func() bool) {
		t.Helper()
		for !pred() {
			if err := waitPause(ctx, 10*time.Millisecond); err != nil {
				t.Fatal("dev condition deadline", err)
			}
		}
	}
	wait(func() bool {
		e, err := invoke("status", "--expect", "ready", "--verdict")
		return err == nil && e.Verdict == "PASS"
	})
	// Use the production executable for ready too; its ordinary unprivileged GPU
	// observation is UNKNOWN, which must not withhold worker readiness.
	binary := filepath.Join(root, "myce")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	command := exec.CommandContext(ctx, binary, "host", "status", "--local", "--config", c.Config, "--expect", "ready", "--verdict", "--json")
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + root, "MYCE_TOKEN=fixture-token"}
	output, err := command.Output()
	if err != nil {
		t.Fatal("executable ready", err)
	}
	ready := validateEnvelopeOutput(t, output)
	if ready.HostRestVerdict == nil || ready.Verdict != "PASS" {
		t.Fatal(ready)
	}
	// A real plain command runs through the API/SQLite/worker, with a visible
	// effect. Current work does not make Ready require empty cgroups.
	marker := filepath.Join(root, "command-started")
	spec := myce.Spec{Name: "fixture-work", Project: "test", Priority: "experiment"}
	spec.Needs.MemoryGB = 1
	spec.Needs.Platform.Any = true
	spec.Run.Command = []string{"sh", "-c", `test -z "$MYCE_TOKEN" || exit 99; printf started > "$1"; sleep 10`, "fixture", marker}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
	var job myce.Job
	if err := d.Client().Call(ctx, "POST", "/api/jobs", spec, &job); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { _, err := os.Stat(marker); return err == nil })
	active, err := invoke("status", "--expect", "ready", "--verdict")
	if err != nil || active.Verdict != "PASS" || active.Counts.Active == nil || *active.Counts.Active != 1 {
		t.Fatalf("active ready %+v error %v", active, err)
	}
	report, err := (myce.HostControl{Dir: c.HostStateDir, Clock: myce.RealClock{}, FreshClose: true, Service: envelopeService{}, Observe: envelopeHardware}).Disable(ctx, "fixture close", 0)
	if err != nil || !report.Confirmed {
		t.Fatal(report, err)
	}
	var closed myce.HostEnvelope
	wait(func() bool {
		closed, err = invoke("status", "--expect", "closed", "--verdict")
		return err == nil && closed.Verdict == "PASS"
	})
	if closed.QueueReachable == nil || !*closed.QueueReachable || closed.Counts.Unsettled == nil || *closed.Counts.Unsettled != 0 {
		t.Fatal("closed did not settle", closed)
	}
	// Close requeues the interrupted experiment. Cancel it before submitting the
	// separate queued-work case so its count and later effect are unambiguous.
	if err := d.Client().Call(ctx, "POST", "/api/jobs/"+job.ID+"/cancel", nil, nil); err != nil {
		t.Fatal(err)
	}
	// Nonempty fenced work is permitted. It cannot create a late assignment.
	spec.Run.Command = []string{"sh", "-c", `printf late > "$1"`, "fixture", filepath.Join(root, "late")}
	if err := d.Client().Call(ctx, "POST", "/api/jobs", spec, &job); err != nil {
		t.Fatal(err)
	}
	closed, err = invoke("status", "--expect", "closed", "--verdict")
	if err != nil || closed.Verdict != "PASS" || closed.Counts.FencedQueued == nil || *closed.Counts.FencedQueued != 1 || !slices.Contains(closed.Reasons, "queued_work_fenced") {
		t.Fatal(closed, err)
	}
	wrong, err := invoke("status", "--expect", "ready", "--verdict")
	if exitCode(err) == 0 || wrong.Verdict != "FAIL" {
		t.Fatal("closed ready accepted", wrong, err)
	}
	if err := d.Client().Call(ctx, "POST", "/api/jobs/"+job.ID+"/cancel", nil, nil); err != nil {
		t.Fatal(err)
	}
	// Reopen with the real production mutation executor; the worker restarts its
	// registered session and acknowledges the new open revision through the API.
	opened, err := invoke("open", "--expect-revision", strconv.FormatUint(*closed.Desired.Revision, 10))
	if err != nil || opened.Accepted == nil {
		t.Fatal(opened, err)
	}
	wait(func() bool {
		e, err := invoke("status", "--expect", "ready", "--verdict")
		return err == nil && e.Verdict == "PASS"
	})
	if _, err := os.Stat(filepath.Join(root, "late")); !os.IsNotExist(err) {
		t.Fatal("closed queued command executed", err)
	}
}

func TestHostRestCommandErrorsAndDeadline(t *testing.T) {
	r, c := envelopeFixture(t)
	for _, args := range [][]string{{"status", "--verdict"}, {"close", "--verdict"}, {"status", "--expect", "other", "--verdict"}} {
		e, err := invokeEnvelope(t, r, c, args...)
		if exitCode(err) != 2 || e.HostRestVerdict != nil {
			t.Fatal(e, err)
		}
	}
	for _, expect := range []string{"closed", "ready"} {
		t.Run(expect, func(t *testing.T) {
			clock := &waitClock{now: time.Now().UTC()}
			start := clock.Now()
			var out bytes.Buffer
			err := hostEnvelopeCommandWithTiming([]string{"status", "--local", "--config", c.Config, "--expect", expect, "--verdict", "--wait", "--timeout", "80ms", "--json"}, myce.MachineProbe{}, &out, r, nil, nil, nil, clock, func(_ context.Context, d time.Duration) error { clock.advance(d); return nil })
			e := validateEnvelopeOutput(t, out.Bytes())
			if !errors.Is(err, myce.ErrHostDeadline) || e.Verdict == "PASS" || !slices.Contains(e.Reasons, "timeout") || !clock.Now().Equal(start.Add(80*time.Millisecond)) {
				t.Fatal(e, err)
			}
		})
	}
	// Passive diagnostics do not create an operational holds_detail/capdiff/cooldown
	// dependency. The generic fleet status and eligible-host redaction stay intact.
	var out bytes.Buffer
	if err := hostEnvelopeCommand([]string{"status", "--local", "--config", c.Config}, myce.MachineProbe{}, &out, r, nil); err != nil || !strings.Contains(out.String(), "Host closed:") {
		t.Fatal(out.String(), err)
	}
}
