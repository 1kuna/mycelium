package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Drive reconcile synchronously: the clock advances only when the test says so.
func TestDockerStopProtocol(t *testing.T) {
	_, s, c, root := dockerFixture(t)
	for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned", "extra": "yes"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	d := &dockerAttempt{dir: s.dir, binary: filepath.Join(root, "docker"), clock: c, timeout: 2 * time.Second, env: os.Environ(), state: dockerExecution{Identity: "owned", Endpoint: "fake", Container: "id1"}}
	d.env = withoutToken(d.env)
	d.stop(c.Now(), StopGrace, false)
	deadline := *d.state.StopAt
	tick := func() {
		t.Helper()
		if _, err := d.reconcile(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		tick()
	}
	calls := func(signal string) int {
		b, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Count(string(b), "kill --signal="+signal+" ")
	}
	if calls("TERM") != 2 || calls("KILL") != 0 {
		t.Fatalf("signals before deadline: TERM=%d KILL=%d", calls("TERM"), calls("KILL"))
	}
	// Reload the durable record exactly as recovery does.
	b, err := os.ReadFile(filepath.Join(s.dir, "owned.docker.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state dockerExecution
	if err = json.Unmarshal(b, &state); err != nil {
		t.Fatal(err)
	}
	d.state = state
	c.advance(StopGrace - time.Nanosecond)
	tick()
	if calls("TERM") != 2 || calls("KILL") != 0 || !d.state.StopAt.Equal(deadline) {
		t.Fatal("restart changed stop protocol")
	}
	c.advance(time.Nanosecond)
	// Keep containers running to prove deadline KILL is safely repeated.
	for i := 0; i < 3; i++ {
		tick()
		_ = os.WriteFile(filepath.Join(root, "running"), nil, 0600)
		_ = os.WriteFile(filepath.Join(root, "extra"), nil, 0600)
	}
	if calls("TERM") != 2 || calls("KILL") != 6 {
		t.Fatalf("signals at deadline: TERM=%d KILL=%d", calls("TERM"), calls("KILL"))
	}
}

func withoutToken(env []string) []string {
	var out []string
	for _, v := range env {
		if !strings.HasPrefix(v, "MYCE_TOKEN=") {
			out = append(out, v)
		}
	}
	return out
}

func TestDockerSidecarStopRecoveryAndEvidence(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerGroupJob(t)
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	if err = os.Remove(filepath.Join(root, "running")); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		b, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Count(string(b), "kill --signal=TERM sidecar-") == 2
	})
	p.docker.close()
	var before dockerExecution
	b, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker.json"))
	if err != nil || json.Unmarshal(b, &before) != nil {
		t.Fatal("missing custody")
	}
	c.advance(StopGrace / 2)
	restored, err := w.restoreDocker(s)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.docker.close()
	await(t, func() bool {
		b, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Count(string(b), "inspect ") >= 4
	})
	restored.docker.mu.Lock()
	if restored.docker.state.Forced || !restored.docker.state.StopAt.Equal(*before.StopAt) {
		t.Error("restart forced stop or lost deadline")
	}
	restored.docker.mu.Unlock()
	c.advance(StopGrace / 2)
	select {
	case f := <-restored.done:
		if f.result.ReleasePending {
			t.Fatal("release pending")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup stuck")
	}
	b, _ = os.ReadFile(filepath.Join(root, "calls"))
	if strings.Count(string(b), "kill --signal=TERM sidecar-") != 2 || strings.Count(string(b), "kill --signal=KILL sidecar-") != 2 {
		t.Fatal("restart resent signals")
	}
	b, err = os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence map[string]dockerMember
	if err = json.Unmarshal(b, &evidence); err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 3 {
		t.Fatal("missing container evidence")
	}
	for _, m := range evidence {
		if m.ExitCode == nil || m.StderrTail != "payload stderr\n" {
			t.Fatalf("missing stderr/exit for %s", m.Name)
		}
	}
	info, _ := os.Stat(filepath.Join(s.dir, s.state.Identity+".docker-evidence.json"))
	if info.Mode().Perm() != 0600 {
		t.Fatal("evidence is not private")
	}
	log, _ := os.ReadFile(filepath.Join(s.dir, s.state.Log))
	if strings.Count(string(log), "payload stderr") != 1 {
		t.Fatal("sidecar private stderr entered uploaded main log")
	}
}

func TestDockerStderrByteBound(t *testing.T) {
	b := &dockerTail{}
	_, _ = b.Write([]byte(strings.Repeat("x", dockerStderrTailBytes*2)))
	_, _ = b.Write([]byte("final stderr\n"))
	if len(b.data) != dockerStderrTailBytes || !strings.HasSuffix(string(b.data), "final stderr\n") {
		t.Fatal("stderr byte tail is not bounded")
	}
}

func TestDockerSidecarEvidenceSurvivesRetention(t *testing.T) {
	for _, mode := range []string{"captured", "missing", "mismatch"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			now := time.Unix(10000, 0)
			code := 143
			st := dockerExecution{Owner: &Result{Machine: "worker", Session: "session", Execution: Execution{ID: "job", Attempt: 1}}, Identity: "owned", Endpoint: "fake", Released: true, EvidenceSaved: true, Containers: map[string]dockerMember{"sidecar": {Name: "server", ExitCode: &code, StderrTail: "private fixture stderr"}}}
			if err := saveWorkerJSON(dir, "owned.docker.json", st); err != nil {
				t.Fatal(err)
			}
			if err := saveWorkerJSON(dir, "owned.docker-ack.json", dockerAcknowledgement{Worker: "worker", At: now}); err != nil {
				t.Fatal(err)
			}
			if mode != "missing" {
				if err := saveWorkerJSON(dir, "owned.docker-evidence.json", st.Containers); err != nil {
					t.Fatal(err)
				}
			}
			removed := false
			call := func(args ...string) ([]byte, error) {
				switch args[0] {
				case "ps":
					return []byte("sidecar"), nil
				case "inspect":
					running := false
					obj := dockerInspection{ID: "sidecar"}
					obj.HostConfig.RestartPolicy = &dockerRestartPolicy{Name: "no"}
					obj.Config.Labels = map[string]string{"myce.attempt": "owned", "myce.worker": "worker"}
					obj.State.Running = &running
					obj.State.Status = "exited"
					obj.State.ExitCode = 143
					if mode == "mismatch" {
						obj.State.ExitCode = 134
					}
					return json.Marshal([]dockerInspection{obj})
				case "rm":
					removed = true
					return nil, nil
				default:
					t.Fatalf("unexpected operation %v", args)
					return nil, nil
				}
			}
			err := pruneStoppedDocker(context.Background(), dir, "worker", "", now, 0, func(*dockerAttempt) func(...string) ([]byte, error) { return call })
			if mode == "captured" {
				if err != nil || !removed {
					t.Fatal("captured container not pruned", err)
				}
				b, err := os.ReadFile(filepath.Join(dir, "owned.docker-evidence.json"))
				if err != nil || !strings.Contains(string(b), "private fixture stderr") {
					t.Fatal("private evidence lost during disposal")
				}
			} else if err == nil || removed {
				t.Fatal("container removed without matching evidence")
			}
		})
	}
}

// Synchronous fake Docker/clock/store fixture, without background reconcile ticks.
func stopAttempt(t *testing.T) (*dockerAttempt, *dockerTestClock, string) {
	t.Helper()
	_, s, c, root := dockerFixture(t)
	for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	d := &dockerAttempt{dir: s.dir, binary: filepath.Join(root, "docker"), clock: c, timeout: 2 * time.Second, env: withoutToken(os.Environ()), state: dockerExecution{Identity: "owned", Endpoint: "fake", Container: "id1"}}
	d.stop(c.Now(), StopGrace, false)
	return d, c, root
}

func reloadStopAttempt(t *testing.T, d *dockerAttempt) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(d.dir, "owned.docker.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &d.state); err != nil {
		t.Fatal(err)
	}
	d.dirty = false
}

func stopCalls(t *testing.T, root, signal string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "calls"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(data), "kill --signal="+signal+" id1")
}

func TestDockerTermAtMostOnce(t *testing.T) {
	for _, mode := range []string{"", "refuse-kill", "unknown-kill", "delivered-error", "crash-before-call", "crash-after-call"} {
		t.Run(mode, func(t *testing.T) {
			d, c, root := stopAttempt(t)
			deadline := *d.state.StopAt
			if err := os.WriteFile(filepath.Join(root, "mode"), []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
			crashed := strings.HasPrefix(mode, "crash-")
			if mode == "crash-before-call" {
				// Exact durable state at the persist-before-delivery crash boundary.
				d.state.Containers = map[string]dockerMember{"id1": {Name: "main", TermDispatched: true, TermOutcome: "unknown-after-restart"}}
				if err := d.saveLocked(); err != nil {
					t.Fatal(err)
				}
			} else {
				if mode == "crash-after-call" {
					d.persist = func(st dockerExecution) error {
						if st.Containers["id1"].TermOutcome == "sent" {
							return errors.New("crash before outcome persistence")
						}
						return saveWorkerJSON(d.dir, st.Identity+".docker.json", st)
					}
				}
				_, err := d.reconcile()
				if (err != nil) != (mode != "") {
					t.Fatalf("first reconcile error=%v", err)
				}
			}
			d.persist = nil
			// New process state: do not overlay the old map/outcome during unmarshal.
			d.state = dockerExecution{}
			reloadStopAttempt(t, d)
			wantOutcome := "error"
			if mode == "" {
				wantOutcome = "sent"
			}
			if crashed {
				wantOutcome = "unknown-after-restart"
			}
			if got := d.state.Containers["id1"].TermOutcome; got != wantOutcome {
				t.Fatalf("outcome=%q want %q", got, wantOutcome)
			}
			// Daemon recovery never permits a second TERM, across healthy ticks/reload.
			if err := os.WriteFile(filepath.Join(root, "mode"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if _, err := d.reconcile(); err != nil {
					t.Fatal(err)
				}
			}
			c.advance(StopGrace - time.Nanosecond)
			if _, err := d.reconcile(); err != nil {
				t.Fatal(err)
			}
			wantTerm := 1
			if mode == "crash-before-call" {
				wantTerm = 0
			}
			if stopCalls(t, root, "TERM") != wantTerm || stopCalls(t, root, "KILL") != 0 || !d.state.StopAt.Equal(deadline) {
				t.Fatal("TERM repeated or KILL early/deadline renewed")
			}
			if mode == "delivered-error" {
				data, err := os.ReadFile(filepath.Join(root, "delivered"))
				if err != nil || strings.Count(string(data), "--signal=TERM id1") != 1 {
					t.Fatal("fixture did not deliver TERM", err)
				}
			}
			c.advance(time.Nanosecond)
			if _, err := d.reconcile(); err != nil {
				t.Fatal(err)
			}
			if done, err := d.reconcile(); err != nil || !done {
				t.Fatal("deadline cleanup failed", err)
			}
			data, err := os.ReadFile(filepath.Join(d.dir, "owned.docker-evidence.json"))
			if err != nil {
				t.Fatal(err)
			}
			var evidence map[string]dockerMember
			if err := json.Unmarshal(data, &evidence); err != nil {
				t.Fatal(err)
			}
			if evidence["id1"].TermOutcome != wantOutcome || evidence["id1"].ExitCode == nil || evidence["id1"].StderrTail != "payload stderr\n" {
				t.Fatal("outcome/exit/stderr custody lost")
			}
		})
	}
}

func TestDockerSignalPersistenceFailureRetry(t *testing.T) {
	for _, recovery := range []string{"live", "restart", "flush-then-restart", "deadline-then-restart"} {
		t.Run(recovery, func(t *testing.T) {
			d, c, root := stopAttempt(t)
			deadline := *d.state.StopAt
			failures := 0
			d.persist = func(st dockerExecution) error {
				if st.Containers["id1"].TermDispatched {
					failures++
					return errors.New("injected intent write failure")
				}
				return saveWorkerJSON(d.dir, st.Identity+".docker.json", st)
			}
			for i := 0; i < 2; i++ {
				if done, err := d.reconcile(); err == nil || done {
					t.Fatal("failed store did not retain custody")
				}
			}
			if failures != 2 || d.state.Containers["id1"].TermDispatched || stopCalls(t, root, "TERM") != 0 {
				t.Fatal("failed intent consumed/sent TERM")
			}
			d.persist = nil
			if recovery == "flush-then-restart" {
				if err := d.saveLocked(); err != nil {
					t.Fatal(err)
				}
			}
			if recovery == "deadline-then-restart" {
				c.advance(StopGrace)
			}
			if recovery != "live" {
				d.state = dockerExecution{}
				reloadStopAttempt(t, d)
			}
			if _, err := d.reconcile(); err != nil {
				t.Fatal(err)
			}
			wantTerm := 1
			if recovery == "deadline-then-restart" {
				wantTerm = 0
			} else {
				if stopCalls(t, root, "TERM") != 1 || stopCalls(t, root, "KILL") != 0 {
					t.Fatal("persistence recovery failed or KILL early")
				}
				c.advance(StopGrace)
				if _, err := d.reconcile(); err != nil {
					t.Fatal(err)
				}
			}
			if done, err := d.reconcile(); err != nil || !done {
				t.Fatal("cleanup failed", err)
			}
			if !d.state.StopAt.Equal(deadline) || stopCalls(t, root, "TERM") != wantTerm || stopCalls(t, root, "KILL") != 1 {
				t.Fatal("recovery changed deadline/signals")
			}
		})
	}
}

func TestDockerDeadlineKillDespitePersistenceFailure(t *testing.T) {
	for _, failStop := range []bool{false, true} {
		t.Run(fmt.Sprintf("stop-write-fails=%v", failStop), func(t *testing.T) {
			d, c, root := stopAttempt(t)
			if failStop {
				d.state.Stopped = false
				d.state.StopAt = nil
			}
			d.persist = func(dockerExecution) error { return errors.New("spool unavailable") }
			if failStop {
				d.stop(c.Now(), StopGrace, false)
			}
			for i := 0; i < 2; i++ {
				if done, err := d.reconcile(); err == nil || done {
					t.Fatal("storage failure released custody")
				}
			}
			c.advance(StopGrace - time.Nanosecond)
			_, _ = d.reconcile()
			if stopCalls(t, root, "TERM") != 0 || stopCalls(t, root, "KILL") != 0 {
				t.Fatal("signal before persistence/deadline")
			}
			c.advance(time.Nanosecond)
			if _, err := d.reconcile(); err != nil {
				t.Fatal(err)
			}
			if stopCalls(t, root, "KILL") != 1 {
				t.Fatal("storage outage suppressed deadline KILL")
			}
			if done, err := d.reconcile(); err == nil || done {
				t.Fatal("storage outage lost evidence custody")
			}
			d.persist = nil
			if done, err := d.reconcile(); err != nil || !done {
				t.Fatal("storage recovery did not capture/release", err)
			}
		})
	}
}

func TestDockerDeadlineKillRetry(t *testing.T) {
	for _, mode := range []string{"refuse-kill", "unknown-kill", "delivered-error"} {
		t.Run(mode, func(t *testing.T) {
			d, c, root := stopAttempt(t)
			if _, err := d.reconcile(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "mode"), []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
			c.advance(StopGrace - time.Nanosecond)
			if _, err := d.reconcile(); err != nil {
				t.Fatal(err)
			}
			if stopCalls(t, root, "KILL") != 0 {
				t.Fatal("premature KILL")
			}
			c.advance(time.Nanosecond)
			for i := 0; i < 2; i++ {
				if done, err := d.reconcile(); err == nil || done {
					t.Fatal("failed KILL released custody")
				}
				d.state = dockerExecution{}
				reloadStopAttempt(t, d)
			}
			if stopCalls(t, root, "TERM") != 1 || stopCalls(t, root, "KILL") != 2 {
				t.Fatal("KILL not retried across restart")
			}
			if err := os.WriteFile(filepath.Join(root, "mode"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := d.reconcile(); err != nil {
				t.Fatal(err)
			}
			if done, err := d.reconcile(); err != nil || !done {
				t.Fatal("KILL retry did not reclaim", err)
			}
			if stopCalls(t, root, "KILL") != 3 {
				t.Fatal("missing final retry")
			}
		})
	}
}
