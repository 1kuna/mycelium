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
	// Keep the fake containers alive after KILL to prove it is latched too.
	for i := 0; i < 3; i++ {
		tick()
		_ = os.WriteFile(filepath.Join(root, "running"), nil, 0600)
		_ = os.WriteFile(filepath.Join(root, "extra"), nil, 0600)
	}
	if calls("TERM") != 2 || calls("KILL") != 2 {
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
			st := dockerExecution{Identity: "owned", Endpoint: "fake", Released: true, EvidenceSaved: true, Containers: map[string]dockerMember{"sidecar": {Name: "server", ExitCode: &code, StderrTail: "private fixture stderr"}}}
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

// Fail only a pre-dispatch write; all earlier stop/deadline writes reach disk.
type dockerFaultStore struct {
	dir        string
	kill, fail bool
	failures   int
}

func (s *dockerFaultStore) save(state dockerExecution) error {
	m := state.Containers["id1"]
	if s.fail && ((!s.kill && m.TermDispatched) || (s.kill && m.KillDispatched)) {
		s.failures++
		return errors.New("injected custody persistence failure")
	}
	return saveWorkerJSON(s.dir, state.Identity+".docker.json", state)
}

func TestDockerSignalPersistenceFailureRetry(t *testing.T) {
	for _, kill := range []bool{false, true} {
		for _, recovery := range []string{"live", "restart", "flush-then-restart", "deadline-then-restart"} {
			t.Run(fmt.Sprintf("kill=%v/%s", kill, recovery), func(t *testing.T) {
				_, s, c, root := dockerFixture(t)
				for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned"} {
					if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
						t.Fatal(err)
					}
				}
				prior := dockerMember{Name: "main", Started: true, TermDispatched: kill}
				store := &dockerFaultStore{dir: s.dir, kill: kill, fail: true}
				d := &dockerAttempt{persist: store.save, dir: s.dir, binary: filepath.Join(root, "docker"), clock: c, timeout: 2 * time.Second, env: withoutToken(os.Environ()), state: dockerExecution{Identity: "owned", Endpoint: "fake", Container: "id1", Containers: map[string]dockerMember{"id1": prior}}}
				d.stop(c.Now(), StopGrace, false)
				deadline := *d.state.StopAt
				if kill {
					c.advance(StopGrace)
				}
				for i := 0; i < 2; i++ {
					if released, err := d.reconcile(); err == nil || released {
						t.Fatal("failed store did not retain custody", err)
					}
				}
				if store.failures != 2 {
					t.Fatalf("faults=%d, want 2", store.failures)
				}
				calls := func(signal string) int {
					data, _ := os.ReadFile(filepath.Join(root, "calls"))
					return strings.Count(string(data), "kill --signal="+signal+" id1")
				}
				if calls("TERM") != 0 || calls("KILL") != 0 {
					t.Fatal("dispatched before persistence")
				}
				if d.state.Containers["id1"] != prior {
					t.Fatal("failed save consumed signal or changed member")
				}
				store.fail = false
				deadlinePassed := recovery == "deadline-then-restart"
				if deadlinePassed && !kill {
					c.advance(StopGrace)
				}
				if recovery == "flush-then-restart" {
					// Reproduce the next tick's dirty-state flush without dispatch.
					if err := d.saveLocked(); err != nil {
						t.Fatal(err)
					}
				}
				if recovery != "live" {
					data, err := os.ReadFile(filepath.Join(s.dir, "owned.docker.json"))
					if err != nil {
						t.Fatal(err)
					}
					var state dockerExecution
					if err := json.Unmarshal(data, &state); err != nil {
						t.Fatal(err)
					}
					d = &dockerAttempt{dir: s.dir, binary: d.binary, clock: c, timeout: d.timeout, env: d.env, state: state}
				}
				tick := func() bool {
					released, err := d.reconcile()
					if err != nil {
						t.Fatal(err)
					}
					return released
				}
				tick()
				if !d.state.StopAt.Equal(deadline) {
					t.Fatal("recovery renewed deadline")
				}
				if !kill && !deadlinePassed {
					tick()
					if calls("TERM") != 1 || calls("KILL") != 0 {
						t.Fatal("TERM retry missing, repeated, or escalated early")
					}
					c.advance(StopGrace)
					tick()
				}
				if !tick() || !d.state.Released {
					t.Fatal("custody stuck after storage recovery")
				}
				wantTerm := 1
				if kill || deadlinePassed {
					wantTerm = 0
				}
				if calls("TERM") != wantTerm || calls("KILL") != 1 {
					t.Fatalf("TERM=%d KILL=%d", calls("TERM"), calls("KILL"))
				}
			})
		}
	}
}
