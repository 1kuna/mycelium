package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoppedContainerRetention(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		released, pending, ack bool
		age                    time.Duration
		worker                 string
		running                bool
		fail                   bool
		remove                 bool
	}{
		{name: "expired", released: true, ack: true, age: 24 * time.Hour, worker: "owner", remove: true},
		{name: "opaque", released: true, ack: true, age: 24 * time.Hour, worker: "owner"},
		{name: "young", released: true, ack: true, age: 23 * time.Hour, worker: "owner"},
		{name: "unconfirmed", ack: true, age: 48 * time.Hour, worker: "owner"},
		{name: "pending", released: true, pending: true, ack: true, age: 48 * time.Hour, worker: "owner"},
		{name: "unacknowledged", released: true, age: 48 * time.Hour, worker: "owner"},
		{name: "foreign", released: true, ack: true, age: 48 * time.Hour, worker: "other"},
		{name: "running", released: true, ack: true, age: 48 * time.Hour, worker: "owner", running: true},
		{name: "retry", released: true, ack: true, age: 48 * time.Hour, worker: "owner", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			now := time.Unix(1000000, 0)
			st := dockerExecution{Identity: "attempt", Endpoint: "fake", Released: tc.released, Pending: tc.pending, Stopped: tc.released, Containers: map[string]dockerMember{"side": {}}}
			if tc.name != "opaque" {
				st.Owner = &Result{Machine: "owner", Session: "session", Execution: Execution{ID: "job", Attempt: 1}}
			}
			if err := saveWorkerJSON(dir, "attempt.docker.json", st); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"attempt.cid", "attempt-side.cid", "unrelated.cid", "attempt.log"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.ack {
				if err := saveWorkerJSON(dir, "attempt.docker-ack.json", dockerAcknowledgement{Worker: "owner", At: now.Add(-tc.age)}); err != nil {
					t.Fatal(err)
				}
			}
			removed := false
			call := func(args ...string) ([]byte, error) {
				if tc.name == "opaque" {
					t.Fatal("opaque custody reached daemon")
				}
				switch args[0] {
				case "ps":
					if removed {
						return nil, nil
					}
					return []byte("id"), nil
				case "inspect":
					c := dockerInspection{ID: "id"}
					c.HostConfig.RestartPolicy = &dockerRestartPolicy{Name: "no"}
					c.Config.Labels = map[string]string{"myce.attempt": "attempt", "myce.worker": tc.worker}
					c.State.Running = &tc.running
					c.State.Status = "exited"
					return json.Marshal([]dockerInspection{c})
				case "rm":
					if tc.fail {
						return nil, errors.New("refused")
					}
					if strings.Contains(strings.Join(args, " "), "--force") {
						t.Fatal("forced removal")
					}
					removed = true
					return nil, nil
				}
				t.Fatalf("unexpected Docker operation %v", args)
				return nil, nil
			}
			_ = pruneStoppedDocker(context.Background(), dir, "owner", "", now, 24*time.Hour, func(d *dockerAttempt) func(...string) ([]byte, error) { return call })
			if removed != tc.remove {
				t.Fatalf("removed=%v want=%v", removed, tc.remove)
			}
			for _, name := range []string{"unrelated.cid", "attempt.log"} {
				if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
					t.Fatal("unrelated evidence removed")
				}
			}
			_, err := os.Stat(filepath.Join(dir, "attempt.cid"))
			if (err == nil) == tc.remove {
				t.Fatal("cid retention mismatch")
			}
			if tc.fail {
				tc.fail = false
				if err := pruneStoppedDocker(context.Background(), dir, "owner", "", now, 0, func(d *dockerAttempt) func(...string) ([]byte, error) { return call }); err != nil {
					t.Fatal(err)
				}
				if !removed {
					t.Fatal("did not retry")
				}
			}
		})
	}
}

func TestDockerAcknowledgementSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s := &workerStore{dir: dir, state: workerState{Identity: "attempt"}}
	st := dockerExecution{Owner: &Result{Machine: "owner", Session: "session", Execution: Execution{ID: "job", Attempt: 1}}, Identity: "attempt", Endpoint: "fake", Released: true}
	if err := saveWorkerJSON(dir, "attempt.docker.json", st); err != nil {
		t.Fatal(err)
	}
	first := time.Unix(10000, 0)
	if err := acknowledgeDocker(s, "owner", first); err != nil {
		t.Fatal(err)
	}
	if err := acknowledgeDocker(s, "owner", first.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var ack dockerAcknowledgement
	b, err := os.ReadFile(filepath.Join(dir, "attempt.docker-ack.json"))
	if err != nil || json.Unmarshal(b, &ack) != nil || !ack.At.Equal(first) {
		t.Fatal("replay reset retention clock")
	}
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := pruneStoppedDocker(context.Background(), dir, "owner", "stale-scan-identity", first.Add(time.Hour), 0, func(d *dockerAttempt) func(...string) ([]byte, error) {
		return func(...string) ([]byte, error) { calls++; return nil, nil }
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("background sweep touched current durable attempt")
	}
}
