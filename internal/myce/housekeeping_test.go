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
				switch args[0] {
				case "ps":
					if removed {
						return nil, nil
					}
					return []byte("id"), nil
				case "inspect":
					c := dockerInspection{ID: "id"}
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

func TestHostQueuesForgetReceipts(t *testing.T) {
	dir := t.TempDir()
	dead := HostWorker{Name: "old", State: t.TempDir(), Queue: "http://127.0.0.1:1111", Host: "local", Unit: "myce-worker@gpu1.service"}
	live := HostWorker{Name: "current", State: t.TempDir(), Queue: "http://queue", Host: "local"}
	for name, w := range map[string]HostWorker{"old": dead, "live": live} {
		if err := saveWorkerJSON(dir, "worker-"+name+".json", w); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"enable-pending.json", "disable-receipt.json"} {
		if err := saveWorkerJSON(dir, name, hostReceipt{Generation: 1, EnableGeneration: 2, Workers: []HostWorker{dead, live}}); err != nil {
			t.Fatal(err)
		}
	}
	h := HostControl{Dir: dir, Notify: func(_ context.Context, w HostWorker, enable, cleared bool, reason string) error {
		if w.Queue == dead.Queue {
			t.Fatal("forgotten queue notified")
		}
		return nil
	}}
	if err := h.ForgetQueue(dead.Queue, false, func(w HostWorker) (bool, error) { return w.Queue == live.Queue, nil }); err != nil {
		t.Fatal(err)
	}
	queues, err := h.Queues()
	if err != nil || len(queues) != 1 || queues[0] != live.Queue {
		t.Fatalf("queues %v err %v", queues, err)
	}
	workers, err := h.workers()
	if err != nil || len(workers) != 2 {
		t.Fatal("forgot queue destroyed worker custody")
	}
	if err := h.ReconcileWorkerEnable(context.Background(), dead.Queue, dead.Host); err != nil {
		t.Fatal(err)
	}
	if err := h.ReconcileEnable(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.ForgetQueue(live.Queue, false, func(HostWorker) (bool, error) { return true, nil }); err == nil {
		t.Fatal("forgot configured queue")
	}
	if err := h.ForgetQueue(live.Queue, true, nil); err != nil {
		t.Fatal(err)
	}
	var state forgottenQueues
	b, err := os.ReadFile(filepath.Join(dir, "forgotten-queues.json"))
	if err != nil || json.Unmarshal(b, &state) != nil || state.Generation == 0 {
		t.Fatal("missing atomic generation")
	}
}

func TestDockerAcknowledgementSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s := &workerStore{dir: dir, state: workerState{Identity: "attempt"}}
	st := dockerExecution{Identity: "attempt", Endpoint: "fake", Released: true}
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

func TestForgottenQueueRegistersAgain(t *testing.T) {
	dir := t.TempDir()
	w := Worker{HostStateDir: dir, Machine: Machine{ID: "worker", HostID: "host"}, Client: Client{URL: "http://queue"}, EnvironmentRoot: t.TempDir()}
	s := &workerStore{dir: t.TempDir()}
	if err := w.registerHost(s); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Dir: dir}
	if err := h.ForgetQueue(w.Client.URL, true, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.registerHost(s); err != nil {
		t.Fatal(err)
	}
	queues, err := h.Queues()
	if err != nil || len(queues) != 1 || queues[0] != w.Client.URL {
		t.Fatal("rejoin did not restore registration")
	}
}
