package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeHostService struct {
	left  bool
	now   atomic.Bool
	grace time.Duration
	hang  bool
}

func (s *fakeHostService) Stop(ctx context.Context, grace time.Duration) error {
	s.now.Store(grace == 0)
	s.grace = grace
	if s.hang {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}
func (s *fakeHostService) Empty(context.Context) (bool, error) { return !s.left, nil }
func TestDisableSystemdCgroup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		left, hang bool
		grace      time.Duration
	}{{"empty", false, false, time.Second}, {"left", true, false, 0}, {"now", false, false, 0}, {"hang", false, true, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeHostService{left: tc.left, hang: tc.hang}
			dir := t.TempDir()
			h := HostControl{Dir: dir, Service: s, Timeout: time.Second, Processes: HostProcesses{Owned: func(workerState) (bool, error) { t.Fatal("systemd used PID registry"); return false, nil }}}
			start := time.Now()
			ensureTestHost(t, h.Dir, h.Clock)
			r, err := h.Disable(context.Background(), "test", tc.grace)
			if (err != nil) != (tc.left || tc.hang) {
				t.Fatalf("report %+v error %v", r, err)
			}
			if tc.hang && time.Since(start) > 2*time.Second {
				t.Fatal("overall deadline ignored")
			}
			if _, e := os.Stat(filepath.Join(dir, hostStateFile)); e != nil {
				t.Fatal(e)
			}
			if !tc.left && !tc.hang && !r.Confirmed {
				t.Fatal("empty cgroup not confirmed")
			}
			if s.now.Load() != (tc.grace == 0) {
				t.Fatal("now lost")
			}
		})
	}
}
func TestDisableEmptyRegistryIsUnconfirmed(t *testing.T) {
	h := HostControl{Dir: t.TempDir()}
	ensureTestHost(t, h.Dir, h.Clock)
	r, err := h.Disable(context.Background(), "test", 0)
	if err == nil || r.Confirmed {
		t.Fatalf("empty registry certified %+v %v", r, err)
	}
}
func TestWorkerUnitCgroupSettings(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}}
	texts := []string{c.ServiceText()}
	for _, p := range []string{"../../deploy/myce-worker.service", "../../deploy/myce-worker@.service", "../../deploy/myce-spark-worker.service"} {
		b, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		texts = append(texts, string(b))
	}
	for _, text := range texts {
		for _, setting := range []string{"Delegate=memory", "KillMode=control-group", "SendSIGKILL=yes", "TimeoutStopSec=10s", "_host-start-check", "Restart=on-failure"} {
			if !strings.Contains(text, setting) {
				t.Errorf("missing %s", setting)
			}
		}
	}
}
func TestHostControlGenerationOrdering(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	j := submit(t, q, spec("experiment"))
	check(t, q, m, nil)
	if e := q.DisableHostGeneration("host", "test", false, nil, 3); e != nil {
		t.Fatal(e)
	}
	if e := q.EnableHostGeneration("host", 2); e == nil {
		t.Fatal("stale enable accepted")
	}
	if e := q.DisableHostGeneration("host", "test", true, nil, 3); e != nil {
		t.Fatal(e)
	}
	if job(t, q, j.ID).State != "queued" {
		t.Fatal("not requeued")
	}
	if e := q.DisableHostGeneration("host", "test", true, nil, 3); e != nil {
		t.Fatal("ack retry", e)
	}
	if e := q.DisableHostGeneration("host", "late pending", false, nil, 3); e != nil {
		t.Fatal("pending retry", e)
	}
	if !q.s.HostControls["host"].Cleared {
		t.Fatal("late pending undid acknowledgement")
	}

	if e := q.EnableHostGeneration("host", 4); e != nil {
		t.Fatal(e)
	}
	check(t, q, m, nil)
	before := job(t, q, j.ID)
	if e := q.DisableHostGeneration("host", "replay", true, nil, 3); e == nil {
		t.Fatal("old disable accepted")
	}
	after := job(t, q, j.ID)
	if before.Attempt != after.Attempt || after.State != "running" {
		t.Fatal("replay requeued new attempt")
	}
	if e := q.EnableHostGeneration("host", 4); e != nil {
		t.Fatal("enable retry", e)
	}
	if e := q.DisableHostGeneration("host", "conflict", true, nil, 4); e == nil {
		t.Fatal("same generation changed action")
	}
}

func TestSystemdProtocolAndDescendantPopulation(t *testing.T) {
	for _, grace := range []time.Duration{0, 3 * time.Second} {
		t.Run(grace.String(), func(t *testing.T) {
			stopped := false
			populated := "1"
			var calls []string
			s := &SystemdHostService{Unit: "myce-worker.service", Command: func(ctx context.Context, n string, args ...string) ([]byte, error) {
				call := strings.Join(args, " ")
				calls = append(calls, call)
				if len(args) > 1 && args[1] == "show" {
					state := "active"
					if stopped {
						state = "inactive"
					}
					return []byte("LoadState=loaded\nKillMode=control-group\nSendSIGKILL=yes\nActiveState=" + state + "\nControlGroup=/user.slice/myce-worker.service\n"), nil
				}
				if strings.Contains(call, "stop --no-block") {
					stopped = true
				}
				return nil, nil
			}, ReadFile: func(p string) ([]byte, error) {
				if !strings.HasPrefix(p, "/sys/fs/cgroup/user.slice/myce-worker.service/") {
					t.Fatal(p)
				}
				if filepath.Base(p) == "cgroup.events" {
					return []byte("populated " + populated + "\n"), nil
				}
				return nil, nil
			}}
			if e := s.Stop(context.Background(), grace); e != nil {
				t.Fatal(e)
			}
			empty, e := s.Empty(context.Background())
			if e != nil || empty {
				t.Fatal("empty parent hid descendant", e)
			}
			populated = "0"
			empty, e = s.Empty(context.Background())
			if e != nil || !empty {
				t.Fatal("empty cgroup rejected", e)
			}
			joined := strings.Join(calls, "\n")
			if grace == 0 {
				if !strings.Contains(joined, "kill --kill-whom=all --signal=SIGKILL myce-worker.service") {
					t.Fatal(joined)
				}
			} else if strings.Contains(joined, "set-property") || !strings.Contains(joined, "stop --no-block myce-worker.service") {
				t.Fatal(joined)
			}
		})
	}
}
func TestSystemdRejectsUnsafeGroupAndUnit(t *testing.T) {
	s := &SystemdHostService{Unit: "verso.service"}
	if e := s.Stop(context.Background(), 0); e == nil {
		t.Fatal("unrelated service accepted")
	}
	s.Unit = "myce-worker.service"
	for _, g := range []string{"/", "/user.slice/../myce-worker.service", "/user.slice/verso.service"} {
		s.group = g
		if _, e := s.cgroupPath(); e == nil {
			t.Fatal("unsafe group", g)
		}
	}
}
func TestDockerConfirmationHookKeepsFlag(t *testing.T) {
	dir := t.TempDir()
	h := HostControl{Dir: dir, Service: &fakeHostService{}, ConfirmContainers: func(context.Context) error { return os.ErrPermission }}
	ensureTestHost(t, h.Dir, h.Clock)
	r, e := h.Disable(context.Background(), "test", 0)
	if e == nil || len(r.Unconfirmed) == 0 {
		t.Fatal(r, e)
	}

}

func TestSystemdRegistryFailureDoesNotGateStop(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "worker-broken.json"), []byte("broken"), 0600)
	s := &fakeHostService{}
	r, e := (HostControl{Dir: readyTestHost(t, dir), Service: s}).Disable(context.Background(), "test", 0)
	if e != nil || !r.Confirmed || len(r.QueueErrors) == 0 {
		t.Fatal(r, e)
	}
}
func TestHostGenerationPersistsAcrossQueueRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.db")
	clock := &fakeClock{now: time.Unix(1000, 0)}
	q, e := Open(path, dir, clock)
	if e != nil {
		t.Fatal(e)
	}
	if e = q.DisableHostGeneration("host", "test", true, nil, 5); e != nil {
		t.Fatal(e)
	}
	if e = q.Close(); e != nil {
		t.Fatal(e)
	}
	q, e = Open(path, dir, clock)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	if e = q.EnableHostGeneration("host", 4); e == nil {
		t.Fatal("restart lost ordering")
	}
	if e = q.EnableHostGeneration("host", 6); e != nil {
		t.Fatal(e)
	}
	if e = q.DisableHostGeneration("host", "legacy", true, nil, 0); e == nil {
		t.Fatal("unordered legacy request bypassed generation")
	}
}
func TestSystemdHangingCommandDeadline(t *testing.T) {
	dir := t.TempDir()
	s := &SystemdHostService{Unit: "myce-worker.service", Command: func(ctx context.Context, _ string, _ ...string) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }}
	start := time.Now()
	r, e := (HostControl{Dir: readyTestHost(t, dir), Service: s, Timeout: time.Second}).Disable(context.Background(), "test", 0)
	if e == nil || time.Since(start) > 2*time.Second || len(r.Unconfirmed) == 0 {
		t.Fatal(r, e)
	}
	if reason, e := HostDisabled(dir); e != nil || reason == "" {
		t.Fatal("missing deadline fence", e)
	}
}
