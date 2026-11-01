package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Only kernel/systemd/Docker observations are synthetic. The drill supplies real
// production spools and SQLite state; this helper never creates drain receipts.
func m4DrillEmptyEvidence(t *testing.T, sources []ServiceConfig) (MachineDrainOptions, func(context.Context, string, ...string) ([]byte, error)) {
	t.Helper()
	root := t.TempDir()
	opts := MachineDrainOptions{Mode: "verified-empty", cgroupRoot: root}
	if err := os.MkdirAll(filepath.Join(root, "user.slice/app.slice"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpu memory pids\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, s := range sources {
		opts.WorkerConfigs = append(opts.WorkerConfigs, s.Config)
	}
	return opts, func(_ context.Context, n string, args ...string) ([]byte, error) {
		if n == "docker" {
			return nil, nil
		}
		if n == "systemctl" && len(args) > 2 && args[0] == "--user" {
			if args[1] == "list-jobs" {
				return nil, nil
			}
			if args[1] == "is-active" {
				return nil, installTestExit(3)
			}
			if args[1] == "show" {
				if args[2] == "myce-queue.service" {
					return []byte("disabled\n"), nil
				}
				if args[2] == "app.slice" {
					return []byte("/user.slice/app.slice\n"), nil
				}
				for _, s := range sources {
					if filepath.Base(s.Service) == args[2] {
						return []byte(fmt.Sprintf("Id=%s\nLoadState=loaded\nActiveState=inactive\nSubState=dead\nMainPID=0\nControlGroup=\nSlice=app.slice\nFragmentPath=%s\n", args[2], s.Service)), nil
					}
				}
			}
		}
		return nil, fmt.Errorf("unexpected drill boundary: %s %v", n, args)
	}
}

func m4EmptyFixture(t *testing.T) (ServiceConfig, *fakeClock, []MachineRename, MachineDrainOptions, func(context.Context, string, ...string) ([]byte, error)) {
	t.Helper()
	c, clock, mapping := m4OfflineQueue(t)
	mapping = mapping[:2] // Unmapped hosts never acquire invented evidence.
	q, err := openConversionQueue(c, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.update(func() error {
		for _, r := range mapping {
			q.s.Machines[r.From].Gone = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q.Close()
	sources, _ := m4Sources(t)
	root := t.TempDir()
	opts := MachineDrainOptions{Mode: "verified-empty", cgroupRoot: root}
	for i, s := range sources {
		if err := PrepareStateRoots(s.State); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.State, "worker.lock"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		state := fmt.Sprintf(`{"schema_version":3,"session":"session-host-gpu%d","fresh":false,"offset":0}`, i)
		if err := os.WriteFile(filepath.Join(s.State, "state.json"), []byte(state), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(s.Service), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(s.Service, []byte(s.ServiceText()), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(s.Config), 0700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(s)
		if err := os.WriteFile(s.Config, data, 0600); err != nil {
			t.Fatal(err)
		}
		opts.WorkerConfigs = append(opts.WorkerConfigs, s.Config)
		cg := filepath.Join(root, "user.slice", "app.slice", filepath.Base(s.Service))
		if err := os.MkdirAll(cg, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cg, "cgroup.procs"), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpu memory pids\n"), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name == "docker" {
			return nil, nil
		}
		if name == "systemctl" && len(args) > 2 && args[0] == "--user" {
			if args[1] == "list-jobs" {
				return nil, nil
			}
			if args[1] == "show" {
				unit := args[2]
				if unit == "myce-queue.service" {
					return []byte("disabled\n"), nil
				}
				if unit == "app.slice" {
					return []byte("/user.slice/app.slice\n"), nil
				}
				for _, s := range sources {
					if filepath.Base(s.Service) == unit {
						return []byte(fmt.Sprintf("Id=%s\nLoadState=loaded\nActiveState=inactive\nSubState=dead\nMainPID=0\nControlGroup=/user.slice/app.slice/%s\nSlice=app.slice\nFragmentPath=%s\n", unit, unit, s.Service)), nil
					}
				}
			}
			if args[1] == "is-active" {
				return nil, installTestExit(3)
			}
		}
		return nil, fmt.Errorf("unexpected boundary: %s %v", name, args)
	}
	return c, clock, mapping, opts, run
}

func TestM4VerifiedEmptyConversionReceipt(t *testing.T) {
	c, clock, mapping, opts, run := m4EmptyFixture(t)
	for _, dry := range []bool{true, false} {
		if err := ConvertQueueInstall(context.Background(), c, mapping, dry, false, clock, run, opts); err != nil {
			t.Fatal(err)
		}
		q, err := openConversionQueue(c, clock)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(q.s.MachineConversion)
		q.Close()
		if dry {
			if string(data) != "null" {
				t.Fatal("preview persisted conversion", string(data))
			}
			continue
		}
		// Read the public receipt format, including zero-valued evidence fields.
		var raw map[string]json.RawMessage
		json.Unmarshal(data, &raw)
		var receipts []map[string]any
		if err := json.Unmarshal(raw["verified_empty_drains"], &receipts); err != nil {
			t.Fatal("missing receipt", err, string(data))
		}
		if len(receipts) != 2 {
			t.Fatal("receipt count", len(receipts))
		}
		for i, r := range receipts {
			if r["machine"] != mapping[i].From || r["session"] != "session-"+mapping[i].From || r["unit_state"] != "inactive" || r["cgroup_empty"] != true || len(fmt.Sprint(r["spool_digest"])) != 64 || r["verified_at"] != clock.Now().Format("2006-01-02T15:04:05Z07:00") {
				t.Fatal("incomplete receipt", r)
			}
			counts, ok := r["queue_counts"].(map[string]any)
			if !ok {
				t.Fatal("missing queue counts", r)
			}
			for _, state := range []string{"queued", "paused", "waiting", "running", "stopping", "preparing", "host_releases", "obligations"} {
				if counts[state] != float64(0) {
					t.Fatal("nonzero receipt", r)
				}
			}
		}
	}
}

func TestM4VerifiedEmptyRefusals(t *testing.T) {
	for _, kind := range []string{"queue", "paused", "waiting", "retained-paused", "retained-waiting", "running", "stopping", "preparing", "obligations", "unit_active", "unit_unreadable", "cgroup_populated", "cgroup_unreadable", "spool_unsettled", "spool_missing", "container_running", "docker_inaccessible"} {
		t.Run(kind, func(t *testing.T) {
			c, clock, mapping, opts, run := m4EmptyFixture(t)
			q, err := openConversionQueue(c, clock)
			if err != nil {
				t.Fatal(err)
			}
			want := "machine_drain_" + kind
			switch kind {
			case "queue", "paused", "waiting", "retained-paused", "retained-waiting", "running", "stopping", "preparing":
				state := strings.TrimPrefix(kind, "retained-")
				if kind == "queue" {
					state = "queued"
				}
				pending := &Job{ID: "pending", State: state}
				if strings.HasPrefix(kind, "retained-") {
					q.s.Submissions = map[string]*submissionRecord{"pending-key": {JobID: pending.ID, Job: pending}}
				} else {
					q.s.Jobs[pending.ID] = pending
				}
				want = "machine_drain_queue_not_empty"
			case "obligations":
				q.s.HostReleases = map[string]HostRelease{"pending": {Host: "host", Machine: "host-gpu0", Session: "session-host-gpu0", Job: "old", Attempt: 1}}
				want = "machine_drain_obligations_pending"
			case "cgroup_populated":
				os.WriteFile(filepath.Join(opts.cgroupRoot, "user.slice/app.slice/myce-worker@gpu0.service/cgroup.procs"), []byte("123\n"), 0600)
			case "cgroup_unreadable":
				os.Remove(filepath.Join(opts.cgroupRoot, "user.slice/app.slice/myce-worker@gpu0.service/cgroup.procs"))
			case "spool_unsettled":
				s, _ := ReadServiceConfig(opts.WorkerConfigs[0])
				os.WriteFile(filepath.Join(s.State, "command.docker.json"), []byte(`{"schema_version":3,"identity":"command","pending":true,"exit_code":0}`), 0600)
			case "spool_missing":
				s, _ := ReadServiceConfig(opts.WorkerConfigs[0])
				os.Remove(filepath.Join(s.State, "state.json"))
				want = "machine_drain_spool_unsettled"
			}
			if err := q.update(func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(q.s)
			q.Close()
			boundary := func(ctx context.Context, n string, args ...string) ([]byte, error) {
				out, err := run(ctx, n, args...)
				if n == "systemctl" {
					if kind == "unit_active" {
						out = []byte(strings.ReplaceAll(string(out), "ActiveState=inactive", "ActiveState=active"))
					}
					if kind == "unit_unreadable" {
						return nil, errors.New("synthetic unreadable manager")
					}
				}
				if n == "docker" {
					if kind == "container_running" {
						return []byte("abc123\n"), nil
					}
					if kind == "docker_inaccessible" {
						return nil, errors.New("synthetic denied socket")
					}
				}
				return out, err
			}
			for _, dry := range []bool{true, false} {
				err := ConvertQueueInstall(context.Background(), c, mapping, dry, false, clock, boundary, opts)
				var typed *HTTPError
				if !errors.As(err, &typed) || typed.Code != want {
					t.Errorf("want %s, got %v", want, err)
				}
			}
			q, err = openConversionQueue(c, clock)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(q.s)
			q.Close()
			if string(before) != string(after) {
				t.Fatal("refusal changed queue state")
			}
		})
	}
}

func TestM4VerifiedEmptyRemainsExplicit(t *testing.T) {
	c, clock, mapping, _, run := m4EmptyFixture(t)
	for _, dry := range []bool{true, false} {
		err := ConvertQueueInstall(context.Background(), c, mapping, dry, false, clock, run)
		var typed *HTTPError
		if !errors.As(err, &typed) || typed.Code != "machine_migration_incomplete" {
			t.Fatal("default source gate changed", err)
		}
	}
}

func TestM4VerifiedEmptyStoppedSourceAndClearedCgroup(t *testing.T) {
	c, clock, mapping, opts, run := m4EmptyFixture(t)
	q, err := openConversionQueue(c, clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range mapping {
		q.s.Machines[r.From].Gone = false
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	q.Close()
	clock.Advance(10 * time.Minute)
	for _, path := range opts.WorkerConfigs {
		s, err := ReadServiceConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		cg := filepath.Join(opts.cgroupRoot, "user.slice/app.slice", filepath.Base(s.Service))
		if err := os.Remove(filepath.Join(cg, "cgroup.procs")); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(cg); err != nil {
			t.Fatal(err)
		}
	}
	boundary := func(ctx context.Context, n string, args ...string) ([]byte, error) {
		out, err := run(ctx, n, args...)
		if n == "systemctl" && len(args) > 2 {
			out = []byte(strings.ReplaceAll(string(out), "ControlGroup=/user.slice/app.slice/"+args[2], "ControlGroup="))
		}
		return out, err
	}
	if err := ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, boundary, opts); err != nil {
		t.Fatal(err)
	}
}

func TestM4VerifiedEmptyLocksSurvivePublication(t *testing.T) {
	c, clock, mapping, opts, run := m4EmptyFixture(t)
	s, err := ReadServiceConfig(opts.WorkerConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(s.State, "worker.lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	err = ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, run, opts)
	var typed *HTTPError
	if !errors.As(err, &typed) || typed.Code != "machine_drain_spool_unsettled" {
		t.Fatal("active spool accepted", err)
	}
	syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	checked := false
	files := installFiles{before: func(op, path string) error {
		if op == "write" && path == c.ProjectConfig {
			checked = true
			if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
				t.Fatal("source lock released before publication")
			}
		}
		return nil
	}}
	if err := convertQueueInstall(context.Background(), c, mapping, false, false, clock, run, files, opts); err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("publication was not exercised")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal("source lock leaked", err)
	}
}

func TestM4VerifiedEmptySameNameRetirement(t *testing.T) {
	c, clock, mapping, opts, run := m4EmptyFixture(t)
	s, err := ReadServiceConfig(opts.WorkerConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	s.Name = "host"
	data, _ := json.Marshal(s)
	if err := os.WriteFile(s.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	q, err := openConversionQueue(c, clock)
	if err != nil {
		t.Fatal(err)
	}
	m := q.s.Machines[mapping[0].From]
	delete(q.s.Machines, mapping[0].From)
	m.ID, m.Gone = "host", false
	q.s.Machines[m.ID] = m
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	q.Close()
	mapping[0].From = "host"
	if err := ConvertQueueInstall(context.Background(), c, mapping, false, false, clock, run, opts); err != nil {
		t.Fatal(err)
	}
	q, err = openConversionQueue(c, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if !q.s.Machines["host"].Gone || q.s.Machines["host"].Session != m.Session {
		t.Fatal("same-name retirement fabricated or retained a live legacy session")
	}
}
