package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

type retentionDaemon struct {
	t          *testing.T
	containers map[string]dockerInspection
	removed    []string
	removeErr  error
}

func (f *retentionDaemon) caller(d *dockerAttempt) func(...string) ([]byte, error) {
	return func(args ...string) ([]byte, error) {
		if d.state.Endpoint != "fake" {
			f.t.Fatalf("unexpected endpoint %q", d.state.Endpoint)
		}
		switch args[0] {
		case "ps":
			var ids []string
			for id, c := range f.containers {
				matches := true
				for i, arg := range args {
					if arg != "--filter" {
						continue
					}
					key, value, equals := strings.Cut(strings.TrimPrefix(args[i+1], "label="), "=")
					if c.Config.Labels[key] == "" || equals && c.Config.Labels[key] != value {
						matches = false
					}
				}
				if matches {
					ids = append(ids, id)
				}
			}
			slices.Sort(ids)
			return []byte(strings.Join(ids, "\n")), nil
		case "inspect":
			var objects []dockerInspection
			for _, id := range args[1:] {
				objects = append(objects, f.containers[id])
			}
			return json.Marshal(objects)
		case "rm":
			if f.removeErr != nil {
				return nil, f.removeErr
			}
			for _, id := range args[1:] {
				c := f.containers[id]
				if c.Config.Labels["myce.worker"] != "retired" || c.Config.Labels["myce.attempt"] == "" || dockerLive(c) || strings.HasPrefix(c.Name, "/verso-") || c.Name == "/docker-render-1" {
					f.t.Fatalf("unsafe removal: %v", args)
				}
				f.removed = append(f.removed, id)
				delete(f.containers, id)
			}
			return nil, nil
		default:
			f.t.Fatalf("unexpected daemon mutation: %v", args)
			return nil, errors.New("unexpected operation")
		}
	}
}

func retentionConversion(t *testing.T) (ServiceConfig, ServiceConfig) {
	t.Helper()
	root := t.TempDir()
	c := ServiceConfig{Role: "worker", Config: filepath.Join(root, "current.json"), State: filepath.Join(root, "current"), Name: "successor", HostID: "machine", Queue: "fixture-queue", DockerEndpoint: "fake"}
	s := c
	s.Name, s.Config, s.State = "retired", filepath.Join(root, "retired.json"), filepath.Join(root, "retired")
	for _, dir := range []string{c.State, s.State} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveWorkerJSON(root, filepath.Base(c.Config), c); err != nil {
		t.Fatal(err)
	}
	if err := saveMigration(machineJournal(c), workerMigration{Version: 1, Phase: "complete", Config: c, Sources: []ServiceConfig{s}}); err != nil {
		t.Fatal(err)
	}
	return c, s
}

func TestReconcileRetiredDockerCustody(t *testing.T) {
	c, source := retentionConversion(t)
	now := time.Unix(200000, 0)
	clock := &dockerTestClock{now: now}
	f := &retentionDaemon{t: t, containers: map[string]dockerInspection{}}
	for _, attempt := range []string{"settled", "no-ack", "unreleased", "wrong-owner", "recent", "no-evidence", "protected", "protected-render", "active"} {
		code := 143
		st := dockerExecution{Identity: attempt, Endpoint: "fake", Released: true, EvidenceSaved: true, Containers: map[string]dockerMember{attempt: {Name: "main", ExitCode: &code, StderrTail: "private stderr"}}}
		if attempt == "unreleased" {
			st.Released = false
		}
		if err := saveWorkerJSON(source.State, attempt+".docker.json", st); err != nil {
			t.Fatal(err)
		}
		ack := dockerAcknowledgement{Worker: source.Name, At: now.Add(-48 * time.Hour)}
		if attempt == "wrong-owner" {
			ack.Worker = "unrelated"
		}
		if attempt == "recent" {
			ack.At = now.Add(-time.Hour)
		}
		if attempt != "no-ack" {
			if err := saveWorkerJSON(source.State, attempt+".docker-ack.json", ack); err != nil {
				t.Fatal(err)
			}
		}
		if attempt != "no-evidence" {
			if err := saveWorkerJSON(source.State, attempt+".docker-evidence.json", st.Containers); err != nil {
				t.Fatal(err)
			}
		}
		obj := dockerInspection{ID: attempt, Name: "/myce-" + attempt}
		obj.Config.Labels = map[string]string{"myce.worker": source.Name, "myce.attempt": attempt}
		obj.State.Running = new(bool)
		obj.State.Status, obj.State.ExitCode = "exited", code
		obj.HostConfig.RestartPolicy = &dockerRestartPolicy{Name: "no"}
		if attempt == "active" {
			*obj.State.Running = true
			obj.State.Status = "running"
		}
		if attempt == "protected" {
			obj.Name = "/verso-fixture"
		}
		if attempt == "protected-render" {
			obj.Name = "/docker-render-1"
		}
		f.containers[obj.ID] = obj
	}
	for _, id := range []string{"unlabelled", "other-worker"} {
		obj := f.containers["settled"]
		obj.ID, obj.Name = id, "/"+id
		obj.Config.Labels = map[string]string{}
		if id == "other-worker" {
			obj.Config.Labels = map[string]string{"myce.attempt": "settled", "myce.worker": "other"}
		}
		f.containers[id] = obj
	}
	// A failed removal retains its exact custody for a later explicit invocation.
	f.removeErr = errors.New("fake daemon removal failed")
	report, err := reconcileRetiredDocker(context.Background(), c.Config, clock, f.caller)
	if err == nil || len(f.removed) != 0 {
		t.Fatal("removal failure lost", err, report)
	}
	if _, err := os.Stat(filepath.Join(source.State, "settled.docker-ack.json")); err != nil {
		t.Fatal("failed removal lost acknowledgement", err)
	}
	f.removeErr = nil
	report, err = reconcileRetiredDocker(context.Background(), c.Config, clock, f.caller)
	if err == nil || !strings.Contains(err.Error(), "private Docker evidence unavailable") {
		t.Fatal("missing evidence failure not reported", err)
	}
	if !slices.Equal(f.removed, []string{"settled"}) {
		t.Fatalf("retired custody not reconciled: removed=%v report=%+v", f.removed, report)
	}
	if len(report) != 9 {
		t.Errorf("held containers not all reported: %+v", report)
	}
	for _, r := range report {
		if !r.Removed && r.Reason == "" {
			t.Errorf("held container has no reason: %+v", r)
		}
	}
	data, err := os.ReadFile(filepath.Join(source.State, "settled.docker-evidence.json"))
	if err != nil || !strings.Contains(string(data), "private stderr") {
		t.Fatal("private evidence lost", err)
	}
}

func TestReconcileRetiredDockerRequiresProvenConversion(t *testing.T) {
	for _, change := range []string{"incomplete", "different-machine", "successor-spool"} {
		t.Run(change, func(t *testing.T) {
			c, source := retentionConversion(t)
			j := workerMigration{Version: 1, Phase: "complete", Config: c, Sources: []ServiceConfig{source}}
			switch change {
			case "incomplete":
				j.Phase = "prepared"
			case "different-machine":
				j.Sources[0].HostID = "other-machine"
			case "successor-spool":
				j.Sources[0].State = c.State
			}
			if err := saveMigration(machineJournal(c), j); err != nil {
				t.Fatal(err)
			}
			_, err := reconcileRetiredDocker(context.Background(), c.Config, &dockerTestClock{now: time.Unix(200000, 0)}, func(*dockerAttempt) func(...string) ([]byte, error) {
				t.Fatal("unproven conversion contacted Docker")
				return nil
			})
			if err == nil {
				t.Fatal("unproven conversion accepted")
			}
		})
	}
}

func TestReconcileRetiredDockerRequiresOfflineSpool(t *testing.T) {
	c, source := retentionConversion(t)
	lock, err := os.OpenFile(filepath.Join(source.State, "worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	f := &retentionDaemon{t: t, containers: map[string]dockerInspection{}}
	_, err = reconcileRetiredDocker(context.Background(), c.Config, &dockerTestClock{now: time.Unix(200000, 0)}, f.caller)
	if err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatal("active retired spool accepted", err)
	}
}
