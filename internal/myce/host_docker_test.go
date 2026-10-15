package myce

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDisableDocker(t *testing.T) {
	for _, mode := range []string{"term", "ignore-term", "hang", "refuse-kill", "unlabeled", "late", "no-docker", "unknown-create", "no-docker-custody", "now", "native-grace", "orphan", "hang-inspect"} {
		t.Run(mode, func(t *testing.T) {
			w, s, clock, root := dockerFixture(t)
			for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned", "mode": mode} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "refuse-kill" || mode == "ignore-term" || mode == "hang-inspect" {
				if err := os.WriteFile(filepath.Join(root, "extra"), []byte("yes"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			st := dockerExecution{Identity: "owned", Endpoint: "unix:///var/run/docker.sock", Container: "id1", Operation: "start"}
			if mode == "late" {
				if err := os.Rename(filepath.Join(root, "running"), filepath.Join(root, "waiting")); err != nil {
					t.Fatal(err)
				}
				st.Pending = true
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				c := exec.Command(binary, "-test.run=^TestDockerCLIHelper$", "--", "late")
				c.Env = os.Environ()
				c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				if err = c.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = c.Wait() })
			}
			if mode == "unknown-create" {
				st.Pending = true
				st.Operation = "create"
				st.Container = ""
				if err := os.Rename(filepath.Join(root, "created"), filepath.Join(root, "waiting")); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "unlabeled" {
				if err := os.WriteFile(filepath.Join(root, "unlabeled"), []byte("legacy"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "no-docker" && mode != "orphan" {
				if err := saveWorkerJSON(s.dir, "owned.docker.json", st); err != nil {
					t.Fatal(err)
				}
				if err := AdoptHostWorker(w.HostStateDir, HostWorker{State: s.dir, Name: w.Machine.ID}); err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasPrefix(mode, "no-docker") {
				t.Setenv("PATH", t.TempDir())
			}
			failed := mode == "hang" || mode == "refuse-kill" || mode == "unknown-create" || mode == "no-docker-custody" || mode == "hang-inspect"
			timeout := 3 * time.Second
			if failed {
				timeout = 1200 * time.Millisecond
			}
			var service HostService = &fakeHostService{}
			if mode == "native-grace" {
				service = dockerGraceService{clock}
			}
			h := HostControl{Dir: w.HostStateDir, Service: service, Clock: clock, Docker: true, Timeout: timeout}
			advanceDone := make(chan struct{})
			go func() {
				tick := time.NewTicker(5 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-advanceDone:
						return
					case <-tick.C:
						if mode == "late" {
							if _, err := os.Stat(filepath.Join(root, "inspected-stopped")); err == nil {
								_ = os.WriteFile(filepath.Join(root, "release-late"), nil, 0600)
								return
							}
						}
						calls, _ := os.ReadFile(filepath.Join(root, "calls"))
						if strings.Contains(string(calls), "--signal=TERM") {
							clock.advance(time.Second)
							return
						}
					}
				}
			}()
			defer close(advanceDone)
			start := time.Now()
			grace := 30 * time.Millisecond
			if mode == "now" {
				grace = 0
			}
			r, err := h.Disable(context.Background(), "test", grace)
			if (err != nil) != failed || r.Confirmed == failed {
				t.Fatalf("report %+v error %v", r, err)
			}
			if time.Since(start) > timeout+time.Second {
				t.Fatal("overall deadline exceeded")
			}
			if _, err := os.Stat(filepath.Join(h.Dir, "DISABLED")); err != nil {
				t.Fatal(err)
			}
			if failed {
				if _, err := os.Stat(filepath.Join(h.Dir, "disable-receipt.json")); !os.IsNotExist(err) {
					t.Fatal("unconfirmed receipt")
				}
				if !strings.Contains(strings.Join(r.Unconfirmed, " "), "owned") {
					t.Fatalf("missing exact attempt: %+v", r)
				}
				if (mode == "refuse-kill" || mode == "hang-inspect") && !strings.Contains(strings.Join(r.Unconfirmed, " "), "id2") {
					t.Fatalf("missing second container: %+v", r)
				}
				if mode != "unknown-create" && !strings.Contains(strings.Join(r.Unconfirmed, " "), "id1") {
					t.Fatalf("missing exact container: %+v", r)
				}
			}
			if mode == "no-docker" {
				if !strings.Contains(r.Docker, "not applicable") {
					t.Fatalf("%+v", r)
				}
				return
			}
			if !failed {
				if _, err := os.Stat(filepath.Join(root, "running")); !os.IsNotExist(err) {
					t.Fatal("container still running")
				}
				calls, err := os.ReadFile(filepath.Join(root, "calls"))
				if err != nil {
					t.Fatal(err)
				}
				signal := "--signal=TERM"
				if mode != "term" {
					signal = "--signal=KILL"
				}
				if mode == "orphan" {
					signal = "stop --time 1 id1"
					if !strings.Contains(r.Docker, dockerOrphanEvidenceNote) || strings.Contains(string(calls), "--signal=TERM") {
						t.Fatalf("orphan protocol/evidence report: %+v calls=%s", r, calls)
					}
				}
				if !strings.Contains(string(calls), signal) {
					t.Fatalf("missing %s: %s", signal, calls)
				}
				if mode == "native-grace" && strings.Contains(string(calls), "--signal=TERM") {
					t.Fatal("Docker received a fresh grace after native cleanup")
				}
				if strings.Contains(string(calls), "legacy") {
					t.Fatal("touched unlabeled container")
				}
				if mode == "unlabeled" {
					if _, err := os.Stat(filepath.Join(root, "unlabeled")); err != nil {
						t.Fatal("legacy disappeared")
					}
				}
			}
		})
	}
}

func TestDockerDisabledLaunchAndResolution(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	if err := saveWorkerJSON(w.HostStateDir, "DISABLED", map[string]string{"reason": "test"}); err != nil {
		t.Fatal(err)
	}
	d := &dockerAttempt{hostDir: w.HostStateDir, clock: w.Clock, state: dockerExecution{Identity: "dispatch", Endpoint: "unix:///fake"}}
	if _, err := d.operation("create", "create"); err == nil {
		t.Fatal("dispatch ignored flag")
	}
	if _, err := w.startDocker(s, dockerJob(), t.TempDir(), nil); err == nil {
		t.Fatal("launched while disabled")
	}
	if _, err := os.Stat(filepath.Join(root, "calls")); !os.IsNotExist(err) {
		t.Fatal("contacted Docker")
	}
	s.state.Identity = "owned"
	j := dockerJob()
	s.state.Job = &j
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(s.dir, "owned.docker.json", dockerExecution{Identity: "owned", Endpoint: "unix:///old.sock", Pending: true}); err != nil {
		t.Fatal(err)
	}
	s.close()
	unlock, err := hostOperationLock(w.HostStateDir, "control.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	if err = ConfirmDockerGone(w.HostStateDir, w.StateDir, "owned", true); err == nil {
		t.Fatal("resolution raced host controls")
	}
	unlock()
	if err := ConfirmDockerGone(w.HostStateDir, w.StateDir, "owned", true); err != nil {
		t.Fatal(err)
	}
	if reason, err := HostDisabled(w.HostStateDir); err != nil || reason == "" {
		t.Fatal("resolution cleared disabled flag")
	}
	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "calls")); !os.IsNotExist(err) {
		t.Fatal("disabled startup contacted Docker")
	}
}

type dockerGraceService struct{ clock *dockerTestClock }

func (s dockerGraceService) Stop(context.Context, time.Duration) error {
	s.clock.advance(time.Second)
	return nil
}
func (s dockerGraceService) Empty(context.Context) (bool, error) { return true, nil }

func TestDisableDockerRegistryUnavailable(t *testing.T) {
	w, _, _, root := dockerFixture(t)
	if err := os.WriteFile(filepath.Join(w.HostStateDir, "worker-broken.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := (HostControl{Dir: w.HostStateDir, Docker: true, Service: &fakeHostService{}, Timeout: time.Second}).Disable(context.Background(), "test", 0)
	if err == nil || r.Confirmed || !strings.Contains(strings.Join(r.Unconfirmed, " "), "custody unconfirmed") {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := os.Stat(filepath.Join(w.HostStateDir, "disable-receipt.json")); !os.IsNotExist(err) {
		t.Fatal("receipt despite unknown pending operations")
	}
	if _, err := os.Stat(filepath.Join(root, "calls")); err != nil {
		t.Fatal("skipped available Docker cleanup")
	}
}
