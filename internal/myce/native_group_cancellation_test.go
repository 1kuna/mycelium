package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNativeGroupGPUCancellationSidecarGrace(t *testing.T) {
	for _, mode := range []string{"cooperative", "sidecar-escalation", "main-escalation"} {
		t.Run(mode, func(t *testing.T) {
			w := recoveryWorker(t)
			clock := &manualWorkerClock{now: time.Date(2026, 10, 29, 21, 0, 0, 0, time.UTC)}
			w.Clock = clock
			w.memoryCeilings = fakeCeilings(t)
			w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = w.Machine.Session
			command := func(name string) []string {
				code := "0"
				if name == "main" {
					code = "143"
				}
				return []string{"sh", "-c", `trap 'touch "$1-term"; while [ ! -f "$1-exit" ]; do sleep .01; done; exit ` + code + `' TERM; touch "$1-ready"; while :; do sleep .01; done`, name, "{work}/" + name}
			}
			j := recoveryJob(t, command("main"))
			j.Spec.Resumable = false
			j.Spec.Needs.HostMemoryGB = 2
			j.Spec.Run.MemoryGB = .5
			j.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: command("helper"), MemoryGB: 1}}
			var p *process
			await(t, func() bool {
				p, err = w.start(store, j)
				return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = syscall.Kill(-p.native.supervisor.Process.Pid, syscall.SIGKILL) }()
			dir := p.job.Spec.Run.Workdir
			nativeFile(t, dir, "main-ready")
			nativeFile(t, dir, "helper-ready")
			p.stopDirective(clock.Now(), StopGrace, false, true)
			nativeFile(t, dir, "main-term")
			if _, err := os.Stat(filepath.Join(dir, "helper-term")); !os.IsNotExist(err) {
				t.Fatal("sidecar stopped before main exited", err)
			}
			clock.Advance(9 * time.Second)
			p.enforceGrace(clock.Now())
			if mode == "main-escalation" {
				clock.Advance(time.Second)
				p.enforceGrace(clock.Now())
			} else {
				nativeTouch(t, dir, "main-exit")
			}
			await(t, func() bool {
				_, err := os.Stat(filepath.Join(dir, "helper-term"))
				p.mu.Lock()
				defer p.mu.Unlock()
				return err == nil || p.native.members[0].Exit != nil
			})
			if _, err := os.Stat(filepath.Join(dir, "helper-term")); err != nil {
				t.Fatal("sidecar exited without TERM", err)
			}
			// Main exit must not spend or truncate the sidecar's own grace.
			clock.Advance(10*time.Second - time.Nanosecond)
			p.stopDirective(clock.Now(), 0, true, true)
			select {
			case <-p.done:
				t.Fatal("released while sidecar was draining")
			default:
			}
			p.mu.Lock()
			for _, m := range p.native.members {
				if m.Name == "helper" {
					for _, s := range m.Signals {
						if s.Signal == "KILL" {
							t.Error("sidecar KILL before grace")
						}
					}
				}
			}
			p.mu.Unlock()
			if mode == "sidecar-escalation" {
				clock.Advance(time.Nanosecond)
				p.enforceGrace(clock.Now())
			} else {
				nativeTouch(t, dir, "helper-exit")
			}
			result := d5Finish(t, p)
			if result.ReleasePending || !result.Interrupted || result.Forced != (mode != "cooperative") {
				t.Fatalf("cancel outcome: %+v", result)
			}
			if members, err := workerGroupMembers(p.native.supervisor.Process.Pid); err != nil || len(members) != 0 {
				t.Fatal("released live group", members, err)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			for _, m := range p.native.members {
				if len(m.Signals) == 0 || m.Signals[0].Signal != "TERM" {
					t.Errorf("member %s missed TERM: %+v", m.Name, m.Signals)
				}
				if m.Name == "helper" && mode != "sidecar-escalation" && len(m.Signals) != 1 {
					t.Errorf("cooperative sidecar received extra signals: %+v", m.Signals)
				}
			}
		})
	}
}

func TestNativeGroupGPUCancellationRecoveryGrace(t *testing.T) {
	for _, resumed := range []bool{false, true} {
		t.Run(map[bool]string{false: "main-phase", true: "sidecar-phase"}[resumed], func(t *testing.T) {
			dir := t.TempDir()
			clock := &manualWorkerClock{now: time.Date(2026, 10, 29, 21, 0, 0, 0, time.UTC)}
			start := func(name string, group int) *exec.Cmd {
				cmd := exec.Command("sh", "-c", `trap 'touch "$1-term"; while [ ! -f "$1-exit" ]; do sleep .01; done; exit 0' TERM; touch "$1-ready"; while :; do sleep .01; done`, name, filepath.Join(dir, name))
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: group}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
				nativeFile(t, dir, name+"-ready")
				return cmd
			}
			helper := start("helper", 0)
			t.Cleanup(func() { _ = syscall.Kill(-helper.Process.Pid, syscall.SIGKILL) })
			birth, err := processBirth(helper.Process.Pid)
			if err != nil {
				t.Fatal(err)
			}
			j := recoveryJob(t, []string{"unused"})
			deadline := clock.Now().Add(10 * time.Second)
			st := workerState{Identity: ID(), PID: helper.Process.Pid, Job: &j, UserCancel: true, StopAt: &deadline, StopGrace: 10 * time.Second, Groups: map[int]map[int]string{helper.Process.Pid: {helper.Process.Pid: birth}}}
			members := []nativeMember{{Name: "helper", PID: helper.Process.Pid, Birth: birth}}
			if resumed {
				at := clock.Now().Add(3 * time.Second)
				st.SidecarStopAt = &at
			} else {
				main := start("main", helper.Process.Pid)
				birth, err := processBirth(main.Process.Pid)
				if err != nil {
					t.Fatal(err)
				}
				st.Groups[helper.Process.Pid][main.Process.Pid] = birth
				members = append(members, nativeMember{Name: "main", PID: main.Process.Pid, Birth: birth})
			}
			store := &workerStore{dir: dir, state: st}
			if err := saveWorkerJSON(dir, "attempt-"+st.Identity+".json", st); err != nil {
				t.Fatal(err)
			}
			if err := saveWorkerJSON(dir, "native-"+st.Identity+".json", struct {
				Identity string
				Members  []nativeMember
			}{st.Identity, members}); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- store.cleanupOrphan(clock) }()
			if !resumed {
				nativeFile(t, dir, "main-term")
				if _, err := os.Stat(filepath.Join(dir, "helper-term")); !os.IsNotExist(err) {
					t.Fatal("recovery stopped helper before main", err)
				}
				clock.Advance(9 * time.Second)
				nativeTouch(t, dir, "main-exit")
			}
			nativeFile(t, dir, "helper-term")
			var saved workerState
			data, err := os.ReadFile(filepath.Join(dir, "attempt-"+st.Identity+".json"))
			if err != nil || json.Unmarshal(data, &saved) != nil || saved.SidecarStopAt == nil {
				t.Fatal("missing durable helper deadline", err)
			}
			want := clock.Now().Add(10 * time.Second)
			if resumed {
				want = *st.SidecarStopAt
			}
			if !saved.SidecarStopAt.Equal(want) {
				t.Fatal("helper deadline renewed or shortened", saved.SidecarStopAt, want)
			}
			clock.Advance(want.Sub(clock.Now()) - time.Nanosecond)
			select {
			case err := <-done:
				t.Fatal("recovery released before helper grace", err)
			default:
			}
			clock.Advance(time.Nanosecond)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("recovery did not finish")
			}
			if live, err := workerGroupMembers(helper.Process.Pid); err != nil || len(live) != 0 {
				t.Fatal("recovery released live group", live, err)
			}
		})
	}
}
