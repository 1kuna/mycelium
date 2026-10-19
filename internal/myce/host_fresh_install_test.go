package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestConfiguredQueueWithoutLocalWorkerSession(t *testing.T) {
	for _, mode := range []string{"empty", "custody", "unreachable"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("MYCE_TOKEN", "fixture")
			dir := readyTestHost(t, t.TempDir(), "never-started-host")
			q, err := Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), RealClock{})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			if mode == "custody" {
				m := machine("required-worker", "fake")
				m.HostID = "never-started-host"
				submit(t, q, spec("experiment"))
				if check(t, q, m, nil).Job == nil {
					t.Fatal("custody fixture did not start an attempt")
				}
			}
			report, err := (HostControl{Dir: dir, Observe: testHostExclusion, Service: &fakeHostService{}}).Disable(context.Background(), "close before first local worker session", 0)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
			defer server.Close()
			client := Client{URL: server.URL, Token: "fixture"}
			makeClient := func(string) (Client, error) {
				if mode == "unreachable" {
					return Client{}, errors.New("offline")
				}
				return client, nil
			}
			err = ReconcileHostLocal(context.Background(), dir, RealClock{}, makeClient, server.URL)
			if (mode == "unreachable") != (err != nil) {
				t.Fatal(err)
			}
			state, err := ReadHostIntent(dir)
			if err != nil {
				t.Fatal(err)
			}
			workers, err := HostWorkers(dir)
			if err != nil || len(workers) != 0 {
				t.Fatal("test created a local session", workers, err)
			}
			if mode == "empty" {
				if state.HostSettlement() != "confirmed" || len(state.Pending) != 0 {
					t.Fatal("empty queue did not confirm exact release", state)
				}
				envelope, err := HostSnapshot(context.Background(), HostInvocation{StateDir: dir, Config: ServiceConfig{Queue: server.URL, Name: "required-worker"}}, RealClock{})
				if err != nil {
					t.Fatal(err)
				}
				envelope.ApplyExclusion(report.Exclusion, RealClock{}.Now())
				envelope.ApplyRestVerdict("closed", RealClock{}.Now())
				if envelope.Verdict != "PASS" {
					t.Fatal("closed never-started host is not PASS", envelope.HostRestVerdict)
				}
			} else {
				if state.HostSettlement() == "confirmed" {
					t.Fatal("missing local registry hid remote custody or outage")
				}
				if mode == "custody" && len(state.Pending) == 0 {
					t.Fatal("queue custody disappeared")
				}
				if _, err := changeHostIntent(dir, RealClock{}, hostIntentChange{Desired: "open", Expected: state.HostRevision}); !errors.Is(err, ErrPreviousCloseUnsettled) {
					t.Fatal("unsafe open accepted", err)
				}
			}
		})
	}
}
