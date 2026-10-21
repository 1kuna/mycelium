package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEnrollmentUpgradeRefusesQueueProtocolBeforeDrain(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	check(t, q, m, nil)
	api := (API{Queue: q, Token: "test"}).Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/workers/") {
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	if _, err := PrepareEnrollmentUpgrade(context.Background(), ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}, Client{URL: server.URL, Token: "test"}, clock, time.Second, nil); err == nil {
		t.Fatal("queue protocol mismatch permitted upgrade")
	}
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.Mode != "enabled" {
		t.Fatal("protocol rejection changed maintenance", status.Policy.Mode, err)
	}
}

func TestEnrollmentUpgradePreservesDisabledAndDrained(t *testing.T) {
	for _, action := range []string{"disable", "drain", "resume"} {
		t.Run(action, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("hypha", "hypha", "fake", 8, 32)
			check(t, q, m, nil)
			if _, err := q.ChangeHostMaintenance("hypha", HostMaintenanceRequest{Action: action}); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
			defer server.Close()
			client := Client{URL: server.URL, Token: "test"}
			c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
			receipt, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = FinishEnrollmentUpgrade(context.Background(), c, client, receipt); err != nil {
				t.Fatal(err)
			}
			status, err := q.HostMaintenanceStatus(m.HostID)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]string{"disable": "disabled", "drain": "drained", "resume": "enabled"}[action]
			if status.Policy.Mode != want {
				t.Fatalf("maintenance changed: %s -> %s", want, status.Policy.Mode)
			}
		})
	}
}
func TestEnrollmentUpgradeCustodyBlocksAndCASDoesNotResumeDisabled(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	j := maintenanceAssign(t, q, m, "experiment")
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
	_, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil })
	if err == nil {
		t.Fatal("busy custody permitted upgrade")
	}
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Quiescent || len(status.Attempts) == 0 || status.Policy.Mode != "enabled" {
		t.Fatal("custody or maintenance lost", status)
	}
	if _, err = q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	// Mode is an additional CAS fence even with an equal timestamp.
	status.Policy.Mode = "drained"
	if err = FinishEnrollmentUpgrade(context.Background(), c, client, status); err == nil {
		t.Fatal("upgrade resumed a newly disabled host")
	}
	got := job(t, q, j.ID)
	if got.Attempt != j.Attempt || got.ReleaseConfirmed {
		t.Fatal("upgrade changed execution custody")
	}
}

func TestEnrollmentUpgradeRerunResumesOwnDrain(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	check(t, q, m, nil)
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
	if _, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil); err != nil {
		t.Fatal(err)
	}
	// Simulate an installer failure and a new invocation with no in-memory receipt.
	receipt, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = FinishEnrollmentUpgrade(context.Background(), c, client, receipt); err != nil {
		t.Fatal(err)
	}
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.Mode != "enabled" {
		t.Fatal("rerun abandoned its own drain", status.Policy.Mode, err)
	}
}

func TestEnrollmentBusyUpgradeDoesNotLeaveDrain(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	maintenanceAssign(t, q, m, "experiment")
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	_, err := PrepareEnrollmentUpgrade(context.Background(), ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}, Client{URL: server.URL, Token: "test"}, clock, time.Second, func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil })
	if err == nil {
		t.Fatal("busy upgrade accepted")
	}
	status, e := q.HostMaintenanceStatus(m.HostID)
	if e != nil || status.Policy.Mode != "enabled" {
		t.Fatal("failed upgrade left host drained", status.Policy.Mode, e)
	}
}

func TestEnrollmentDrainRaceTimeoutRestoresPolicy(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	check(t, q, m, nil)
	c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
	api := (API{Queue: q, Token: "test"}).Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/maintenance") {
			status, err := q.HostMaintenanceStatus(m.HostID)
			if r.Method == "POST" {
				var request HostMaintenanceRequest
				if err = json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				status, err = q.ChangeHostMaintenance(m.HostID, request)
			}
			// Model an obligation that arrived after the enabled snapshot and before
			// the drain CAS. It cannot be canceled by onboarding.
			if status.Policy.Mode == "drained" {
				status.Quiescent = false
			}
			respond(w, status, err)
			return
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	_, err := PrepareEnrollmentUpgrade(context.Background(), c, Client{URL: server.URL, Token: "test"}, clock, time.Second, func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil })
	if err == nil {
		t.Fatal("racing custody allowed install")
	}
	status, e := q.HostMaintenanceStatus(m.HostID)
	if e != nil || status.Policy.Mode != "enabled" {
		t.Fatal("timeout abandoned drain", status.Policy.Mode, e)
	}
	if _, e = os.Stat(filepath.Join(c.State, enrollmentResumeFile)); !os.IsNotExist(e) {
		t.Fatal("successful timeout undo retained resume authority", e)
	}
}

func TestEnrollmentUpgradeReceiptFailureRestoresPolicy(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	check(t, q, m, nil)
	state := t.TempDir()
	if err := os.Mkdir(filepath.Join(state, enrollmentResumeFile+".tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	_, err := PrepareEnrollmentUpgrade(context.Background(), ServiceConfig{Name: m.ID, HostID: m.HostID, State: state}, Client{URL: server.URL, Token: "test"}, clock, time.Second, nil)
	if err == nil {
		t.Fatal("upgrade permitted without durable resume authority")
	}
	status, e := q.HostMaintenanceStatus(m.HostID)
	if e != nil || status.Policy.Mode != "enabled" {
		t.Fatal("receipt failure left drain", status.Policy.Mode, e)
	}
}
