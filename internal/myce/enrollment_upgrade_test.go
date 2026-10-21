package myce

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	if _, err := PrepareEnrollmentUpgrade(context.Background(), ServiceConfig{Name: m.ID, HostID: m.HostID}, Client{URL: server.URL, Token: "test"}, clock, time.Second, nil); err == nil {
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
			c := ServiceConfig{Name: m.ID, HostID: m.HostID}
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
	c := ServiceConfig{Name: m.ID, HostID: m.HostID}
	_, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil })
	if err == nil {
		t.Fatal("busy custody permitted upgrade")
	}
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Quiescent || len(status.Attempts) == 0 || status.Policy.Mode != "drained" {
		t.Fatal("custody or maintenance lost", status)
	}
	if _, err = q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	// The clock deliberately has not advanced: mode is an additional CAS fence.
	if err = FinishEnrollmentUpgrade(context.Background(), c, client, status); err == nil {
		t.Fatal("upgrade resumed a newly disabled host")
	}
	got := job(t, q, j.ID)
	if got.Attempt != j.Attempt || got.ReleaseConfirmed {
		t.Fatal("upgrade changed execution custody")
	}
}
