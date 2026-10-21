package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEnrollmentCompletedReceiptAfterResumeAllowsNextUpgrade(t *testing.T) {
	for _, scenario := range []string{"operator-resumed", "crash-after-owned-resume-commit"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("hypha", "hypha", "fake", 8, 32)
			check(t, q, m, nil)
			c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
			server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
			defer server.Close()
			client := Client{URL: server.URL, Token: "test"}
			receipt, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "operator-resumed" {
				if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
					t.Fatal(err)
				}
				if _, err := ResolveEnrollmentUpgrade(context.Background(), c, client); err == nil || !strings.Contains(err.Error(), "myce host resume hypha") {
					t.Fatalf("operator drain not preserved with recovery instruction: %v", err)
				}
				if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "resume"}); err != nil {
					t.Fatal(err)
				}
			} else {
				crash := errors.New("crash after resume committed")
				transport := &enrollmentOwnershipTransport{base: http.DefaultTransport, before: func(*http.Request) {}, after: func(r *http.Request) {
					if r.Method == "POST" {
						panic(crash)
					}
				}}
				func() {
					defer func() {
						if v := recover(); v != crash {
							t.Fatalf("resume commit crash not reached: %v", v)
						}
					}()
					_ = FinishEnrollmentUpgrade(context.Background(), c, Client{URL: client.URL, Token: client.Token, HTTP: &http.Client{Transport: transport}}, receipt)
				}()
			}
			status, err := q.HostMaintenanceStatus(m.HostID)
			if err != nil || status.Policy.Mode != "enabled" || status.Policy.DrainOwner != "" {
				t.Fatalf("resume not committed: %+v, %v", status.Policy, err)
			}
			if _, err := os.Stat(filepath.Join(c.State, enrollmentResumeFile)); err != nil {
				t.Fatal("completed receipt missing before recovery", err)
			}
			pending, err := ResolveEnrollmentUpgrade(context.Background(), c, client)
			if err != nil || pending.Policy.Mode != "" {
				t.Fatalf("enabled host blocked by stale receipt: %+v, %v", pending.Policy, err)
			}
			if _, err := os.Stat(filepath.Join(c.State, enrollmentResumeFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("stale receipt not retired", err)
			}
			next, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
			if err != nil || next.Policy.Mode != "drained" || next.Policy.DrainOwner == "" || next.Policy.DrainOwner == receipt.Policy.DrainOwner {
				t.Fatalf("next upgrade did not acquire a fresh owned drain: %+v, %v", next.Policy, err)
			}
			if err := FinishEnrollmentUpgrade(context.Background(), c, client, next); err != nil {
				t.Fatal(err)
			}
			status, err = q.HostMaintenanceStatus(m.HostID)
			if err != nil || status.Policy.Mode != "enabled" {
				t.Fatalf("next upgrade did not finish: %+v, %v", status.Policy, err)
			}
		})
	}
}

func TestEnrollmentReceiptWithoutDrainPreservesQueuePolicy(t *testing.T) {
	for _, action := range []string{"resume", "disable"} {
		for _, intent := range []bool{false, true} {
			t.Run(action+map[bool]string{false: "/completed", true: "/intent"}[intent], func(t *testing.T) {
				q, clock := fixture(t)
				m := cpuMachine("hypha", "hypha", "fake", 8, 32)
				check(t, q, m, nil)
				c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
				server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
				defer server.Close()
				client := Client{URL: server.URL, Token: "test"}
				receipt, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
				if err != nil {
					t.Fatal(err)
				}
				if intent {
					if err := saveWorkerJSON(c.State, enrollmentResumeFile, enrollmentResumeReceipt{Intent: true, Host: c.HostID, Name: c.Name, UpdatedAt: receipt.Policy.UpdatedAt, DrainOwner: receipt.Policy.DrainOwner}); err != nil {
						t.Fatal(err)
					}
				}
				before, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: action})
				if err != nil {
					t.Fatal(err)
				}
				// An old in-memory receipt must not produce an ineffective command
				// either, including when a response was lost after resume committed.
				if err := FinishEnrollmentUpgrade(context.Background(), c, client, receipt); err == nil || strings.Contains(err.Error(), "myce host resume") {
					t.Fatalf("unexpected stale resume result: %v", err)
				}
				if pending, err := ResolveEnrollmentUpgrade(context.Background(), c, client); err != nil || pending.Policy.Mode != "" {
					t.Fatalf("non-drained policy blocked by receipt: %+v, %v", pending.Policy, err)
				}
				if _, err := os.Stat(filepath.Join(c.State, enrollmentResumeFile)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("stale receipt not retired", err)
				}
				after, err := q.HostMaintenanceStatus(m.HostID)
				if err != nil || after.Policy.Mode != before.Policy.Mode || !after.Policy.UpdatedAt.Equal(before.Policy.UpdatedAt) || after.Policy.DrainOwner != before.Policy.DrainOwner {
					t.Fatalf("queue policy changed: %+v -> %+v, %v", before.Policy, after.Policy, err)
				}
			})
		}
	}
}
