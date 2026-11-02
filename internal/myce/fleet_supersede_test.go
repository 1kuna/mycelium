package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// #588: a stopped operation from days ago plus a worker without runtime proof
// refused fleet upgrade opaquely. The refusal must name both, and supersession
// must retire the operation with an audit record while keeping its drain.
func TestFleetUpgradeRefusalNamesStoppedOperationAndSupersedes(t *testing.T) {
	q, c, workers := fleetFixture(t, "queue", "remote", "laptop")
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	ctx := context.Background()
	var status FleetUpgradeStatus
	if err := client.Call(ctx, "POST", "/api/fleet/upgrade", FleetUpgradeRequest{QueueOnly: true}, &status); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	failure := FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: "fleet_storage_preflight_failed"}
	if err := client.Call(ctx, "POST", "/api/fleet/ack", failure, nil); err != nil || q.s.FleetUpgrade.State != "stopped" {
		t.Fatal("helper failure did not stop the operation", err)
	}
	c.Advance(5 * 24 * time.Hour)
	fleetHeartbeatAdvance(t, q, c)
	q.s.Machines[workers[1].ID].Fleet = nil

	var refusal *HTTPError
	err := client.Call(ctx, "POST", "/api/fleet/upgrade", FleetUpgradeRequest{WaitProductionIdle: true}, nil)
	if !errors.As(err, &refusal) || refusal.Status != 409 || refusal.Code != "fleet_upgrade_policy_conflict" {
		t.Fatalf("expected typed policy conflict, got %v", err)
	}
	for _, want := range []string{"fleet operation " + i.Operation + " is stopped", "--queue-only", "5d0h ago", "storage_preflight_failed", "myce fleet supersede --operation " + i.Operation, "worker " + workers[1].ID + " on host laptop: no fleet runtime proof"} {
		if !strings.Contains(refusal.Message, want) {
			t.Errorf("refusal %q lacks %q", refusal.Message, want)
		}
	}

	if err = client.Call(ctx, "POST", "/api/fleet/supersede", FleetSupersedeRequest{Operation: i.Operation}, nil); !errors.As(err, &refusal) || refusal.Code != "fleet_supersede_reason_required" {
		t.Fatalf("supersession without a reason: %v", err)
	}
	if err = client.Call(ctx, "POST", "/api/fleet/supersede", FleetSupersedeRequest{Operation: "other", Reason: "x"}, nil); !errors.As(err, &refusal) || refusal.Code != "fleet_operation_mismatch" || !strings.Contains(refusal.Message, i.Operation+" (stopped)") {
		t.Fatalf("supersession of the wrong operation: %v", err)
	}
	var record FleetSupersession
	if err = client.Call(ctx, "POST", "/api/fleet/supersede", FleetSupersedeRequest{Operation: i.Operation, Reason: "storage repaired; restart rollout"}, &record); err != nil {
		t.Fatal(err)
	}
	if record.Operation != i.Operation || record.State != "stopped" || record.Reason != "storage repaired; restart rollout" || len(record.RetainedDrains) != 1 || record.RetainedDrains[0] != "queue" || !strings.Contains(record.Next, "myce host resume") {
		t.Fatalf("incomplete audit record: %+v", record)
	}
	if p := q.s.HostMaintenance["queue"]; p.Mode != "drained" || p.DrainOwner != i.Operation {
		t.Fatalf("supersession changed drain custody: %+v", p)
	}
	var snapshot Snapshot
	if err = client.Call(ctx, "GET", "/api/status", nil, &snapshot); err != nil || snapshot.FleetUpgrade != nil || len(snapshot.FleetSuperseded) != 1 || snapshot.FleetSuperseded[0].Reason != record.Reason {
		t.Fatalf("status lost the audit record: %+v %v", snapshot.FleetSuperseded, err)
	}
	// The worker's durable receipt for the retired operation is refused as stale.
	if err = client.Call(ctx, "POST", "/api/fleet/ack", failure, nil); !errors.As(err, &refusal) || refusal.Status != 409 || refusal.Code != "fleet_ack_stale" {
		t.Fatalf("retired receipt not refused as stale: %v", err)
	}

	err = client.Call(ctx, "POST", "/api/fleet/upgrade", FleetUpgradeRequest{WaitProductionIdle: true}, nil)
	if !errors.As(err, &refusal) || refusal.Code != "fleet_worker_unmanaged" || !strings.Contains(refusal.Message, workers[1].ID) || !strings.Contains(refusal.Message, "install a fleet-capable release") {
		t.Fatalf("missing proof not named: %v", err)
	}
	q.s.Machines[workers[1].ID].Fleet = fleetTestProof(c, workers[1].Session, 300, false)
	if err = client.Call(ctx, "POST", "/api/fleet/upgrade", FleetUpgradeRequest{WaitProductionIdle: true}, &status); err != nil || status.Operation == i.Operation || status.State != "running" {
		t.Fatalf("new operation refused after supersession: %+v %v", status, err)
	}
	// The audit record and the new operation's start time survive a reopen.
	server.Close()
	q = reopenQueue(t, q, c)
	if len(q.s.FleetSuperseded) != 1 || q.s.FleetSuperseded[0].Reason != record.Reason || q.s.FleetUpgrade == nil || q.s.FleetUpgrade.Operation != status.Operation || !q.s.FleetUpgrade.CreatedAt.Equal(c.Now()) {
		t.Fatalf("supersession state lost on reopen: %+v %+v", q.s.FleetSuperseded, q.s.FleetUpgrade)
	}
}

// #600: the worker re-posted a refused failure receipt every poll (~2,600 an
// hour) and never presented its runtime proof. A definitive refusal settles it;
// transient failures back off on the injected clock without withholding proof.
func TestFleetFailureReceiptSettlesOrBacksOff(t *testing.T) {
	q, _ := fixture(t) // No running operation: the real queue answers fleet_ack_stale.
	stale := API{Queue: q, Token: "test"}.Handler()
	for _, tc := range []struct {
		name    string
		respond func(http.ResponseWriter, *http.Request)
		maxPost int
	}{
		{"queue stale 409", stale.ServeHTTP, 1},
		{"typed 400 refusal", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":"request body is invalid","code":"request_invalid"}`))
		}, 1},
		// Untyped request_failed is an internal queue failure (for example a
		// state commit error), so the receipt is retried with backoff.
		{"untyped 400 request_failed", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":"request failed","code":"request_failed"}`))
		}, 20},
		{"unavailable 503", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			if err := writeFleetReceipt(f.c, fleetInstallReceipt{Instruction: f.i, ConfigSHA256: strings.Repeat("a", 64), BeforeSHA256: strings.Repeat("b", 64), Phase: "failed", Failure: "fleet_storage_preflight_failed"}); err != nil {
				t.Fatal(err)
			}
			var posts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/fleet/ack" {
					t.Error("receipt poll made an unrelated request", r.URL.Path)
				}
				posts.Add(1)
				tc.respond(w, r)
			}))
			defer server.Close()
			clock := &fakeClock{now: time.Date(2026, 10, 27, 21, 12, 0, 0, time.UTC)}
			collect := func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error) {
				return &FleetRuntimeProof{Session: "new-session", ServiceHealthy: true, ObservedAt: clock.Now()}, nil
			}
			client := Client{URL: server.URL, Token: "test"}
			var result fleetPollResult
			var last, gap time.Duration
			start := clock.Now()
			for clock.Now().Sub(start) < time.Hour {
				before := posts.Load()
				// Ineligible: only receipt delivery and proof collection run.
				result = pollFleetRuntimeAt(context.Background(), f.c, client, collect, nil, f.i.Host, f.i.Machine, "new-session", false, result, clock)
				if result.proof == nil {
					t.Fatal("undelivered receipt withheld the runtime proof")
				}
				if posts.Load() != before {
					at := clock.Now().Sub(start)
					if before > 0 {
						gap = at - last
					}
					last = at
				}
				clock.Advance(1350 * time.Millisecond) // The observed poll cadence.
			}
			if n := int(posts.Load()); n < 1 || n > tc.maxPost {
				t.Fatalf("posted %d receipts in an hour, want 1..%d", n, tc.maxPost)
			}
			if tc.maxPost > 1 && (gap < fleetAckRetryMax || gap > fleetAckRetryMax+2*time.Second) {
				t.Fatalf("retry gap %v not capped at %v", gap, fleetAckRetryMax)
			}
		})
	}
}
