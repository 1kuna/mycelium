package myce

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Mixed shortages must describe both eligible hosts, regardless of whether
// the CPU-busy host also lacks RAM. The RAM fallback used to skip busy CPUs.

// A RAM shortage on another eligible host must not hide the occupied card on
// the host that has enough RAM (#537). Cover both shared and exclusive slots.

// Retained regression: #537 shared occupied card diagnostic holder/tier.
func TestQueuedReasonCardContentionWithOtherHostRAMShortage(t *testing.T) {
	for _, exclusive := range []bool{false} {
		t.Run(map[bool]string{false: "shared", true: "exclusive"}[exclusive], func(t *testing.T) {
			q, clock := fixture(t)
			m := sharedUnifiedMachine()
			var running []*Job
			if exclusive {
				s := sharedSpec("experiment", 17)
				s.Needs.Model = "served"
				running = []*Job{assignGPU(t, q, m, submit(t, q, s))}
			} else {
				a, b := sharedPair(t, q, m)
				running = []*Job{a, b}
			}
			other := sharedUnifiedMachine()
			other.ID, other.HostID, other.Session = "ram-short", "ram-short", "ram-short-session"
			other.HostAvailableMemoryGB = 0.5
			machineCheck(t, q, other)
			j := submit(t, q, sharedSpec("experiment", 17))
			clock.Advance(time.Second)
			if got := assignedJob(machineCheck(t, q, m, running...)); got != nil {
				t.Fatal("contended card admitted another attempt", got.ID)
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			receipt, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, reason := range []string{q.waitReason(q.s.Jobs[j.ID]), job(t, q, j.ID).Reason, receipt.Reason} {
				want := "device 0 shared with 2 attempts"
				if exclusive {
					want = "card busy"
				}
				if !strings.Contains(reason, want) {
					t.Errorf("missing %q: %s", want, reason)
				}
				for _, holder := range running {
					if !strings.Contains(reason, holder.ID+" (experiment)") {
						t.Errorf("missing holding job and tier: %s", reason)
					}
				}
			}
			for _, reason := range []string{job(t, q, j.ID).Reason, receipt.Reason} {
				for _, want := range []string{"cores reserved; utilization unknown", "RAM live 101.0 GiB available", "GiB admission headroom"} {
					if !strings.Contains(reason, want) {
						t.Errorf("card contention hid capacity context %q: %s", want, reason)
					}
				}
			}
			other.HostAvailableMemoryGB = 101
			if got := assignedJob(machineCheck(t, q, other)); got == nil || got.ID != j.ID {
				t.Fatal("busy card hid a now-runnable target", got)
			}
		})
	}
}
