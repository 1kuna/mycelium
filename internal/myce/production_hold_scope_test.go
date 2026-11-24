package myce

import (
	"slices"
	"strings"
	"testing"
)

// MY-50 removed the whole-machine scope. The policy API refuses it; a startup
// file or saved state from the previous release converts to device scope, and
// both survive restart.
func TestWholeMachineHoldRefusedOrConverted(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	startup := map[string]HostPolicy{"machine": {ProductionHold: true}}
	if err := q.ConfigureHosts(startup); err != nil {
		t.Fatal("whole-machine startup file did not convert:", err)
	}
	device := HostPolicy{ProductionHold: true, ProductionHoldScope: "device"}
	record := q.PriorityPolicy()
	if got := record.Policy.Hosts["machine"]; got != device {
		t.Fatalf("startup file kept whole-machine scope: %+v", got)
	}
	for _, bad := range []HostPolicy{{ProductionHold: true}, {ProductionHold: true, ProductionHoldScope: "host"}, {ProductionHold: true, ProductionHoldScope: "card"}, {ProductionHoldScope: "device"}} {
		record.Policy.Hosts = map[string]HostPolicy{"machine": bad}
		_, err := q.SetPriorityPolicy(record.Revision, record.Policy)
		if err == nil {
			t.Fatalf("invalid host policy accepted: %+v", bad)
		}
		if bad.ProductionHold && bad.ProductionHoldScope != "card" && !strings.Contains(err.Error(), "whole-machine production hold was removed") {
			t.Fatalf("refusal does not explain the removal: %v", err)
		}
	}
	if got := q.PriorityPolicy(); got.Revision != record.Revision || got.Policy.Hosts["machine"] != device {
		t.Fatalf("rejected change altered policy: %+v", got)
	}
	// Omitting hosts (an older client) preserves them.
	record.Policy.Hosts = nil
	record, err := q.SetPriorityPolicy(record.Revision, record.Policy)
	if err != nil || record.Policy.Hosts["machine"] != device {
		t.Fatalf("omitted hosts replaced policy: %+v %v", record, err)
	}
	// Saved state written by the previous release still says whole-machine.
	if err := q.update(func() error {
		q.s.Hosts["machine"].Policy = HostPolicy{ProductionHold: true}
		q.s.HostConfig = startup
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	path, root := q.dbPathForTest(t), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if got := q.PriorityPolicy().Policy.Hosts["machine"]; got != device {
		t.Fatalf("saved whole-machine policy not converted on restart: %+v", got)
	}
	if err := q.ConfigureHosts(startup); err != nil {
		t.Fatal("unchanged startup file refused after conversion:", err)
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"machine": {ProductionHold: false}}); err == nil || !strings.Contains(err.Error(), "saved policy") {
		t.Fatalf("edited startup file accepted: %v", err)
	}

	m := m3Machine()
	submit(t, q, bigSpec("production", 8))
	p := assignedJob(machineCheck(t, q, m))
	if p == nil {
		t.Fatal("production did not start after restart")
	}
	submit(t, q, bigSpec("experiment", 8))
	x := assignedJob(machineCheck(t, q, m, p))
	if x == nil || slices.Equal(p.AllocatedDevices, x.AllocatedDevices) {
		t.Fatal("device scope not applied after restart", x)
	}
}
