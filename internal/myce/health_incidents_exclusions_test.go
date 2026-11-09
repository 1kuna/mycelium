package myce

import (
	"testing"
)

func incidentAllowance(t *testing.T, q *Queue, m *Machine, gb float64) {
	t.Helper()
	// Reports cannot change the queue-owned allowance after registration.
	// Replay the operator update and worker acknowledgement before resuming.
	for _, request := range []HostMaintenanceRequest{{Action: "drain"}, {Action: "budget", MemoryBudgetGB: &gb}} {
		if _, err := q.ChangeHostMaintenance(hostID(m), request); err != nil {
			t.Fatal(err)
		}
	}
	m.HostMemoryBudgetGB = gb
	check(t, q, *m, nil)
	if _, err := q.ChangeHostMaintenance(hostID(m), HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
}

func incidentPeer(t *testing.T, q *Queue, m *Machine, id string) Machine {
	t.Helper()
	peer := cloneMachine(m)
	peer.ID, peer.HostID, peer.Session = id, id, "session-"+id
	// Qualification is host-bound; copying another host's verdict does not
	// make this peer an eligible alternative, even with identical hardware.
	for i, old := range peer.ExtensionReports {
		e, err := q.extensionByDigest(old.Digest)
		if err != nil {
			t.Fatal(err)
		}
		r := newExtensionReport(e, &peer, old.Work)
		r.Setup, r.Qualified, r.CheckID = true, true, "peer-check"
		peer.ExtensionReports[i] = r
	}
	return peer
}

func incidentVariantFixture(t *testing.T, capGB float64) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
	v.Execution.Main.MemoryGB = capGB
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("variant-worker", "variant-host", "cpu", 8, 64)
	m.ExtensionFingerprint = "cpu-v1"
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	s := Spec{Name: "variant-work", Project: "synthetic", Extension: extensionKey(e), Workload: &Workload{InputShape: "small-v1", Units: 10}}
	return q, clock, m, s
}
