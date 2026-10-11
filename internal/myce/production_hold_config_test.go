package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostPolicyDefaultsValidationAndPersistence(t *testing.T) {
	q, _ := fixture(t)
	if e := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true, ProductionIdle: "-1s"}}); e == nil {
		t.Fatal("negative idle accepted")
	}
	file := filepath.Join(t.TempDir(), "hosts.yaml")
	if e := os.WriteFile(file, []byte("hosts:\n  b70: {production_hold: true}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	p, e := ReadHostPolicies(file)
	if e != nil {
		t.Fatal(e)
	}
	if e = q.ConfigureHosts(p); e != nil {
		t.Fatal(e)
	}
	if e = q.ConfigureHosts(p); e != nil {
		t.Fatal("same config rejected", e)
	}
	if e = q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: false}}); e == nil || !strings.Contains(e.Error(), "saved policy") {
		t.Fatalf("relaxed policy %v", e)
	}
	if e = os.WriteFile(file, []byte("hosts:\n  b70: {production_hold: true, typo: true}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ReadHostPolicies(file); e == nil {
		t.Fatal("unknown setting accepted")
	}
}
func TestProductionHoldSimulation(t *testing.T) {
	var f SimFleet
	var w SimWorkload
	if e := ReadSimYAML("../../examples/sim/production-hold/fleet.yaml", &f); e != nil {
		t.Fatal(e)
	}
	if e := ReadSimYAML("../../examples/sim/production-hold/workload.yaml", &w); e != nil {
		t.Fatal(e)
	}
	r, e := Simulate(f, w, SimOptions{Days: .01, Seed: 1})
	if e != nil {
		t.Fatal(e)
	}
	if r.Completed != 3 || r.Preemptions != 2 || r.Production.P95 > 2 || r.Machines["spark"].BusyHours < .1 {
		t.Fatalf("hold simulation %+v", r)
	}
}
