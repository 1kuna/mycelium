package myce

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestProductionHoldParitySpillover(t *testing.T) {
	for _, mode := range []string{"approved RAM", "unapproved RAM", "approved device", "unapproved device", "other work approval", "wrong digest", "explicit placement", "stale alternative", "insufficient RAM", "disabled alternative"} {
		t.Run(mode, func(t *testing.T) {
			deviceOnly := strings.HasSuffix(mode, "device")
			q, c := fixture(t)
			e := fakeExtension(t)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			if err := q.ConfigureHosts(map[string]HostPolicy{"hold-host": {ProductionHold: true}, "other-host": {ProductionHold: true}}); err != nil {
				t.Fatal(err)
			}
			policy := q.PriorityPolicy()
			approved := []string{"other-host"}
			if strings.HasPrefix(mode, "unapproved") {
				approved = nil
			}
			work := "kind:sample"
			if mode == "other work approval" {
				work = "program:sample-script"
			}
			policy.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{work: approved}}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
			a := placementMachine("a", "separate", 64, 8, e)
			b, other := placementMachine("b", "separate", 64, 8, e), placementMachine("other", "separate", 64, 8, e)
			a.HostID, b.HostID, other.HostID = "hold-host", "hold-host", "other-host"
			a.HostMemoryBudgetGB, b.HostMemoryBudgetGB, other.HostMemoryBudgetGB = 24, 24, 24
			check(t, q, a, nil)
			s := placementSpec(false)
			s.Priority, s.Extension, s.ExtensionDigest = "production", extensionKey(e), e.Digest
			s.SizeHint.HostMemoryGB = 16
			first := submit(t, q, s)
			c.Advance(time.Millisecond)
			s.SizeHint.HostMemoryGB = 12
			if deviceOnly {
				s.SizeHint.HostMemoryGB = 1 // Fits RAM; only the device slot is occupied.
			}
			if mode == "explicit placement" {
				s.AllowedMachines = []string{a.ID, b.ID}
			}
			queued := submit(t, q, s)
			running := check(t, q, a, nil).Job
			if running == nil || running.ID != first.ID {
				t.Fatal("production not running")
			}
			if err := q.Started(result(running, true)); err != nil {
				t.Fatal(err)
			}
			if !deviceOnly {
				check(t, q, b, nil) // Free sibling device, sharing the saturated RAM allowance.
			}
			if got := job(t, q, queued.ID); got.ProductionHost != "hold-host" || got.State != "queued" {
				t.Fatalf("incident not reproduced: %+v", got)
			}
			lower := spec("experiment")
			lower.AllowedMachines = []string{b.ID}
			submit(t, q, lower)
			if !deviceOnly && check(t, q, b, nil).Job != nil {
				t.Fatal("lower tier admitted on held host")
			}
			switch mode {
			case "wrong digest":
				for i := range other.ExtensionReports {
					other.ExtensionReports[i].Digest = "different-bundle"
				}
			case "insufficient RAM":
				other.HostMemoryBudgetGB = 8
			case "disabled alternative":
				if _, err := q.ChangeHostMaintenance(other.HostID, HostMaintenanceRequest{Action: "disable"}); err != nil {
					t.Fatal(err)
				}
			case "stale alternative":
				// Register without admitting work, then make its snapshot stale.
				other.ExtensionReports = nil
				check(t, q, other, nil)
				q.mu.Lock()
				q.s.Machines[other.ID].ExtensionReports = a.ExtensionReports
				q.mu.Unlock()
				c.Advance(6 * time.Second)
			}
			var assigned *Job
			if mode == "stale alternative" {
				check(t, q, a, running)
			} else {
				assigned = check(t, q, other, nil).Job
			}
			wantMove := mode == "approved RAM" || mode == "approved device"
			got := job(t, q, queued.ID)
			if wantMove {
				if assigned == nil || assigned.ID != queued.ID || got.ProductionHost != "other-host" || got.State != "running" || got.Attempt != 1 {
					t.Fatalf("approved idle host did not admit queued production: %+v", got)
				}
				stale := result(assigned, true)
				stale.Attempt++
				if err := q.Complete(stale); !errors.Is(err, ErrStale) {
					t.Fatalf("stale execution accepted: %v", err)
				}
			} else if assigned != nil || got.ProductionHost != "hold-host" || got.State != "queued" {
				t.Fatalf("unsafe spillover: %+v", got)
			}
			if mode == "unapproved RAM" && (!strings.HasPrefix(got.Reason, "waiting for RAM") || !strings.Contains(got.Reason, "bound to hold-host") || !strings.Contains(got.Reason, "production_parity_hosts")) {
				t.Fatalf("RAM/parity reason hidden: %s", got.Reason)
			}
			if mode == "unapproved device" && (!strings.HasPrefix(got.Reason, "waiting for admission") || !strings.Contains(got.Reason, "device occupied")) {
				t.Fatalf("device admission reason hidden: %s", got.Reason)
			}
			if original := job(t, q, running.ID); original.State != "running" || original.Machine != a.ID || original.ProductionHost != "hold-host" || original.Attempt != running.Attempt || original.Session != running.Session || original.StopAt != nil {
				t.Fatalf("running custody changed: %+v", original)
			}
			if !deviceOnly && check(t, q, b, nil).Job != nil {
				t.Fatal("spillover released lower tiers while original production runs")
			}
		})
	}
}

func TestProductionParityPolicyPersists(t *testing.T) {
	q, c := fixture(t)
	record := q.PriorityPolicy()
	if len(record.Policy.Projects["test"].ProductionParityHosts) != 0 {
		t.Fatal("parity must default to no approved hosts")
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"approved-host": {}}); err != nil {
		t.Fatal(err)
	}
	record.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"approved-host"}}}
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal(err)
	}
	// Input and exported slices cannot mutate the live policy without its CAS.
	record.Policy.Projects["test"].ProductionParityHosts["kind:sample"][0] = "unapproved-host"
	exported := q.PriorityPolicy()
	exported.Policy.Projects["test"].ProductionParityHosts["kind:sample"][0] = "unapproved-host"
	exported.Policy.Projects["test"].ProductionParityHosts["program:sample-script"] = []string{"unapproved-host"}
	path, root := q.dbPathForTest(t), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record = reopened.PriorityPolicy()
	if approvals := record.Policy.Projects["test"].ProductionParityHosts; len(approvals) != 1 || len(approvals["kind:sample"]) != 1 || approvals["kind:sample"][0] != "approved-host" || record.Revision != 1 {
		t.Fatalf("approval not preserved: %+v", record)
	}
	record.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {" "}}}
	if _, err := reopened.SetPriorityPolicy(record.Revision, record.Policy); err == nil {
		t.Fatal("empty host accepted")
	}
	record.Policy.Projects["test"] = ProjectPolicy{Weight: 1}
	if _, err := reopened.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal(err)
	}
	if len(reopened.PriorityPolicy().Policy.Projects["test"].ProductionParityHosts) != 0 {
		t.Fatal("consumer could not withdraw approval")
	}
}

func TestProductionParityPolicyRejectsUnknownHosts(t *testing.T) {
	for _, entry := range []string{"policy API", "project config"} {
		t.Run(entry, func(t *testing.T) {
			q, _ := fixture(t)
			record := q.PriorityPolicy()
			record.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"unknown-host"}}}
			var err error
			if entry == "policy API" {
				_, err = q.SetPriorityPolicy(record.Revision, record.Policy)
			} else {
				err = q.ConfigureProjects(record.Policy.Projects)
			}
			if err == nil || !strings.Contains(err.Error(), "unknown host") {
				t.Fatalf("unknown parity host accepted: %v", err)
			}
			if got := q.PriorityPolicy(); got.Revision != record.Revision || len(got.Policy.Projects) != 0 {
				t.Fatalf("rejected policy changed state: %+v", got)
			}
		})
	}
}
