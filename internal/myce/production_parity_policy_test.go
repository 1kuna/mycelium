package myce

import (
	"strings"
	"testing"
	"time"
)

func TestProductionParityPolicyConstrainsVariants(t *testing.T) {
	for _, mode := range []string{"legacy", "current", "measured"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			e := compositionExtension(t, false)
			// Equal capacity makes the unapproved candidate win the normal
			// deterministic placement tie, unless owner policy fences it.
			a := valueMachine("reference-worker", "xpu", 96)
			b := valueMachine("candidate-worker", "cuda", 96)
			a.CapabilityProfile.OS, a.CapabilityProfile.Arch = "linux", "amd64"
			b.CapabilityProfile.OS, b.CapabilityProfile.Arch = "linux", "arm64"
			v := e.Variants[0]
			e.Variants = []ExtensionVariant{v, v}
			for i, m := range []*Machine{&a, &b} {
				e.Variants[i].Target = machineTarget(m)
				e.Variants[i].Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1, HostMemoryGB: 1}
				if mode == "measured" {
					e.Variants[i].Quality = map[string]map[string]QualityMeasurement{"extension:" + e.Name: {"score": {Reference: "frozen-reference", Value: qualityNumber(1), ReferenceValue: qualityNumber(1), MarginOfError: qualityNumber(0)}}}
				}
			}
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			if mode == "legacy" {
				// An existing registration predates server-owned parity metadata.
				if err := q.update(func() error {
					old := q.s.Extensions[extensionKey(e)]
					old.ProductionParityRequired = false
					q.touchCatalog("extensions/" + extensionKey(e))
					q.s.Extensions[extensionKey(e)] = old
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			for _, m := range []*Machine{&a, &b} {
				m.ExtensionFingerprint = "native-variant-fixture"
				r := newExtensionReport(e, m, "extension:"+e.Name)
				r.Setup, r.Qualified, r.CheckID = true, true, "native-check"
				m.ExtensionReports = []ExtensionReport{r}
				check(t, q, *m, nil)
			}
			policy := q.PriorityPolicy()
			policy.Policy.Hosts = map[string]HostPolicy{hostID(&a): {}, hostID(&b): {ProductionHold: true, ProductionHoldScope: "device"}}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
			setParityHosts(t, q, "test", "extension:"+e.Name, hostID(&a))
			spec := variantSpec(e, "production")
			spec.Workload = &Workload{InputShape: "recording-v1", Units: 1}
			spec.FanOut = &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "recording"}}}
			parent := submit(t, q, spec)
			if assigned := check(t, q, b, nil).Job; assigned != nil {
				t.Fatal("unapproved host executed production", assigned.ID)
			}
			clock.Advance(time.Second)
			q = reopenVariants(t, q, clock)
			got, err := q.jobSnapshot(parent.ID)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range got.VariantEligibility {
				if row.Host == hostID(&b) && row.Variant == machineTarget(&b) {
					found = true
					if !row.Qualified || row.Eligible || !strings.Contains(strings.Join(row.Reasons, ";"), "is not a machine kind the project approved") {
						t.Fatalf("unapproved variant status: %+v", row)
					}
				}
			}
			if !found {
				t.Fatal("missing candidate variant status")
			}
			if assigned := check(t, q, b, nil).Job; assigned != nil {
				t.Fatal("restart admitted unapproved production", assigned.ID)
			}
			assigned := check(t, q, a, nil).Job
			if assigned == nil || assigned.Batch == nil || assigned.Batch.Parent != parent.ID {
				t.Fatal("approved host did not receive the production child", assigned)
			}
			// A later owner change fences the already accepted child's next
			// admission.
			setParityHosts(t, q, "test", spec.workType(), hostID(&b))
			parityReason := func(id string, m *Machine) string {
				q.mu.Lock()
				defer q.mu.Unlock()
				return q.variantParityReason(q.s.Jobs[id], m)
			}
			if parityReason(assigned.ID, &a) == "" {
				t.Fatal("accepted child bypassed live owner policy")
			}
			if reason := parityReason(parent.ID, &b); reason != "" {
				t.Fatal("explicit owner approval did not admit the candidate", reason)
			}
			// Experiments remain available for comparing candidate outputs.
			experiment := submit(t, q, variantSpec(e, "experiment"))
			if reason := parityReason(experiment.ID, &a); reason != "" {
				t.Fatal("production list restricted an experiment", reason)
			}
			setParityHosts(t, q, "test", spec.workType())
			if parityReason(parent.ID, &b) == "" {
				t.Fatal("empty owner list admitted production")
			}
			checkJob := *job(t, q, parent.ID)
			checkJob.Spec.ExtensionCheck = &ExtensionCheckRequest{Work: spec.workType()}
			q.mu.Lock()
			reason := q.variantParityReason(&checkJob, &b)
			q.mu.Unlock()
			if reason != "" {
				t.Fatal("owner list prevented a native evidence check", reason)
			}
		})
	}
}

func TestProductionParityPolicyRejectsUnknownHosts(t *testing.T) {
	for _, entry := range []string{"parity add", "project config"} {
		t.Run(entry, func(t *testing.T) {
			q, _ := fixture(t)
			record := q.PriorityPolicy()
			record.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"unknown-host"}}}
			var err error
			if entry == "parity add" {
				_, err = q.AddParity(ParityRequest{Project: "test", Work: "kind:sample", Host: "unknown-host", Evidence: "job"})
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
