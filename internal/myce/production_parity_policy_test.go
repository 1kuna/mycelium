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
			a := valueMachine("reference-worker", "xpu", 32)
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
			policy.Policy.Hosts = map[string]HostPolicy{hostID(&a): {}, hostID(&b): {ProductionHold: true}}
			policy.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"extension:" + e.Name: {hostID(&a)}}}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
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
					if !row.Qualified || row.Eligible || !strings.Contains(strings.Join(row.Reasons, ";"), "host outside production_parity_hosts") {
						t.Fatalf("unapproved variant status: %+v", row)
					}
				}
			}
			if !found {
				t.Fatal("missing candidate variant status")
			}
			if got.ProductionHost == hostID(&b) {
				t.Fatal("production hold bound demand to an unapproved host")
			}
			if q.productionParityKnown(q.s.Jobs[parent.ID], &b) {
				t.Fatal("unapproved host acquired retargeting authority")
			}
			if assigned := check(t, q, b, nil).Job; assigned != nil {
				t.Fatal("restart admitted unapproved production", assigned.ID)
			}
			assigned := check(t, q, a, nil).Job
			if assigned == nil || assigned.Batch == nil || assigned.Batch.Parent != parent.ID {
				t.Fatal("approved host did not receive the production child", assigned)
			}
			// A later owner change fences the already accepted child's next
			// admission, even if a scheduler binding still names this host.
			policy = q.PriorityPolicy()
			policy.Policy.Projects["test"].ProductionParityHosts[spec.workType()] = []string{hostID(&b)}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
			if q.variantParityReason(q.s.Jobs[assigned.ID], &a) == "" {
				t.Fatal("accepted child bypassed live owner policy")
			}
			if reason := q.variantParityReason(q.s.Jobs[parent.ID], &b); reason != "" {
				t.Fatal("explicit owner approval did not admit the candidate", reason)
			}
			// Experiments remain available for comparing candidate outputs.
			experiment := submit(t, q, variantSpec(e, "experiment"))
			if reason := q.variantParityReason(q.s.Jobs[experiment.ID], &a); reason != "" {
				t.Fatal("production list restricted an experiment", reason)
			}
			policy = q.PriorityPolicy()
			policy.Policy.Projects["test"].ProductionParityHosts[spec.workType()] = []string{}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
			if q.variantParityReason(q.s.Jobs[parent.ID], &b) == "" {
				t.Fatal("empty owner list admitted production")
			}
			checkJob := *q.s.Jobs[parent.ID]
			checkJob.Spec.ExtensionCheck = &ExtensionCheckRequest{Work: spec.workType()}
			if reason := q.variantParityReason(&checkJob, &b); reason != "" {
				t.Fatal("owner list prevented a native evidence check", reason)
			}
		})
	}
}
