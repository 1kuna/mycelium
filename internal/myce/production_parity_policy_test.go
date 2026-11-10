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
					if !row.Qualified || row.Eligible || !strings.Contains(strings.Join(row.Reasons, ";"), "held: host not in production parity list") {
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
			q.mu.Lock()
			known := q.productionParityKnown(q.s.Jobs[parent.ID], &b)
			q.mu.Unlock()
			if known {
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
			policy = q.PriorityPolicy()
			policy.Policy.Projects["test"].ProductionParityHosts[spec.workType()] = []string{}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
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

func TestProductionParityPolicyRebindsQueuedAfterRestart(t *testing.T) {
	for _, keepHolds := range []bool{true, false} {
		name := "holds enabled"
		if !keepHolds {
			name = "holds removed"
		}
		t.Run(name, func(t *testing.T) {
			q, clock := fixture(t)
			// Inspect saved bindings without Snapshot's maintenance/rebinding.
			savedJob := func(id string) Job {
				q.mu.Lock()
				defer q.mu.Unlock()
				return cloneJob(q.s.Jobs[id])
			}
			e, a, b := variantsFixture(t, true)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			check(t, q, a, nil)
			check(t, q, b, nil)
			policy := q.PriorityPolicy()
			policy.Policy.Hosts = map[string]HostPolicy{hostID(&a): {ProductionHold: true}, hostID(&b): {ProductionHold: true}}
			policy.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"extension:" + e.Name: {hostID(&a)}}}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
			first := submit(t, q, variantSpec(e, "production"))
			running := check(t, q, a, nil).Job
			if running == nil || running.ID != first.ID {
				t.Fatal("reference production did not start")
			}
			if err := q.Started(result(running, true)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Millisecond)
			queued := submit(t, q, variantSpec(e, "production"))
			if savedJob(queued.ID).ProductionHost != hostID(&a) {
				t.Fatal("queued production was not bound to the reference host")
			}
			policy = q.PriorityPolicy()
			policy.Policy.Projects["test"].ProductionParityHosts[queued.Spec.workType()] = []string{hostID(&b)}
			if !keepHolds {
				policy.Policy.Hosts = map[string]HostPolicy{}
			}
			if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
				t.Fatal(err)
			}
			// The policy write persists without scheduling. Reopen with the
			// old binding still present, as it can be during an upgrade.
			if savedJob(queued.ID).ProductionHost != hostID(&a) {
				t.Fatal("fixture did not preserve the invalid saved binding")
			}
			clock.Advance(time.Second)
			q = reopenVariants(t, q, clock)
			assigned := check(t, q, b, nil).Job
			if assigned == nil || assigned.ID != queued.ID {
				t.Fatal("newly approved idle host could not take queued production", job(t, q, queued.ID))
			}
			if keepHolds && savedJob(queued.ID).ProductionHost != hostID(&b) {
				t.Fatal("new hold binding was not committed")
			}
			original := savedJob(running.ID)
			if original.State != "running" || original.ProductionHost != hostID(&a) || original.Machine != a.ID || original.Attempt != running.Attempt || original.Session != running.Session || original.StopAt != nil {
				t.Fatal("policy change altered running execution custody", original)
			}
		})
	}
}

func TestProductionParityPolicyLegacyWaitDiagnostic(t *testing.T) {
	q, clock := fixture(t)
	e := fakeExtension(t) // No variants or server-owned parity requirement.
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("candidate", "separate", 64, 8, e)
	check(t, q, m, nil)
	policy := q.PriorityPolicy()
	policy.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {}}}
	if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
		t.Fatal(err)
	}
	spec := placementSpec(false)
	spec.Priority = "production"
	queued := submit(t, q, spec)
	if assigned := check(t, q, m, nil).Job; assigned != nil {
		t.Fatal("empty parity list admitted production", assigned.ID)
	}
	clock.Advance(time.Second)
	got, err := q.jobSnapshot(queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Reason, "held: host not in production parity list") || len(got.VariantEligibility) != 0 || got.Blocker == nil || got.Blocker.Code != productionParityHostHold || got.Blocker.Owner != "queue owner" {
		t.Fatalf("legacy parity-only hold is not actionable: reason=%q blocker=%+v", got.Reason, got.Blocker)
	}
	policy = q.PriorityPolicy()
	policy.Policy.Projects["test"].ProductionParityHosts[spec.workType()] = []string{hostID(&m)}
	if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
		t.Fatal(err)
	}
	if assigned := check(t, q, m, nil).Job; assigned == nil || assigned.ID != queued.ID {
		t.Fatal("owner approval did not release the held legacy job", assigned)
	}
}
