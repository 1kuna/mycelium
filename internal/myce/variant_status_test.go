package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func variantsFixture(t *testing.T, cpu bool) (Extension, Machine, Machine) {
	t.Helper()
	e := compositionExtension(t, false)
	e.Variants[0].Target = ExecutionTarget{"linux", "amd64", "fake"}
	e.Variants[0].Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1, HostMemoryGB: 1}
	if cpu {
		v := e.Variants[0]
		v.Target.Platform = "cpu"
		v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1}
		e.Variants = append(e.Variants, v)
	}
	e = freezeComposition(t, e)
	a, b := valueMachine("accelerator", "fake", 32), valueMachine("general", "cpu", 16)
	for _, m := range []*Machine{&a, &b} {
		m.ExtensionFingerprint = "variant-fixture"
		if e.variant(machineTarget(m)) != nil {
			r := newExtensionReport(e, m, "extension:"+e.Name)
			r.Setup, r.Qualified, r.CheckID = true, true, "native-reference-check"
			m.ExtensionReports = []ExtensionReport{r}
		}
	}
	return e, a, b
}

func variantSpec(e Extension, class string) Spec {
	return Spec{Name: "variants", Project: "test", Class: class, Extension: extensionKey(e)}
}

func reopenVariants(t *testing.T, q *Queue, clock Clock) *Queue {
	t.Helper()
	database, root := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(database, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	fakeCheckpointAvailability(t, reopened, true)
	return reopened
}

func TestVariantStatusParityApproval(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		e, a, b := variantsFixture(t, true)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		check(t, q, a, nil)
		check(t, q, b, nil)
		j := submit(t, q, variantSpec(e, "production"))
		q = reopenVariants(t, q, clock)
		if !q.s.Extensions[extensionKey(e)].ProductionParityRequired || !q.s.Jobs[j.ID].Extensions[0].ProductionParityRequired {
			t.Fatal("restart lost production approval requirement")
		}
		for _, m := range []Machine{a, b} {
			if got := check(t, q, m, nil).Job; got != nil {
				t.Fatal("native qualification bypassed owner parity", got)
			}
		}
		got, err := q.jobSnapshot(j.ID)
		if err != nil || !strings.Contains(got.Reason, "MY-19") {
			t.Fatal("parity hold unexplained", got, err)
		}
		for _, r := range got.VariantEligibility {
			if r.Variant == r.Target && (!r.Qualified || r.Eligible || len(r.Reasons) != 1 || !strings.Contains(r.Reasons[0], "production_parity_hosts")) {
				t.Fatal("parity-only blocker not explicit", r)
			}
		}
		// An approval for unrelated work grants no placement. Checks/experiments
		// remain usable for evidence; only this exact work gets production approval.
		policy := q.PriorityPolicy()
		policy.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {hostID(&b)}}}
		if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
			t.Fatal(err)
		}
		if got := check(t, q, b, nil).Job; got != nil {
			t.Fatal("unrelated work approved production")
		}
		policy = q.PriorityPolicy()
		policy.Policy.Projects["test"] = ProjectPolicy{Weight: 1, ProductionParityHosts: map[string][]string{"extension:" + e.Name: {hostID(&b)}}}
		if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		assigned := check(t, q, b, nil).Job
		if assigned == nil || assigned.ID != j.ID {
			t.Fatal("approved fitting CPU variant not placed", assigned)
		}
		// Policy cannot manufacture an exact native check for a different identity.
		bad := b
		bad.ExtensionFingerprint = "changed"
		if qualifiedExtension(q.s.Jobs[j.ID], &bad) != nil {
			t.Fatal("approval manufactured native qualification")
		}
		checkJob := Job{Spec: variantSpec(e, "production"), Extensions: []Extension{q.s.Extensions[extensionKey(e)]}}
		checkJob.Spec.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:" + e.Name}
		if q.variantParityReason(&checkJob, &a) != "" {
			t.Fatal("parity blocked evidence check")
		}

	})

	t.Run("VariantStatusHistoricalRegistration", func(t *testing.T) {
		q, clock := fixture(t)
		e, a, b := variantsFixture(t, true)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		// Simulate a durable pre-gate registration (the absent bool decodes false).
		if err := q.update(func() error {
			old := q.s.Extensions[extensionKey(e)]
			old.ProductionParityRequired = false
			q.s.Extensions[extensionKey(e)] = old
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		check(t, q, a, nil)
		check(t, q, b, nil)
		j := submit(t, q, variantSpec(e, "production"))
		q = reopenVariants(t, q, clock)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		if q.s.Extensions[extensionKey(e)].ProductionParityRequired || q.s.Jobs[j.ID].Extensions[0].ProductionParityRequired {
			t.Fatal("replay changed historical admission")
		}
		if got := check(t, q, b, nil).Job; got == nil || got.ID != j.ID {
			t.Fatal("historical job stranded")
		}

	})

	t.Run("VariantStatusMixedRegistrationParity", func(t *testing.T) {
		for _, state := range []struct{ legacy, added bool }{{true, false}, {true, true}, {false, true}} {
			t.Run(fmt.Sprintf("legacy=%t,new=%t", state.legacy, state.added), func(t *testing.T) {
				q, clock := fixture(t)
				added, _, b := variantsFixture(t, true)
				if err := q.AddExtension(added); err != nil {
					t.Fatal(err)
				}
				legacy, _, _ := variantsFixture(t, true)
				legacy.Name = "z-legacy"
				legacy.Variants = legacy.Variants[1:] // one historical-contract alternative
				delete(legacy.Variants[0].Baselines, "extension:"+added.Name)
				legacy = freezeComposition(t, legacy)
				if err := q.AddExtension(legacy); err != nil {
					t.Fatal(err)
				}
				b.ExtensionReports = nil
				for _, item := range []struct {
					e         Extension
					qualified bool
				}{{added, state.added}, {legacy, state.legacy}} {
					r := newExtensionReport(item.e, &b, "kind:sample")
					r.Setup, r.Qualified, r.CheckID = true, item.qualified, "native-check"
					b.ExtensionReports = append(b.ExtensionReports, r)
				}
				check(t, q, b, nil)
				j := submit(t, q, Spec{Name: "mixed", Project: "test", Kind: "sample", Class: "production"})
				run := check(t, q, b, nil).Job
				if !state.legacy {
					if run != nil {
						t.Fatal("unapproved new alternative admitted", run)
					}
					return
				}
				if run == nil || run.ID != j.ID || run.Extension == nil || run.Extension.Digest != legacy.Digest {
					t.Fatal("qualified legacy alternative blocked or wrong digest selected", run)
				}
				if err := q.Started(result(run, true)); err != nil {
					t.Fatal("selected legacy digest blocked at start", err)
				}
				q = reopenVariants(t, q, clock)
				if reason := q.variantParityReason(q.s.Jobs[j.ID], &b); reason != "" {
					t.Fatal("selected legacy digest blocked after restart", reason)
				}
			})
		}

	})
}

func TestVariantStatusDev(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		root := newTestStateRoot(t)
		e, _, _ := variantsFixture(t, true)
		for i := range e.Variants {
			e.Variants[i].Target.OS, e.Variants[i].Target.Arch = runtime.GOOS, runtime.GOARCH
			e.Variants[i].Execution.Main.Command = []string{"sh", "-ec", "test -z \"$MYCE_TOKEN\"; printf 'verified variants\\n' > output"}
		}
		e.Outputs = []string{"output"}
		e = freezeComposition(t, e)
		d, err := StartDev(DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "files"), ExtensionRoot: filepath.Join(root, "extensions"), Token: "variant-dev-fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		client := d.Client()
		var registration ExtensionRegistration
		if err := client.Call(ctx, "POST", "/api/extensions", e, &registration); err != nil {
			t.Fatal(err)
		}
		j, err := client.Submit(ctx, variantSpec(e, "experiment"))
		if err != nil {
			t.Fatal(err)
		}
		completed, err := client.Wait(ctx, j.ID)
		if err != nil || completed.State != "succeeded" {
			t.Fatal("fresh unpinned variants failed", completed, err)
		}
		destination := filepath.Join(root, "accepted-output")
		if err := client.DownloadResult(ctx, j.ID, "output", destination); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(destination)
		if err != nil || string(data) != "verified variants\n" {
			t.Fatal("artifact not verified", err)
		}
		if len(completed.Spec.AllowedMachines) != 0 {
			t.Fatal("dev proof used host pins")
		}

	})

	t.Run("VariantStatusPlacementAndAPI", func(t *testing.T) {
		q, clock := fixture(t)
		e, a, b := variantsFixture(t, true)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		check(t, q, a, nil)
		check(t, q, b, nil)
		j := submit(t, q, variantSpec(e, "experiment"))
		stored := cloneJob(q.s.Jobs[j.ID])
		server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
		defer server.Close()
		c := Client{URL: server.URL, Token: "fixture"}
		for _, endpoint := range []string{"/api/jobs/" + j.ID, "/api/status"} {
			var got Job
			if strings.HasPrefix(endpoint, "/api/jobs/") {
				if err := c.Call(context.Background(), "GET", endpoint, nil, &got); err != nil {
					t.Fatal(err)
				}
			} else {
				var s Snapshot
				if err := c.Call(context.Background(), "GET", endpoint, nil, &s); err != nil {
					t.Fatal(err)
				}
				got = *s.Jobs[0]
			}
			if len(got.VariantEligibility) != 4 {
				t.Fatalf("missing per-host matrix: %+v", got.VariantEligibility)
			}
			eligible := 0
			for _, r := range got.VariantEligibility {
				if r.Eligible {
					eligible++
				}
				if !r.Eligible && len(r.Reasons) == 0 {
					t.Fatal("unexplained variant", r)
				}
			}
			if eligible != 2 {
				t.Fatalf("qualified variants lost eligibility: %+v", got.VariantEligibility)
			}
			data, _ := json.Marshal(got)
			if bytes.Contains(data, []byte("run.sh")) {
				t.Fatal("recipe leaked")
			}
			var rendered bytes.Buffer
			if err := statusPage.Execute(&rendered, Snapshot{Now: clock.Now(), Jobs: []*Job{&got}}); err != nil || !strings.Contains(rendered.String(), "Variant eligibility by host") {
				t.Fatal("status page omitted variants", err)
			}
		}
		if !reflect.DeepEqual(stored, cloneJob(q.s.Jobs[j.ID])) {
			t.Fatal("read mutated frozen job")
		}
		if got := check(t, q, a, nil).Job; got != nil {
			t.Fatal("scarce pull stole work from qualified CPU variant")
		}
		assigned := check(t, q, b, nil).Job
		if assigned == nil || assigned.ID != j.ID || assigned.Spec.Needs.cores() != 1 || !assigned.Spec.Needs.cpuOnly() || len(assigned.Spec.AllowedMachines) != 0 {
			t.Fatal("un-pinned CPU variant not selected", assigned)
		}
		if err := q.Started(result(assigned, true)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(3 * time.Second)
		got, err := c.GetJob(context.Background(), j.ID)
		if err != nil || len(got.VariantEligibility) != 4 {
			t.Fatal("started receipt lost variants", err)
		}

	})
}
