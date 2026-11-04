package myce

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestVariantStatusPlacementAndAPI(t *testing.T) {
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
}

func TestVariantStatusParityApproval(t *testing.T) {
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
}

func TestVariantStatusHistoricalRegistration(t *testing.T) {
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
}

func TestVariantStatusMissingVariantAdvice(t *testing.T) {
	q, clock := fixture(t)
	e, a, b := variantsFixture(t, false)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	busy := spec("experiment")
	busy.AllowedMachines = []string{a.ID}
	submit(t, q, busy)
	running := check(t, q, a, nil).Job
	if running == nil {
		t.Fatal("contention fixture not assigned")
	}
	j := submit(t, q, variantSpec(e, "experiment"))
	got, err := q.jobSnapshot(j.ID)
	if err != nil || len(got.VariantAdvice) != 1 || !strings.Contains(got.VariantAdvice[0], "linux/amd64/cpu") || !strings.Contains(got.VariantAdvice[0], "MY-19") {
		t.Fatal("missing CPU variant blocker advice", got, err)
	}
	large := variantSpec(e, "experiment")
	large.SizeHint.HostMemoryGB = 20 // scarce host fits, the idle CPU host cannot
	largeJob := submit(t, q, large)
	largeStatus, err := q.jobSnapshot(largeJob.ID)
	if err != nil || len(largeStatus.VariantAdvice) != 0 {
		t.Fatal("nonfitting RAM alternative advised", largeStatus, err)
	}
	// Large CPU envelopes and stale inventory must not suggest unsupported
	// destinations or treat old inventory as idle capacity.
	clock.Advance(LossAfter + time.Second)
	check(t, q, a, running)
	got, err = q.jobSnapshot(j.ID)
	if err != nil || len(got.VariantAdvice) != 0 {
		t.Fatal("stale destination advised", got, err)
	}
	for _, r := range got.VariantEligibility {
		if r.Host == b.HostID && (r.Eligible || r.Freshness == "fresh") {
			t.Fatal("stale host eligible", r)
		}
	}
}

func TestVariantStatusRegistrationWarnings(t *testing.T) {
	q, _ := fixture(t)
	e, a, b := variantsFixture(t, false)
	e.RequirementReasons = map[string]string{"backend:fake": "genuine requirement; private author evidence"}
	e = freezeComposition(t, e)
	check(t, q, a, nil)
	check(t, q, b, nil)
	server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	var registered ExtensionRegistration
	if err := c.Call(context.Background(), "POST", "/api/extensions", e, &registered); err != nil || !registered.OK {
		t.Fatal("hard requirement refused", registered, err)
	}
	text := strings.Join(registered.Warnings, " ")
	if !strings.Contains(text, "Missing variant for observed linux/amd64/cpu") || !strings.Contains(text, "still needs exact native qualification") || strings.Contains(text, "private author evidence") {
		t.Fatal("coverage/qualification warnings missing or leaked", registered)
	}
	if len(q.s.Jobs) != 0 || q.s.Extensions[extensionKey(e)].Digest != e.Digest {
		t.Fatal("warning executed work or changed identity")
	}
}

func TestVariantStatusDev(t *testing.T) {
	root := newTestStateRoot(t)
	e, _, _ := variantsFixture(t, true)
	for i := range e.Variants {
		e.Variants[i].Target.OS, e.Variants[i].Target.Arch = runtime.GOOS, runtime.GOARCH
		e.Variants[i].Execution.Main.Command = []string{"sh", "-ec", "test -z \"$MYCE_TOKEN\"; printf 'verified variants\\n' > output"}
	}
	e.Outputs = []string{"output"}
	e = freezeComposition(t, e)
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "files"), ExtensionRoot: filepath.Join(root, "extensions"), Token: "variant-dev-fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
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
}
