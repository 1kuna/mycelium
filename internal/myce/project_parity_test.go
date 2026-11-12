package myce

import (
	"errors"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// setParityHosts approves the kinds of the named hosts for one work, as
// migration converts a previous-release host approval, for placement tests
// whose subject is not the evidence check. No hosts means an explicit empty
// list.
func setParityHosts(t *testing.T, q *Queue, project, work string, hosts ...string) {
	t.Helper()
	if err := q.update(func() error {
		if err := q.validateProductionParityHosts(map[string][]string{work: hosts}); err != nil {
			return err
		}
		q.migrateParityKinds()
		store := q.s.ParityKinds.clone()
		if store.Projects[project] == nil {
			store.Projects[project] = map[string][]ParityApproval{}
		}
		store.Projects[project][work] = q.convertHostApprovals(work, hostApprovals(hosts))
		q.s.ParityKinds = store
		q.mirrorProjectParity()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// setParityKinds approves machine kinds for one work directly.
func setParityKinds(t *testing.T, q *Queue, project, work string, kinds ...string) {
	t.Helper()
	if err := q.update(func() error {
		q.migrateParityKinds()
		store := q.s.ParityKinds.clone()
		if store.Projects[project] == nil {
			store.Projects[project] = map[string][]ParityApproval{}
		}
		list := []ParityApproval{}
		for _, text := range kinds {
			k, err := ParseMachineKind(text)
			if err != nil {
				return err
			}
			list = append(list, ParityApproval{Kind: k, Evidence: ParityEvidence{Migrated: true}})
		}
		store.Projects[project][work] = list
		q.s.ParityKinds = store
		q.mirrorProjectParity()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func clearParityHosts(t *testing.T, q *Queue, project string) {
	t.Helper()
	if err := q.update(func() error {
		q.migrateParityKinds()
		store := q.s.ParityKinds.clone()
		delete(store.Projects, project)
		q.s.ParityKinds = store
		q.mirrorProjectParity()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// parityFixture: two hosts of one kind and a third of another kind (a
// different CPU architecture), all qualified for kind:sample. The project has
// no approvals yet.
func parityFixture(t *testing.T) (q *Queue, clock *fakeClock, reference, sameKind, otherKind Machine) {
	t.Helper()
	q, clock = fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	reference = placementMachine("reference", "separate", 64, 8, e)
	sameKind = placementMachine("same-kind", "separate", 64, 8, e)
	otherKind = placementMachine("other-kind", "separate", 64, 8, e)
	otherKind.CapabilityProfile.Arch = otherArch()
	reference.HostID, sameKind.HostID, otherKind.HostID = "reference-host", "same-kind-host", "other-kind-host"
	for _, m := range []Machine{reference, sameKind, otherKind} {
		check(t, q, m, nil)
	}
	return q, clock, reference, sameKind, otherKind
}

// runComparison runs project's kind:sample check on m to the given outcome.
func runComparison(t *testing.T, q *Queue, clock *fakeClock, project string, m Machine, success bool) *Job {
	t.Helper()
	s := placementSpec(false)
	s.Project, s.Name, s.AllowedMachines = project, "parity-comparison", []string{m.ID}
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("comparison did not run on its host", assigned)
	}
	finish(t, q, assigned, success)
	clock.Advance(time.Second)
	return j
}

func parityRefusal(t *testing.T, err error, want string) {
	t.Helper()
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Code != "parity_evidence_required" || !strings.Contains(refusal.Message, want) || strings.Count(refusal.Message, "next:") != 1 || !strings.Contains(refusal.Message, "--evidence JOB_ID") {
		t.Fatalf("want one-action parity refusal containing %q, got %v", want, err)
	}
}

// otherArch is a release architecture other than this test host's.
func otherArch() string {
	if runtime.GOARCH == "amd64" {
		return "arm64"
	}
	return "amd64"
}

func kindLabels(approvals []ParityApproval) []string {
	out := []string{}
	for _, a := range approvals {
		out = append(out, a.label())
	}
	return out
}

// MY-51: a comparison on one host approves its kind of machine, so production
// runs on a second host of that kind that never ran a comparison; a host of
// another kind is refused with a reason naming both kinds.
func TestKindApprovalAdmitsEveryHostOfThatKind(t *testing.T) {
	q, clock, reference, sameKind, otherKind := parityFixture(t)
	revision := q.PriorityPolicy().Revision
	evidence := runComparison(t, q, clock, "test", reference, true)
	added := clock.Now()
	got, err := q.AddParity(ParityRequest{Project: "test", Work: "kind:sample", Evidence: evidence.ID})
	if err != nil {
		t.Fatal(err)
	}
	approved := runtime.GOOS + "/" + runtime.GOARCH + "/fake"
	approvals := got.Kinds["kind:sample"]
	if len(approvals) != 1 || approvals[0].Kind.String() != approved || approvals[0].Evidence.Job != evidence.ID || approvals[0].Evidence.Attempt != 1 || !approvals[0].AddedAt.Equal(added) || len(got.Warnings) != 0 {
		t.Fatalf("the evidence run's kind was not approved with its evidence: %+v", got)
	}
	if record := q.PriorityPolicy(); record.Revision != revision || len(record.Policy.Projects) != 0 {
		t.Fatalf("a project write must not edit queue policy: %+v", record)
	}
	production := placementSpec(false)
	production.Priority = "production"
	// Keep production off the comparison's own host, so it must run on a
	// second host of the kind or on the other kind.
	production.ExcludedMachines = []string{reference.ID}
	// Only the other kind may take this one, so its wait names the refusal.
	pinned := placementSpec(false)
	pinned.Priority = "production"
	pinned.AllowedMachines = []string{otherKind.ID}
	queued := submit(t, q, production)
	refused := submit(t, q, pinned)
	if assigned := check(t, q, otherKind, nil).Job; assigned != nil {
		t.Fatal("production ran on a machine kind the project did not approve", assigned.ID)
	}
	clock.Advance(time.Second)
	held, err := q.jobSnapshot(refused.ID)
	if err != nil {
		t.Fatal(err)
	}
	if held.Hold == nil || held.Hold.Kind != productionParityHostHold || held.Hold.Owner != "consumer" || !strings.Contains(held.Reason, targetLabel(machineTarget(&otherKind))+" is not a machine kind the project approved") {
		t.Fatalf("refusal of the other kind is unclear: reason=%q hold=%+v", held.Reason, held.Hold)
	}
	if assigned := check(t, q, sameKind, nil).Job; assigned == nil || assigned.ID != queued.ID {
		t.Fatal("production did not run on a second host of the approved kind", assigned)
	}
	// A broader kind the evidence ran as admits the other architecture too.
	if _, err := q.AddParity(ParityRequest{Project: "test", Work: "kind:sample", Kind: "fake", Evidence: evidence.ID}); err != nil {
		t.Fatal(err)
	}
	if assigned := check(t, q, otherKind, nil).Job; assigned == nil || assigned.ID != refused.ID {
		t.Fatal("a platform-wide approval did not admit a host of another architecture", assigned)
	}
	// Narrowing needs no evidence; the last removal keeps an explicit empty list.
	for _, kind := range []string{approved, "*/*/fake"} {
		if got, err = q.RemoveParity(ParityRequest{Project: "test", Work: "kind:sample", Kind: kind}); err != nil {
			t.Fatal(err)
		}
	}
	if list, configured := got.Kinds["kind:sample"]; !configured || len(list) != 0 {
		t.Fatalf("removing every kind must leave an empty list that admits none: %+v", got)
	}
}

func TestParityApprovalWithoutPassingEvidenceIsRefused(t *testing.T) {
	q, clock, reference, sameKind, otherKind := parityFixture(t)
	failed := runComparison(t, q, clock, "test", sameKind, false)
	passed := runComparison(t, q, clock, "test", reference, true)
	production := placementSpec(false)
	production.Priority = "production"
	production.AllowedMachines = []string{sameKind.ID}
	prod := submit(t, q, production)
	request := ParityRequest{Project: "test", Work: "kind:sample"}
	for _, tc := range []struct{ evidence, want string }{
		{"", "no evidence job named"},
		{"missing-job", "is unknown"},
		{failed.ID, "only a succeeded comparison counts"},
		{prod.ID, "is production work"},
	} {
		request.Evidence = tc.evidence
		_, err := q.AddParity(request)
		parityRefusal(t, err, tc.want)
	}
	// The comparison cannot approve a kind it did not run as.
	other := targetLabel(machineTarget(&otherKind))
	_, err := q.AddParity(ParityRequest{Project: "test", Work: "kind:sample", Kind: other, Evidence: passed.ID})
	parityRefusal(t, err, "which is not kind "+other)
	_, err = q.AddParity(ParityRequest{Project: "test", Work: "program:sample-script", Evidence: passed.ID})
	parityRefusal(t, err, "ran kind:sample, not program:sample-script")
	for _, kind := range []string{"*/*/*", "linux,mac", "cuda/x86"} {
		if _, err := q.AddParity(ParityRequest{Project: "test", Work: "kind:sample", Kind: kind, Evidence: passed.ID}); err == nil {
			t.Fatalf("kind %q accepted", kind)
		}
	}
	if got, _ := q.ProjectParity("test"); len(got.Kinds) != 0 {
		t.Fatalf("refused approvals changed the list: %+v", got)
	}
}

func TestAnotherProjectCannotEditThisProjectsApprovals(t *testing.T) {
	q, clock, reference, sameKind, _ := parityFixture(t)
	setParityKinds(t, q, "test", "kind:sample", "otheros/amd64/fake")
	theirs := runComparison(t, q, clock, "other", reference, true)
	_, err := q.AddParity(ParityRequest{Project: "test", Work: "kind:sample", Evidence: theirs.ID})
	parityRefusal(t, err, "belongs to another project")
	// Its own approval changes only its own list.
	if _, err := q.AddParity(ParityRequest{Project: "other", Work: "kind:sample", Evidence: theirs.ID}); err != nil {
		t.Fatal(err)
	}
	mine, _ := q.ProjectParity("test")
	if !slices.Equal(kindLabels(mine.Kinds["kind:sample"]), []string{"otheros/amd64/fake"}) {
		t.Fatalf("another project's write changed this project's list: %+v", mine)
	}
	if _, err := q.RemoveParity(ParityRequest{Project: "other", Work: "kind:sample", Kind: "otheros/amd64/fake"}); err == nil {
		t.Fatal("another project removed a kind from a list it does not own")
	}
	production := placementSpec(false)
	production.Priority = "production"
	submit(t, q, production)
	if assigned := check(t, q, sameKind, nil).Job; assigned != nil {
		t.Fatal("this project's production followed another project's list", assigned.ID)
	}
}

// Previous-release state names hosts. The first open converts each to its
// host's kinds without dropping one (a host that has not reported its kind
// stays approved by name until it checks in), keeps the host-form store and
// policy field as read-only rollback mirrors, and never converts again.
func TestHostApprovalsMigrateToKindsWithoutLoss(t *testing.T) {
	q, clock, reference, sameKind, otherKind := parityFixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{"reference-host": {}, "late-host": {}}); err != nil {
		t.Fatal(err)
	}
	evidence := runComparison(t, q, clock, "test", reference, true)
	if err := q.update(func() error {
		q.s.ParityKinds = nil
		q.s.ProjectParity = &projectParityStore{Projects: map[string]map[string][]ParityHost{"test": {
			"kind:sample":           {{Host: "reference-host", Evidence: ParityEvidence{Job: evidence.ID, Attempt: 1}}, {Host: "late-host"}},
			"program:sample-script": {},
		}}}
		q.s.Policies = map[string]ProjectPolicy{"test": {Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"reference-host", "late-host"}, "program:sample-script": {}}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	path, root := q.dbPathForTest(t), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	kind := targetLabel(machineTarget(&reference))
	got, _ := q.ProjectParity("test")
	approvals := got.Kinds["kind:sample"]
	if !slices.Equal(kindLabels(approvals), []string{"pending conversion of a host approval", runtime.GOOS + "/" + runtime.GOARCH + "/cpu", kind}) && !slices.Equal(kindLabels(approvals), []string{runtime.GOOS + "/" + runtime.GOARCH + "/cpu", kind, "pending conversion of a host approval"}) {
		t.Fatalf("host approvals not converted to kinds: %+v", approvals)
	}
	for _, a := range approvals {
		if !a.Evidence.Migrated || a.PendingHost == "" && (a.MigratedFrom != "reference-host" || a.Evidence.Job != evidence.ID) || a.PendingHost != "" && a.PendingHost != "late-host" {
			t.Fatalf("conversion lost its source or evidence: %+v", a)
		}
	}
	if list, ok := got.Kinds["program:sample-script"]; !ok || len(list) != 0 || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "waiting for its host to report its machine kind") {
		t.Fatalf("explicit empty list or pending warning lost: %+v", got)
	}
	// The rollback mirrors now list every host the kinds admit.
	mirror := []string{"late-host", "reference-host", "same-kind-host"}
	if !slices.Equal(parityHostNames(q.s.ProjectParity.Projects["test"]["kind:sample"]), mirror) || !slices.Equal(q.PriorityPolicy().Policy.Projects["test"].ProductionParityHosts["kind:sample"], mirror) {
		t.Fatalf("rollback mirrors: %+v %+v", q.s.ProjectParity.Projects["test"], q.PriorityPolicy().Policy.Projects["test"])
	}
	// Production runs on the converted kind's other host, not another kind.
	production := placementSpec(false)
	production.Priority = "production"
	queued := submit(t, q, production)
	if assigned := check(t, q, otherKind, nil).Job; assigned != nil {
		t.Fatal("migration admitted another kind", assigned.ID)
	}
	if assigned := check(t, q, sameKind, nil).Job; assigned == nil || assigned.ID != queued.ID {
		t.Fatal("migrated approval did not admit a host of the same kind", assigned)
	}
	// The late host keeps its approval by name until it reports its kind,
	// then converts at check-in.
	late := otherKind
	late.ID, late.HostID = "late", "late-host"
	check(t, q, late, nil)
	got, _ = q.ProjectParity("test")
	if labels := kindLabels(got.Kinds["kind:sample"]); slices.Contains(labels, "pending conversion of a host approval") || !slices.Contains(labels, targetLabel(machineTarget(&late))) || len(got.Warnings) != 0 {
		t.Fatalf("pending host approval not converted at check-in: %+v", got)
	}
	// The policy field is read-only: the export is accepted, a change refused.
	record := q.PriorityPolicy()
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal("unchanged export refused:", err)
	}
	record = q.PriorityPolicy()
	record.Policy.Projects["test"].ProductionParityHosts["kind:sample"] = []string{"reference-host"}
	var refusal *HTTPError
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); !errors.As(err, &refusal) || refusal.Code != "parity_hosts_project_owned" || !strings.Contains(refusal.Message, "machine kinds") {
		t.Fatalf("policy edit of project approvals accepted or unclear: %v", err)
	}
	// Reopening never converts the host form again over the kind store.
	if err := q.update(func() error {
		q.s.ProjectParity = &projectParityStore{Projects: map[string]map[string][]ParityHost{"test": {"kind:sample": {}}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if q, err = Open(path, root, clock); err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if got, _ := q.ProjectParity("test"); len(got.Kinds["kind:sample"]) != 4 {
		t.Fatalf("reopen converted the host form again: %+v", got)
	}
}

// A previous-release client names a host: the queue approves that host's
// kind from the evidence and warns (MY-42).
func TestHostNamedParityRequestIsConvertedWithWarning(t *testing.T) {
	q, clock, reference, sameKind, _ := parityFixture(t)
	evidence := runComparison(t, q, clock, "test", reference, true)
	_, err := q.AddParity(ParityRequest{Project: "test", Work: "kind:sample", Host: "same-kind-host", Evidence: evidence.ID})
	parityRefusal(t, err, "ran on host reference-host, not same-kind-host")
	got, err := q.AddParity(ParityRequest{Project: "test", Work: "kind:sample", Host: "reference-host", Evidence: evidence.ID})
	kind := targetLabel(machineTarget(&reference))
	if err != nil || !slices.Equal(kindLabels(got.Kinds["kind:sample"]), []string{kind}) || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "converted to machine kind "+kind) {
		t.Fatalf("host-named request not converted with a warning: %+v %v", got, err)
	}
	if got, err = q.RemoveParity(ParityRequest{Project: "test", Work: "kind:sample", Host: hostID(&sameKind)}); err != nil || len(got.Kinds["kind:sample"]) != 0 || len(got.Warnings) != 1 {
		t.Fatalf("host-named removal: %+v %v", got, err)
	}
}

func TestParseMachineKind(t *testing.T) {
	for text, want := range map[string]string{
		"cuda": "*/*/cuda", "mac": "darwin/*/*", "linux,x86": "linux/amd64/*", "arm,mlx": "*/arm64/metal",
		"linux/amd64/cuda": "linux/amd64/cuda", "*/aarch64/*": "*/arm64/*", " Darwin/ARM64/Metal ": "darwin/arm64/metal",
	} {
		k, err := ParseMachineKind(text)
		if err != nil || k.String() != want {
			t.Errorf("%q: got %v %v, want %s", text, k, err, want)
		}
	}
	for _, text := range []string{"", "*/*/*", "linux,mac", "a/b", "cu da"} {
		if _, err := ParseMachineKind(text); err == nil {
			t.Errorf("%q accepted", text)
		}
	}
}
