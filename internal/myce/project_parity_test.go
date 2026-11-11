package myce

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// setParityHosts writes one work's project list directly, as a migrated entry,
// for placement tests whose subject is not the evidence check. No hosts means
// an explicit empty list.
func setParityHosts(t *testing.T, q *Queue, project, work string, hosts ...string) {
	t.Helper()
	if hosts == nil {
		hosts = []string{}
	}
	if err := q.update(func() error {
		if err := q.validateProductionParityHosts(map[string][]string{work: hosts}); err != nil {
			return err
		}
		q.migrateProjectParity()
		store := q.s.ProjectParity.clone()
		if store.Projects[project] == nil {
			store.Projects[project] = map[string][]ParityHost{}
		}
		store.Projects[project][work] = migratedParityHosts(map[string][]string{work: hosts})[work]
		q.s.ProjectParity = store
		q.mirrorProjectParity()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func clearParityHosts(t *testing.T, q *Queue, project string) {
	t.Helper()
	if err := q.update(func() error {
		q.migrateProjectParity()
		store := q.s.ProjectParity.clone()
		delete(store.Projects, project)
		q.s.ProjectParity = store
		q.mirrorProjectParity()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// parityFixture: production of kind:sample may run on reference-host only;
// candidate-host is qualified but not yet on the project's list.
func parityFixture(t *testing.T) (*Queue, *fakeClock, Machine, Machine) {
	t.Helper()
	q, clock := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	reference := placementMachine("reference", "separate", 64, 8, e)
	candidate := placementMachine("candidate", "separate", 64, 8, e)
	reference.HostID, candidate.HostID = "reference-host", "candidate-host"
	check(t, q, reference, nil)
	check(t, q, candidate, nil)
	setParityHosts(t, q, "test", "kind:sample", "reference-host")
	return q, clock, reference, candidate
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

func TestProjectWidensOwnParityListWithEvidence(t *testing.T) {
	q, clock, _, candidate := parityFixture(t)
	revision := q.PriorityPolicy().Revision
	evidence := runComparison(t, q, clock, "test", candidate, true)
	production := placementSpec(false)
	production.Priority = "production"
	queued := submit(t, q, production)
	if assigned := check(t, q, candidate, nil).Job; assigned != nil {
		t.Fatal("production ran on a host outside the project's list", assigned.ID)
	}
	added := clock.Now()
	got, err := q.AddParityHost(ParityHostRequest{Project: "test", Work: "kind:sample", Host: "candidate-host", Evidence: evidence.ID})
	if err != nil {
		t.Fatal(err)
	}
	hosts := got.Hosts["kind:sample"]
	if len(hosts) != 2 || hosts[1].Host != "candidate-host" || hosts[1].Evidence.Job != evidence.ID || hosts[1].Evidence.Attempt != 1 || !hosts[1].AddedAt.Equal(added) || !hosts[0].Evidence.Migrated {
		t.Fatalf("addition not recorded with its evidence: %+v", got)
	}
	record := q.PriorityPolicy()
	if record.Revision != revision || len(record.Policy.Projects) != 0 {
		t.Fatalf("a project write must not edit queue policy: %+v", record)
	}
	if assigned := check(t, q, candidate, nil).Job; assigned == nil || assigned.ID != queued.ID {
		t.Fatal("production did not place on the newly approved host", assigned)
	}
	// Narrowing needs no evidence; the last removal keeps an explicit empty list.
	for _, host := range []string{"reference-host", "candidate-host"} {
		if got, err = q.RemoveParityHost(ParityHostRequest{Project: "test", Work: "kind:sample", Host: host}); err != nil {
			t.Fatal(err)
		}
	}
	if list, configured := got.Hosts["kind:sample"]; !configured || len(list) != 0 {
		t.Fatalf("removing every host must leave an empty list that admits none: %+v", got)
	}
}

func TestParityAdditionWithoutPassingEvidenceIsRefused(t *testing.T) {
	q, clock, reference, candidate := parityFixture(t)
	failed := runComparison(t, q, clock, "test", candidate, false)
	elsewhere := runComparison(t, q, clock, "test", reference, true)
	production := placementSpec(false)
	production.Priority = "production"
	production.AllowedMachines = []string{candidate.ID}
	prod := submit(t, q, production)
	request := ParityHostRequest{Project: "test", Work: "kind:sample", Host: "candidate-host"}
	for _, tc := range []struct{ evidence, want string }{
		{"", "no evidence job named"},
		{"missing-job", "is unknown"},
		{failed.ID, "only a succeeded comparison counts"},
		{elsewhere.ID, "ran on host reference-host, not candidate-host"},
		{prod.ID, "is production work"},
	} {
		request.Evidence = tc.evidence
		_, err := q.AddParityHost(request)
		parityRefusal(t, err, tc.want)
	}
	other := request
	other.Work, other.Evidence = "program:sample-script", elsewhere.ID
	_, err := q.AddParityHost(other)
	parityRefusal(t, err, "ran kind:sample, not program:sample-script")
	if got, _ := q.ProjectParityHosts("test"); !slices.Equal(parityHostNames(got.Hosts["kind:sample"]), []string{"reference-host"}) {
		t.Fatalf("refused additions changed the list: %+v", got)
	}
}

func TestAnotherProjectCannotEditThisProjectsParityList(t *testing.T) {
	q, clock, _, candidate := parityFixture(t)
	theirs := runComparison(t, q, clock, "other", candidate, true)
	// Another project's passing comparison cannot widen this project's list.
	_, err := q.AddParityHost(ParityHostRequest{Project: "test", Work: "kind:sample", Host: "candidate-host", Evidence: theirs.ID})
	parityRefusal(t, err, "belongs to another project")
	// Its own addition changes only its own list.
	if _, err := q.AddParityHost(ParityHostRequest{Project: "other", Work: "kind:sample", Host: "candidate-host", Evidence: theirs.ID}); err != nil {
		t.Fatal(err)
	}
	mine, _ := q.ProjectParityHosts("test")
	if !slices.Equal(parityHostNames(mine.Hosts["kind:sample"]), []string{"reference-host"}) {
		t.Fatalf("another project's write changed this project's list: %+v", mine)
	}
	if _, err := q.RemoveParityHost(ParityHostRequest{Project: "other", Work: "kind:sample", Host: "reference-host"}); err == nil {
		t.Fatal("another project removed a host from a list it does not own")
	}
	production := placementSpec(false)
	production.Priority = "production"
	submit(t, q, production)
	if assigned := check(t, q, candidate, nil).Job; assigned != nil {
		t.Fatal("this project's production followed another project's list", assigned.ID)
	}
}

// Previous-release state keeps its policy lists; the first open copies them
// to the project store, and the policy field becomes a read-only mirror.
func TestProjectParityMigratesAndPolicyFieldIsReadOnly(t *testing.T) {
	q, c := fixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{"approved-host": {}, "other-host": {}}); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		q.s.ProjectParity = nil
		q.s.Policies = map[string]ProjectPolicy{"test": {Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"approved-host"}}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	path, root := q.dbPathForTest(t), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	got, _ := q.ProjectParityHosts("test")
	if hosts := got.Hosts["kind:sample"]; len(hosts) != 1 || hosts[0].Host != "approved-host" || !hosts[0].Evidence.Migrated {
		t.Fatalf("policy list not copied to the project store: %+v", got)
	}
	record := q.PriorityPolicy()
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal("unchanged export refused:", err)
	}
	record = q.PriorityPolicy()
	record.Policy.Projects["test"] = ProjectPolicy{Weight: 2}
	if record, err = q.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal("omitted field refused:", err)
	}
	if !slices.Equal(record.Policy.Projects["test"].ProductionParityHosts["kind:sample"], []string{"approved-host"}) {
		t.Fatalf("omitting the read-only field dropped the project list: %+v", record.Policy.Projects["test"])
	}
	record.Policy.Projects["test"].ProductionParityHosts["kind:sample"] = []string{"other-host"}
	var refusal *HTTPError
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); !errors.As(err, &refusal) || refusal.Code != "parity_hosts_project_owned" || !strings.Contains(refusal.Message, "myce parity add --project test") {
		t.Fatalf("policy edit of a project list accepted or unclear: %v", err)
	}
	if got, _ := q.ProjectParityHosts("test"); !slices.Equal(parityHostNames(got.Hosts["kind:sample"]), []string{"approved-host"}) {
		t.Fatalf("exported slices or a refused set changed the live list: %+v", got)
	}
	// Reopening never re-copies the policy field over the project store.
	if err := q.update(func() error {
		policies := map[string]ProjectPolicy{"test": {Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"other-host"}}}}
		q.s.Policies = policies
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if q, err = Open(path, root, c); err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if got, _ := q.ProjectParityHosts("test"); !slices.Equal(parityHostNames(got.Hosts["kind:sample"]), []string{"approved-host"}) {
		t.Fatalf("reopen re-copied the policy field: %+v", got)
	}
}
