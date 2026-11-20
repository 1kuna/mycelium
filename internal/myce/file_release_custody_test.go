package myce

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestFileReleaseCustodyDependencyChain(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	recovery, _, ref := releaseTerminal(t, q, c, "recovery")
	eval, _, _ := releaseTerminal(t, q, c, "evaluation")
	serving, _, _ := releaseTerminal(t, q, c, "serving")
	if err := q.update(func() error {
		for _, id := range []string{recovery.ID, eval.ID} {
			j := q.touchJob(id)
			j.Attempt, j.ReleaseConfirmed = 1, true
			j.Copies = []Copy{{Attempt: 1, ReleaseConfirmed: true}}
		}
		q.touchJob(eval.ID).Spec.After = &PieceDependency{Job: recovery.ID, Outputs: map[string]string{"upstream": "result"}}
		j := q.touchJob(serving.ID)
		j.Spec.After = &PieceDependency{Job: eval.ID, Outputs: map[string]string{"upstream": "result"}}
		j.State, j.Attempt, j.FinishedAt = "running", 1, nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Second)
	p := releasePlan(t, q, releaseSelection(recovery))
	want := []FileReleaseCustodyBlocker{{JobID: serving.ID, State: "running", Path: []FileReleaseCustodyStep{
		{Relationship: "after", JobID: eval.ID}, {Relationship: "after", JobID: serving.ID},
	}}}
	if !reflect.DeepEqual(p.Jobs[0].CustodyBlockers, want) || !slices.Contains(p.Jobs[0].Blockers, "physical_custody") || p.Jobs[0].Release != "pending" || p.ReclaimableBytes != 0 {
		t.Fatalf("transitive custody not explained: %+v", p)
	}
	if again := releasePlan(t, q, p.Selection); again.ID != p.ID {
		t.Fatal("blocker ordering changed plan identity")
	}
	_, err := releaseApply(q, p)
	releaseCode(t, err, "file_release_protected")
	if _, err := os.Stat(q.files.blob(ref)); err != nil || job(t, q, recovery.ID).FilesExpired {
		t.Fatal("diagnostics changed custody", err)
	}
	if err := q.update(func() error { q.touchJob(serving.ID).ReleaseConfirmed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	p = releasePlan(t, q, p.Selection)
	if len(p.Jobs[0].CustodyBlockers) != 0 || len(p.Jobs[0].Blockers) != 0 || p.ReclaimableBytes != ref.SizeBytes {
		t.Fatal("confirmed descendant still blocks release", p)
	}
}

func TestFileReleaseCustodyFanOutAndPrivacy(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	parent, _, _ := releaseTerminal(t, q, c, "parent")
	child, _, _ := releaseTerminal(t, q, c, "child")
	foreign, _, _ := releaseTerminal(t, q, c, "foreign")
	if err := q.update(func() error {
		p := q.touchJob(parent.ID)
		p.Attempt = 1 // Own custody must be reported alongside every member.
		p.FanOut = &FanOutProgress{Children: []string{child.ID, foreign.ID}}
		j := q.touchJob(child.ID)
		j.Copies = []Copy{{Attempt: 1}} // An old, unconfirmed execution.
		// An alias edge must not duplicate this blocker.
		j.Spec.After = &PieceDependency{Job: parent.ID}
		j = q.touchJob(foreign.ID)
		j.Spec.Project, j.State = "private-project", "private-state"
		q.s.HostReleases = map[string]HostRelease{"pending": {Job: foreign.ID}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	p := releasePlan(t, q, releaseSelection(parent))
	got := p.Jobs[0].CustodyBlockers
	if len(got) != 3 || got[0].JobID != parent.ID || len(got[0].Path) != 0 {
		t.Fatal("missing own/member custody", got)
	}
	var own, other bool
	for _, b := range got[1:] {
		if b.JobID == child.ID {
			own = b.State == "succeeded" && reflect.DeepEqual(b.Path, []FileReleaseCustodyStep{{Relationship: "after", JobID: child.ID}})
		}
		if b.OtherProject {
			other = b.JobID == "" && b.State == "" && reflect.DeepEqual(b.Path, []FileReleaseCustodyStep{{Relationship: "fan_out", OtherProject: true}})
		}
	}
	data, err := json.Marshal(got)
	if err != nil || !own || !other {
		t.Fatal("incorrect member diagnostics", string(data), err)
	}
	for _, private := range []string{foreign.ID, "private-project", "private-state"} {
		if strings.Contains(string(data), private) {
			t.Fatal("foreign job details leaked", string(data))
		}
	}
}
