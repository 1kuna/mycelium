package myce

import (
	"strings"
	"testing"
)

func TestResolveNativeAuthoredMetadataRemainsUnqualified(t *testing.T) {
	q, _, _, s := observationFixture(t)
	e := q.s.Extensions[s.Extension]
	// Even a caller-supplied registration digest cannot turn local metadata into
	// qualified evidence. The existing portability reader clears it.
	before := string(wireJSON(t, q.s))
	r, err := q.Resolve(ResolveRequest{Spec: Spec{Project: s.Project}, Recipe: &e})
	if err != nil {
		t.Fatal(err)
	}
	if r.Portability == nil || len(r.Targets) == 0 {
		t.Fatalf("native metadata not resolved: %+v", r)
	}
	for _, target := range r.Targets {
		if target.Eligible || !strings.Contains(target.Reason, "frozen native registration") {
			t.Fatalf("unfrozen metadata claimed eligible: %+v", target)
		}
	}
	if after := string(wireJSON(t, q.s)); before != after {
		t.Fatal("native resolution mutated queue")
	}
}
