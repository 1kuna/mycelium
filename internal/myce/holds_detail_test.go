package myce

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestHoldsDetailKindsAndLegacyCompatibility(t *testing.T) {
	q, c := fixture(t)
	m := machine("worker", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	setMaintenance(t, q, "host", "disable")
	deadline := c.Now()
	q.s.Jobs["startup"] = &Job{ID: "startup", State: "failed", Spec: Spec{StartBy: &deadline}}
	r := HostRelease{Host: "host", Job: "startup", Machine: m.ID, Session: m.Session, Attempt: 1}
	q.s.HostReleases = map[string]HostRelease{releaseKey(r): r}
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]bool{}
	for _, d := range s.Holds {
		kinds[d.Kind] = true
		// Only a deliberate disable waits on the operator; a live startup
		// release clears itself.
		if d.operatorAction() != (d.Kind == "host_disabled") {
			t.Fatalf("operator fence: %+v", d)
		}
		if strings.Contains(d.Detail, "private reason") {
			t.Fatal("private reason exposed")
		}
	}
	if !kinds["host_disabled"] || !kinds["startup_release"] || len(hostHoldTexts(s.Holds)) != 0 {
		t.Fatalf("fences: %+v", s)
	}
	data, _ := json.Marshal(s)
	if strings.Contains(string(data), `"host_holds"`) {
		t.Fatal("empty legacy list must be omitted")
	}
	if !strings.Contains(string(data), `"holds":[`) {
		t.Fatal("missing details array")
	}
}

func TestHoldsDetailEmptyIsArray(t *testing.T) {
	q, _ := fixture(t)
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"holds":[]`) || strings.Contains(string(data), `"host_holds"`) {
		t.Fatalf("empty holds fields: %s", data)
	}
}

// The production fence evidence status used to list as host holds.
func hostHoldTexts(holds []Hold) []string {
	var out []string
	for _, h := range holds {
		if strings.HasPrefix(h.Kind, "production") || h.Kind == "unclassified" {
			out = append(out, h.Detail)
		}
	}
	return out
}
