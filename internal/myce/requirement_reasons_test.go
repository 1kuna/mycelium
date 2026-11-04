package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRequirementReasonsFrozenAndProjected(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.AllowedMachines = []string{"worker-a"}
	s.SubmissionKey = "reasons"
	s.RequirementReasons = map[string]string{"host": "private explanation /private/data", "private-key": "private-secret", "device": " "}
	j := submit(t, q, s)
	s.RequirementReasons["host"] = "mutated after submission"
	if q.s.Jobs[j.ID].Spec.RequirementReasons["host"] != "private explanation /private/data" {
		t.Fatal("caller mutated frozen explanation")
	}
	// Persisted explanations survive age/restart; observations do not publish
	// their prose or arbitrary keys, and redacting twice retains safe metadata.
	database, root := strings.TrimSuffix(q.lock.Name(), ".lock"), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(24 * time.Hour)
	var err error
	q, err = Open(database, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	for _, endpoint := range []string{"/api/jobs/" + j.ID, "/api/status"} {
		var raw json.RawMessage
		if err := c.Call(context.Background(), "GET", endpoint, nil, &raw); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "private") || strings.Contains(string(raw), "mutated") {
			t.Fatal("raw reason or arbitrary key leaked")
		}
		var got Job
		if endpoint == "/api/status" {
			var snapshot Snapshot
			if err := json.Unmarshal(raw, &snapshot); err != nil || len(snapshot.Jobs) != 1 {
				t.Fatal("status", err)
			}
			got = *snapshot.Jobs[0]
		} else if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Spec.RequirementReasons) != 0 || got.SpecProjection == nil || !slices.Contains(got.SpecProjection.Omitted, "requirement_reasons") {
			t.Fatal("raw spec map not marked omitted")
		}
		if len(got.RequirementReasons) != 3 {
			t.Fatal("missing safe metadata", got.RequirementReasons)
		}
		for _, row := range got.RequirementReasons {
			if row.Constraint == "host" && (row.ReasonSource != "authored" || row.Source != "spec.requirement_reasons[host]") || row.Constraint == "device" && row.ReasonSource != "missing" {
				t.Fatal("incorrect presence/provenance", row)
			}
		}
		if twice := statusJobCopy(&got); !slices.Equal(twice.RequirementReasons, got.RequirementReasons) {
			t.Fatal("repeated projection lost metadata")
		}
	}
	if q.s.Jobs[j.ID].Spec.RequirementReasons["host"] != "private explanation /private/data" {
		t.Fatal("read or restart changed frozen map")
	}
}

func TestRequirementReasonsNativeAndMissingPin(t *testing.T) {
	q, _ := fixture(t)
	e := compositionExtension(t, false)
	e.RequirementReasons = map[string]string{"host": "private native reason"}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "native", Project: "test", Extension: extensionKey(e), AllowedMachines: []string{"worker-a"}}
	j := submit(t, q, s)
	got, err := q.jobSnapshot(j.ID)
	if err != nil || len(got.RequirementReasons) != 1 || got.RequirementReasons[0] != (RequirementReasonStatus{"host", "authored", "extensions[digest=" + e.Digest + "].requirement_reasons[host]"}) {
		t.Fatal("native snapshot provenance", got.RequirementReasons, err)
	}
	if q.s.Jobs[j.ID].Extensions[0].RequirementReasons["host"] != "private native reason" {
		t.Fatal("native explanation not frozen with job")
	}
	plain := spec("experiment")
	plain.AllowedMachines = []string{"worker-a"}
	missing := submit(t, q, plain)
	got, err = q.jobSnapshot(missing.ID)
	if err != nil || len(got.RequirementReasons) != 1 || got.RequirementReasons[0].ReasonSource != "missing" {
		t.Fatal("missing pin reason invisible", got.RequirementReasons, err)
	}
}

func TestRequirementReasonsResolveAndPortability(t *testing.T) {
	q, _ := fixture(t)
	s := cpuResolveSpec()
	s.AllowedMachines = []string{"worker-a"}
	s.RequirementReasons = map[string]string{"host": "private authored explanation"}
	for _, item := range []struct {
		name    string
		legacy  map[string]string
		missing bool
	}{{"shared spec", nil, false}, {"legacy request", map[string]string{"host": "private legacy explanation"}, false}, {"explicit legacy override", map[string]string{}, true}} {
		t.Run(item.name, func(t *testing.T) {
			r, err := q.Resolve(ResolveRequest{Spec: s, RequirementReasons: item.legacy})
			if err != nil || strings.Contains(strings.Join(r.Notes, " "), "Host pin has no declared") != item.missing {
				t.Fatal("resolve ignored authored explanation/legacy override", r.Notes, err)
			}
			p, err := q.Portability(PortabilityRequest{Spec: &s, RequirementReasons: item.legacy})
			data, _ := json.Marshal(p)
			if err != nil || strings.Contains(strings.Join(p.Notes, " "), "Host pin has no declared") != item.missing || strings.Contains(string(data), "private") {
				t.Fatal("portability ignored explanation or leaked prose", err)
			}
		})
	}
	if len(q.s.Jobs) != 0 {
		t.Fatal("advice submitted work")
	}
}
