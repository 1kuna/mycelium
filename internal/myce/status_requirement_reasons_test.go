package myce

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// #748: the per-job read showed a pinned adapter job's host reason as
// authored while fleet status showed missing, because the compact job
// whitelist dropped the frozen map before the markers were derived.
func TestStatusRequirementReasonsMatchJobRead(t *testing.T) {
	q, _ := fixture(t)
	m := machine("pinned", "fake")
	check(t, q, m, nil)
	s := spec("experiment")
	s.AllowedMachines = []string{m.ID}
	s.RequirementReasons = map[string]string{"host": "private dataset path on this host"}
	job := submit(t, q, s)
	handler := (API{Queue: q, Token: "test"}).Handler()
	get := func(path string) []byte {
		t.Helper()
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "private dataset") {
			t.Fatalf("%s published raw requirement text", path)
		}
		return w.Body.Bytes()
	}
	var receipt Job
	if err := json.Unmarshal(get("/api/jobs/"+job.ID), &receipt); err != nil {
		t.Fatal(err)
	}
	want := []RequirementReasonStatus{{Constraint: "host", ReasonSource: "authored", Source: "spec.requirement_reasons[host]"}}
	if !reflect.DeepEqual(receipt.RequirementReasons, want) {
		t.Fatalf("job read: %+v", receipt.RequirementReasons)
	}
	for _, path := range []string{"/api/status?view=snapshot", "/api/status?jobs=recent", "/api/status?sections=jobs", "/api/status?detail=full"} {
		var snapshot Snapshot
		if err := json.Unmarshal(get(path), &snapshot); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, j := range snapshot.Jobs {
			if j.ID == job.ID {
				found = true
				if !reflect.DeepEqual(j.RequirementReasons, want) {
					t.Errorf("%s: status %+v, job read %+v", path, j.RequirementReasons, receipt.RequirementReasons)
				}
			}
		}
		if !found {
			t.Errorf("%s: job missing", path)
		}
	}
}
