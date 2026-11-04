package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestScarceHostYieldRequirementPrivacy(t *testing.T) {
	q, _, _, _, _ := yieldFixture(t, false)
	s := spec("experiment")
	s.Needs.Platform = Platform{Required: []string{"fake"}}
	s.RequirementReasons = map[string]string{
		"host":                       "private host explanation",
		"device":                     "private device explanation",
		"backend:fake":               "private backend explanation",
		"backend:private-project-id": "private backend metadata",
		"os:linux":                   "private OS explanation",
		"os:private-project-id":      "private OS metadata",
		"arch:amd64":                 "private architecture explanation",
		"arch:private-project-id":    "private architecture metadata",
	}
	demand := submit(t, q, s)
	server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	for _, endpoint := range []string{"/api/jobs/" + demand.ID, "/api/status"} {
		var raw json.RawMessage
		if err := c.Call(context.Background(), "GET", endpoint, nil, &raw); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "private") {
			t.Fatal("yield observation leaked arbitrary requirement keys or prose", endpoint)
		}
		var got *Job
		if endpoint == "/api/status" {
			var snapshot Snapshot
			if err := json.Unmarshal(raw, &snapshot); err != nil {
				t.Fatal(err)
			}
			for _, j := range snapshot.Jobs {
				if j.ID == demand.ID {
					got = j
				}
			}
		} else if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got == nil || got.YieldPlan == nil || !slices.Equal(got.YieldPlan.Requirements, []string{"backend:fake", "device", "host", "other"}) {
			t.Fatal("yield requirements lost finite names or failed to coalesce private keys", endpoint, got)
		}
	}
}
