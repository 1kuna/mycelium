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
	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("VariantStatusRegistrationWarnings", func(t *testing.T) {
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

	})

	t.Run("RequirementReasonsNativeAndMissingPin", func(t *testing.T) {
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

	})

	t.Run("RequirementReasonsResolveAndPortability", func(t *testing.T) {
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

	})

	t.Run("ResolveFlagsDataLocalityPin", func(t *testing.T) {
		q, c := fixture(t)
		for _, id := range []string{"data-host", "other-host"} {
			m := machine(id, "cpu")
			m.LastSeen = c.Now()
			check(t, q, m, nil)
		}
		s := cpuResolveSpec()
		s.AllowedMachines = []string{"data-host"}
		s.Run.Command = []string{"sh", "-c", "wc -l /srv/local-data/clips.csv"}
		r, err := q.Resolve(ResolveRequest{Spec: s})
		if err != nil {
			t.Fatal(err)
		}
		pin := resolveFinding(r, "allowed_machines")
		if pin == nil || pin.Side != "submitter" || !strings.Contains(pin.Problem, "other-host") || !strings.Contains(pin.Fix, "--files-from") || !strings.Contains(pin.Fix, "shared_inputs") {
			t.Fatalf("data-locality pin not flagged with staging: %+v", r.Findings)
		}
		path := resolveFinding(r, "run.command[2]")
		if path == nil || path.Side != "worker" || !strings.Contains(path.Problem, "/srv/local-data/clips.csv") {
			t.Fatalf("local data path not named: %+v", r.Findings)
		}
		eligible := slices.IndexFunc(r.Targets, func(t ResolveTarget) bool { return t.Eligible })
		if eligible < 0 || r.Targets[eligible].Machine != "data-host" {
			t.Fatalf("pinned eligibility: %+v", r.Targets)
		}
		r, err = q.Resolve(ResolveRequest{Spec: s, RequirementReasons: map[string]string{"host": "approved parity"}})
		if err != nil || resolveFinding(r, "allowed_machines") != nil {
			t.Fatalf("authored host requirement flagged: %+v %v", r.Findings, err)
		}

	})

	t.Run("ScarceHostYieldRequirementPrivacy", func(t *testing.T) {
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

	})
}
