package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func statusViewResponse(t *testing.T, q *Queue, path string) string {
	t.Helper()
	r := httptest.NewRequest("GET", path, nil)
	r.SetBasicAuth("myce", "status-test")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "status-test"}.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("status HTTP %d", w.Code)
	}
	return w.Body.String()
}

func TestStatusViewReadOnlyAndTokenProtected(t *testing.T) {
	q, c := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("test-hypha", "unified", 16, 16, e)
	m.ExtensionReports = []ExtensionReport{{Name: e.Name, Version: e.Version, Digest: e.Digest, Work: "kind:sample", Platform: "fake", Reason: "synthetic-private-secret /private/example"}}
	m.LastSeen = c.Now()
	if err := setMaintenance(t, q, hostID(&m), "disable"); err != nil {
		t.Fatal(err)
	}
	m.CapabilityProfile.Availability = &Availability{Reason: "synthetic-private-secret", CheckedAt: c.Now()}
	if err := q.update(func() error { q.s.Machines[m.ID] = &m; return nil }); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Run.Command = []string{"echo", "synthetic-private-secret"}
	s.Run.Env = map[string]string{"PASSWORD": "synthetic-private-secret"}
	s.Run.Workdir = "/private/example"
	j := submit(t, q, s)
	if err := q.update(func() error {
		q.s.Jobs[j.ID].Spec.Environment = Environment{LockFile: "/private/example", Python: "3.11"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/api/status"} {
		body := statusViewResponse(t, q, path)
		for _, bad := range []string{"synthetic-private-secret", "/private/example", "extension.yaml", "baseline/input"} {
			if strings.Contains(body, bad) {
				t.Errorf("%s exposed %s", path, bad)
			}
		}
	}
	var snap Snapshot
	if err := json.Unmarshal([]byte(statusViewResponse(t, q, "/api/status")), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Machines[0].Disabled == "" || snap.Machines[0].ExtensionReports[0].Qualified {
		t.Fatal("redaction lost state")
	}
	// Stored commands and extension contents remain intact for execution.
	for _, j := range q.s.Jobs {
		if j.Spec.Kind == "" && j.Spec.Run.Env["PASSWORD"] != "synthetic-private-secret" {
			t.Fatal("status mutated job")
		}
	}

	h := API{Queue: q, Token: "status-test"}.Handler()
	for _, method := range []string{"GET", "POST", "PUT", "DELETE"} {
		for _, path := range []string{"/", "/api/status"} {
			for _, auth := range []bool{false, true} {
				r := httptest.NewRequest(method, path, nil)
				if auth {
					r.SetBasicAuth("myce", "status-test")
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				want := 401
				if auth && method == "GET" {
					want = 200
				}
				// Basic auth is intentionally accepted for status reads only.
				if w.Code != want {
					t.Fatalf("%s %s auth=%t: %d", method, path, auth, w.Code)
				}
			}
			if method != "GET" {
				r := httptest.NewRequest(method, path, nil)
				r.Header.Set("Authorization", "Bearer status-test")
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != 405 {
					t.Fatalf("%s %s: %d", method, path, w.Code)
				}
			}
		}
	}

	{
		m := machine("private-api-worker", "cpu")
		check(t, q, m, nil)
		server := httptest.NewServer(API{Queue: q, Token: "private-test-token"}.Handler())
		defer server.Close()
		resp, err := http.Get(server.URL + "/api/portability")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Fatal("unauthenticated advice")
		}
		client := Client{URL: server.URL, Token: "private-test-token"}
		request := PortabilityRequest{Spec: &Spec{Environment: Environment{LockFile: "/private/never-read-this"}, Needs: ResourceNeeds{MemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}}}
		before := identityHash(q.s)
		var r PortabilityReport
		if err := client.Call(context.Background(), "POST", "/api/portability", request, &r); err != nil {
			t.Fatal(err)
		}
		if identityHash(q.s) != before {
			t.Fatal("API advice mutated state")
		}
		var out bytes.Buffer
		if err := WritePortability(&out, r, false); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "Inventory: fresh") {
			t.Fatal(out.String())
		}

	}
}
