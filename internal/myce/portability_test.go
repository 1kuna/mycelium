package myce

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func portabilityRow(t *testing.T, r PortabilityReport, target ExecutionTarget) PortabilityTarget {
	t.Helper()
	for _, row := range r.Targets {
		if row.Target == target {
			return row
		}
	}
	t.Fatalf("missing target %+v in %+v", target, r)
	return PortabilityTarget{}
}

func TestPortabilityAPIReadOnlyAndPrivate(t *testing.T) {
	q, _ := fixture(t)
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

// The real dev API can observe mixed synthetic check-ins on a GPU-free laptop.
// Recovering prevents these metadata-only fixtures from accepting payloads.
