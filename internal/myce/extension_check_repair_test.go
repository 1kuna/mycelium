package myce

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestExtensionCheckRetainsRepairedSetupAfterPreparationFailure(t *testing.T) {
	e := compositionExtension(t, false)
	e.Kinds = nil
	e.Variants[0].Baselines = nil // Smoke check; this test asserts preparation, not qualification.
	e = freezeComposition(t, e)
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20}
	// A local setup repaired the failed automatic preparation. The enrolled host
	// alias differs from os.Hostname, as it does on the affected worker.
	if _, err := PrepareExtension(context.Background(), root, e, "fake", "0", false, io.Discard, cache); err != nil {
		t.Fatal(err)
	}
	// The cancelled older check already erased the setup bit. No setup steps
	// or assets exist; the verified frozen files need no additional preparation.
	old := readExtensionReports(root, e, "fake")
	old[0].Setup = false
	if err := writeExtensionReports(root, e, "fake", old); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := ExtensionFingerprint(context.Background(), "fake", "0")
	if err != nil {
		t.Fatal(err)
	}
	m := machine("repair-worker", "fake")
	m.HostID = "enrolled-alias"
	m.Device = "0"
	m.ExtensionFingerprint = fingerprint
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
	key := extensionPreparationKey(e, &m)
	m.EnvironmentReports = []EnvironmentReport{{Kind: "extension-setup", Key: key, State: "failed", Error: "previous build failed"}}
	observed := false
	response := func(v any) (*http.Response, error) {
		b, _ := json.Marshal(v)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(b)))}, nil
	}
	client := Client{URL: "http://fixture", HTTP: &http.Client{Transport: clientTestTransport(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/api/status":
			return response(Snapshot{Machines: []*Machine{&m}, Jobs: []*Job{{ID: "owned-check", State: "succeeded"}}})
		case "/api/extensions":
			return response(nil)
		case "/api/jobs":
			observed = true
			saved := readExtensionReports(root, e, "fake")
			if len(saved) != 1 || !saved[0].Setup || saved[0].Qualified || saved[0].Host != m.HostID {
				t.Fatalf("check discarded repaired setup: %+v", saved)
			}
			m.ExtensionReports = saved
			j := &Job{Spec: Spec{ExtensionCheck: &ExtensionCheckRequest{Work: "extension:" + e.Name}}, Extensions: []Extension{e}}
			if !environmentReady(j, &m) {
				t.Fatal("repaired check still waits behind the failed preparation")
			}
			return response(Job{ID: "owned-check", State: "succeeded"})
		case "/api/jobs/cancel":
			return response(map[string]any{"release": "released"})
		case "/api/jobs/owned-check/logs":
			return response([]LogChunk{})
		}
		t.Fatalf("unexpected request: %s", req.URL.Path)
		return nil, os.ErrInvalid
	})}}
	if _, err := CheckExtension(context.Background(), root, e, "fake", "0", m.HostID, io.Discard, cache, client); err != nil {
		t.Fatal(err)
	}
	if !observed {
		t.Fatal("check never submitted")
	}
}
