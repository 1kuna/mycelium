package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtensionUsabilityTypedConflictsAndShortIDs(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{q, "fixture"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	ctx := context.Background()
	e := compositionExtension(t, false)
	if err := c.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
		t.Fatal(err)
	}
	old := e.Digest
	e.Files["new.txt"] = []byte("changed")
	e = freezeComposition(t, e)
	err := c.Call(ctx, "POST", "/api/extensions", e, nil)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "extension_version_conflict" || api.Status != 409 || !strings.Contains(api.Message, old) || !strings.Contains(api.Message, e.Digest) || !strings.Contains(api.Message, "bump the version") {
		t.Fatal("opaque version conflict", err)
	}
	if _, err := q.SetPriorityPolicy(0, defaultPriorityPolicy()); err != nil {
		t.Fatal(err)
	}
	err = c.Call(ctx, "PUT", "/api/policy", map[string]any{"expected_revision": 0, "policy": defaultPriorityPolicy()}, nil)
	if !errors.As(err, &api) || api.Code != "policy_revision_conflict" || api.CurrentRevision == nil || *api.CurrentRevision != 1 || !strings.Contains(api.Message, "current revision 1") {
		t.Fatal("opaque policy conflict", err)
	}
	j := submit(t, q, spec("experiment"))
	if got, err := c.GetJob(ctx, ShortID(j.ID)); err != nil || got.ID != j.ID {
		t.Fatal("short get", got, err)
	}
	if err := c.Call(ctx, "GET", "/api/jobs/"+ShortID(j.ID)+"/logs", nil, nil); err != nil {
		t.Fatal("short logs", err)
	}
	if _, err := c.Cancel(ctx, ShortID(j.ID)); err != nil {
		t.Fatal("short cancel", err)
	}
	if _, err := c.Wait(ctx, ShortID(j.ID)); err != nil {
		t.Fatal("short wait", err)
	}
	for _, route := range []struct {
		method, path, code string
		body               any
	}{
		{"GET", "/api/usage?since=bogus", "usage_invalid", nil},
		{"GET", "/api/jobs/" + ShortID(j.ID) + "/logs?after=-1", "job_logs_invalid_cursor", nil},
		{"GET", "/api/jobs/not-a-job/logs", "job_unknown", nil},
		{"PUT", "/api/policy", "policy_invalid", map[string]any{}},
		{"POST", "/api/extensions", "extension_invalid", Extension{}},
		{"GET", "/api/extensions/unknown", "extension_unknown", nil},
	} {
		err := c.Call(ctx, route.method, route.path, route.body, nil)
		if !errors.As(err, &api) || api.Code != route.code {
			t.Errorf("%s: %v", route.path, err)
		}
	}
	jobs := []*Job{{ID: "12345678-one"}, {ID: "12345678-two"}}
	if _, err := FindJob(jobs, "12345678"); !errors.As(err, &api) || api.Code != "job_id_ambiguous" {
		t.Fatal("ambiguous prefix", err)
	}
	if got, err := FindJob(jobs, "12345678-o"); err != nil || got.ID != jobs[0].ID {
		t.Fatal(got, err)
	}
}

func TestExtensionRunOutputContract(t *testing.T) {
	q, _ := fixture(t)
	e := compositionExtension(t, false)
	e.Outputs = []string{"result.json", "nested/other.json"}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, Spec{Name: "run", Project: "demo", Extension: extensionKey(e)})
	if len(j.Spec.Outputs) != 2 {
		t.Fatal("missing output defaults", j.Spec.Outputs)
	}
	if _, err := q.Submit(Spec{Name: "overlap", Project: "demo", Extension: extensionKey(e), Inputs: map[string]string{"result.json": "source"}}); err == nil {
		t.Fatal("overlapping input/output accepted")
	}
	for _, name := range []string{"../escape", "/absolute", "a/../b", "a\\b", "a:b"} {
		e.Outputs = []string{name}
		if err := e.validate(); err == nil {
			t.Fatal("unsafe output contract accepted", name)
		}
	}
	root := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "nested")); err != nil {
		t.Fatal(err)
	}
	if _, err := localResultPath(root, "nested/file"); err == nil {
		t.Fatal("result escaped through symlink")
	}
}

func TestExtensionNoEnvironmentWaitsForPreparation(t *testing.T) {
	e := compositionExtension(t, false)
	m := machine("local", "fake")
	m.CapabilityProfile = &CapabilityProfile{OS: e.Variants[0].Target.OS, Arch: e.Variants[0].Target.Arch}
	j := &Job{Spec: Spec{Extension: extensionKey(e), ExtensionCheck: &ExtensionCheckRequest{Work: "extension:" + e.Name}}, Extensions: []Extension{e}}
	if environmentReady(j, &m) {
		t.Fatal("check assigned before frozen files were installed")
	}
	report := newExtensionReport(e, &m, "extension:"+e.Name)
	report.Setup = true
	m.ExtensionReports = []ExtensionReport{report}
	if !environmentReady(j, &m) {
		t.Fatal("no-op preparation still requires a synthetic environment")
	}
}

func TestClientRunFailureProvidesTypedLogHint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/jobs":
			w.Write([]byte(`{"id":"12345678abcdef"}`))
		case "/api/jobs/12345678abcdef":
			w.Write([]byte(`{"id":"12345678abcdef","state":"failed","reason":"execution failed"}`))
		default:
			t.Error("failed run attempted result download", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	j, err := c.Run(context.Background(), "sample@1", nil, t.TempDir(), RunOptions{Project: "demo", Key: "stable"})
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "job_failed" || api.JobID != "12345678abcdef" || strings.Contains(err.Error(), "HTTP") || !strings.Contains(api.Message, "myce logs 12345678") || j == nil || j.State != "failed" {
		t.Fatal(j, err)
	}
}
