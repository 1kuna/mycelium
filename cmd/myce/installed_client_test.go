package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestOwnerWritesInstalledPinnedAuthority(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	q, _, _, _ := waitQueue(t)
	const token = "owner-fixture"
	writes := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("owner authority missing")
		}
		if strings.HasPrefix(r.URL.Path, "/api/jobs/") || r.URL.Path == "/api/jobs/cancel" {
			myce.API{Queue: q, Token: token}.Handler().ServeHTTP(w, r)
			return
		}
		if r.Method != "GET" {
			writes++
		}
		fmt.Fprint(w, `{"revision":1,"policy":{}}`)
	}))
	defer server.Close()
	dir := t.TempDir()
	joining := filepath.Join(dir, "joining.json")
	pin := sha256.Sum256(server.Certificate().Raw)
	if err := myce.WriteInvitation(joining, myce.Invitation{Version: 1, Token: token, Address: server.URL, Fingerprint: hex.EncodeToString(pin[:])}); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(os.Getenv("HOME"), ".local", "share", "mycelium", "queue", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(myce.ServiceConfig{Role: "queue", JoiningFile: joining})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	policy := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(policy, []byte("version: 1\ntiers:\n  - name: shared\n    selectors: ['*/*']\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, flags := range [][]string{{"--config", config}, {"--joining-file", joining}, nil} {
		// Legacy environment credentials must not discard the installed pin.
		if flags == nil {
			t.Setenv("MYCE_TOKEN", token)
			t.Setenv("MYCE_QUEUE", server.URL)
		}
		var out bytes.Buffer
		if err := policyCommand(append([]string{"set", policy, "--if-revision", "0"}, flags...), &out); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), token) {
			t.Fatal("credential in output")
		}
		if err := modelCommand(append([]string{"sync", "example", "--to", "all"}, flags...)); err != nil {
			t.Fatal(err)
		}
	}
	if writes != 6 {
		t.Fatalf("writes = %d", writes)
	}
	// A real server with a different certificate receives no owner request.
	wrongRequests := 0
	wrong := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { wrongRequests++ }))
	identity, err := myce.LoadJoiningIdentity(filepath.Join(dir, "other-identity"))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := identity.TLS()
	if err != nil {
		t.Fatal(err)
	}
	wrong.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	wrong.StartTLS()
	defer wrong.Close()
	t.Setenv("MYCE_QUEUE", wrong.URL)
	if err := policyCommand([]string{"set", policy, "--if-revision", "0"}, &bytes.Buffer{}); err == nil || wrongRequests != 0 {
		t.Fatal("owner write bypassed installed pin")
	}
	t.Setenv("MYCE_QUEUE", strings.Replace(server.URL, "https:", "http:", 1))
	if err := policyCommand([]string{"set", policy, "--if-revision", "0"}, &bytes.Buffer{}); err == nil {
		t.Fatal("owner write downgraded installed trust")
	}

	// Queue owner discovery above is complete; cancellation must discover worker authority.
	for _, name := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"} {
		t.Setenv(name, "")
	}
	if err := os.Remove(config); err != nil {
		t.Fatal(err)
	}
	config = filepath.Join(os.Getenv("HOME"), ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ = json.Marshal(myce.ServiceConfig{Role: "worker", JoiningFile: joining})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	invitation := myce.Invitation{Version: 1, Token: token, Address: server.URL, Fingerprint: hex.EncodeToString(pin[:])}

	byID := waitSubmit(t, q)
	byKey, err := q.Submit(myce.Spec{Name: "cancel-key", Project: "test", SubmissionKey: "installed-cancel", Priority: "experiment", Run: myce.RunSpec{Command: []string{"echo", "done"}}, Needs: myce.ResourceNeeds{MemoryGB: 1, Platform: myce.Platform{Any: true}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		id   string
	}{
		{[]string{"cancel", byID.ID}, byID.ID},
		{[]string{"cancel", "--project", "test", "--key", "installed-cancel"}, byKey.ID},
	} {
		out, diagnostics, err := captureUsageOutput(t, tc.args)
		var ack myce.Cancellation
		if err != nil || json.Unmarshal([]byte(out), &ack) != nil || ack.JobID == nil || *ack.JobID != tc.id || ack.State != "cancelled" || !ack.WillNotRun {
			t.Fatalf("cancel %v: ack=%+v err=%v stderr=%s", tc.args, ack, err, diagnostics)
		}
		if strings.Contains(out+diagnostics, token) {
			t.Fatal("cancel exposed authority")
		}
	}
	// A wrong installed pin must reject cancellation before it changes the job.
	pending := waitSubmit(t, q)
	invitation.Fingerprint = strings.Repeat("0", 64)
	if err := myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
	}
	if _, _, err := captureUsageOutput(t, []string{"cancel", pending.ID, "--config", config}); err == nil {
		t.Fatal("cancel accepted a mismatched certificate pin")
	}
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if job, err := myce.FindJob(snapshot.Jobs, pending.ID); err != nil || job.State == "cancelled" {
		t.Fatalf("pin refusal changed job: %+v %v", job, err)
	}
}

func TestOwnerWritesInstalledQueueEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer queue-owner-fixture" {
			t.Error("missing installed owner")
		}
		if r.Method == "GET" {
			if r.URL.Path != "/api/policy" {
				t.Error("installed authority not used")
			}
			fmt.Fprint(w, `{"revision":23,"policy":{}}`)
			return
		}
		fmt.Fprint(w, `{"revision":2,"policy":{}}`)
	}))
	defer server.Close()
	dir := t.TempDir()
	credentials := filepath.Join(dir, "fleet.env")
	if err := os.WriteFile(credentials, []byte("MYCE_TOKEN=queue-owner-fixture\nMYCE_QUEUE="+server.URL+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "config.json")
	data, _ := json.Marshal(myce.ServiceConfig{Role: "queue", EnvironmentFile: credentials})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	policy := filepath.Join(dir, "policy.yaml")
	if err := os.WriteFile(policy, []byte("version: 1\ntiers:\n  - name: shared\n    selectors: ['*/*']\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := policyCommand([]string{"set", policy, "--if-revision", "1", "--config", config}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, flags := range [][]string{{"--config", filepath.Join(dir, "missing")}, {"--queue", "https://example.invalid", "--token", "fixture"}} {
		err := policyCommand(append([]string{"set", policy, "--if-revision", "1"}, flags...), &bytes.Buffer{})
		var refusal *myce.HTTPError
		if !errors.As(err, &refusal) || !strings.HasPrefix(refusal.Code, "owner_authority_") {
			t.Fatalf("missing typed owner refusal: %v", err)
		}
	}

	for _, role := range []string{"worker", "queue"} {
		config := filepath.Join(home, ".local", "share", "mycelium", role, "config.json")
		if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(myce.ServiceConfig{Role: role, EnvironmentFile: credentials})
		if err := os.WriteFile(config, data, 0600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := policyCommand([]string{"get"}, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), `"revision":23`) || strings.Contains(out.String(), "queue-owner-fixture") {
			t.Fatal("incorrect policy or credential in output")
		}
		if err := os.Remove(config); err != nil {
			t.Fatal(err)
		}
	}
}
