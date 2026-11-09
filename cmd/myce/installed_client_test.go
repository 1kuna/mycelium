package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestOwnerWritesInstalledPinnedAuthority(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	const token = "owner-fixture"
	q, _, _, _ := waitQueue(t)
	live := myce.API{Queue: q, Token: token}.Handler()
	writes := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("owner authority missing")
		}
		if strings.HasPrefix(r.URL.Path, "/api/hosts/") || strings.HasPrefix(r.URL.Path, "/api/jobs/") {
			live.ServeHTTP(w, r)
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
	invitation := myce.Invitation{Version: 1, Token: token, Address: server.URL, Fingerprint: hex.EncodeToString(pin[:])}
	if err := myce.WriteInvitation(joining, invitation); err != nil {
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
	for _, flags := range [][]string{{"--config", config}, nil} {
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

	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	{
		var err error
		m := maintenanceCLIWorker("worker")
		m.Devices = []myce.MachineDevice{{ID: "0", Platform: "fake", Identity: "fake-stable", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
		if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			args []string
			ok   func(myce.HostMaintenanceStatus) bool
		}{

			{[]string{"drain", "host"}, func(s myce.HostMaintenanceStatus) bool { return s.Policy.Mode == "drained" }},

			{[]string{"resume", "host"}, func(s myce.HostMaintenanceStatus) bool { return s.Policy.Mode == "enabled" }},
		} {
			var out bytes.Buffer
			var status myce.HostMaintenanceStatus
			if err = hostMaintenanceCommand(context.Background(), tc.args, &out); err != nil || json.Unmarshal(out.Bytes(), &status) != nil || !tc.ok(status) {
				t.Fatalf("host %v: %s %v", tc.args, out.String(), err)
			}
			if strings.Contains(out.String(), token) {
				t.Fatal("host maintenance exposed authority")
			}
		}
		// A wrong installed pin must refuse before the policy changes.
		invitation.Fingerprint = strings.Repeat("0", 64)
		if err = myce.WriteInvitation(joining, invitation); err != nil {
			t.Fatal(err)
		}
		if err = hostMaintenanceCommand(context.Background(), []string{"device", "disable", "host", "0", "--config", config}, &bytes.Buffer{}); err == nil {
			t.Fatal("host maintenance accepted a mismatched certificate pin")
		}
		if current, err := q.HostMaintenanceStatus("host"); err != nil || current.Policy.Mode != "enabled" {
			t.Fatalf("pin refusal changed policy: %+v %v", current.Policy, err)
		}
	}
	invitation.Fingerprint = hex.EncodeToString(pin[:])
	if err := myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
	}
	{
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
	invitation.Fingerprint = hex.EncodeToString(pin[:])
	if err := myce.WriteInvitation(joining, invitation); err != nil {
		t.Fatal(err)
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
}

func TestOwnerWritesInstalledQueueEnvironment(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	var readOnly atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer queue-owner-fixture" {
			t.Error("missing installed owner")
		}
		if readOnly.Load() {
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

	{
		const token = "queue-owner-fixture"
		readOnly.Store(true)
		config := filepath.Join(os.Getenv("HOME"), ".local", "share", "mycelium", "worker", "config.json")
		if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(myce.ServiceConfig{Role: "worker", EnvironmentFile: credentials})
		if err := os.WriteFile(config, data, 0600); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := policyCommand([]string{"get"}, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), `"revision":23`) || strings.Contains(out.String(), token) {
			t.Fatal("incorrect policy or credential in output")
		}
	}

}
