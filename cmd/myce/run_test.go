package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestRunQueueOutcomesAndCancellation(t *testing.T) {
	for _, outcome := range []string{"succeeded", "failed", "cancelled", "interrupt", "detach"} {
		t.Run(outcome, func(t *testing.T) {
			q, clock, _, _ := waitQueue(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			api := myce.API{Queue: q, Token: "test-private-token"}.Handler()
			var submitted *myce.Job
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" && r.URL.Path == "/api/jobs" {
					var s myce.Spec
					if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					var err error
					submitted, err = q.Submit(s)
					if err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					clock.advance(time.Second)
					if outcome == "cancelled" {
						_ = q.Cancel(submitted.ID)
					}
					if outcome == "succeeded" || outcome == "failed" {
						m := maintenanceCLIWorker("cpu-worker")
						m.CPUIsolation = "select-none"
						reply, err := q.Checkin(myce.Checkin{Machine: m})
						if err != nil || reply.Job == nil {
							t.Errorf("assignment: %+v %v %s", reply, err, waitAssignmentDiagnostics(q))
							w.WriteHeader(500)
							return
						}
						j := reply.Job
						code := 7
						result := myce.Result{Machine: j.Machine, Session: j.Session, Execution: myce.Execution{ID: j.ID, Attempt: j.Attempt}, Success: outcome == "succeeded"}
						if outcome == "failed" {
							result.ExecutionCause = &myce.RuntimeCause{Operation: "command", Class: "command_exit", ExitCode: &code}
						}
						if err = q.Complete(result); err != nil {
							t.Error(err)
						}
					}
					_ = json.NewEncoder(w).Encode(submitted)
					return
				}
				if outcome == "interrupt" && strings.HasPrefix(r.URL.Path, "/api/jobs/") {
					cancel()
				}
				api.ServeHTTP(w, r)
			}))
			defer server.Close()
			t.Setenv("MYCE_QUEUE", server.URL)
			t.Setenv("MYCE_TOKEN", "test-private-token")
			t.Setenv("MYCE_JOINING_FILE", "")
			var out, diagnostics bytes.Buffer
			args := []string{"--cpu", "2", "--mem", "512M", "--project", "demo", "--class", "experiment"}
			if outcome == "detach" {
				args = append(args, "--detach")
			}
			args = append(args, "--", "printf", "%s", "two words", "--flag")
			err := runCommand(ctx, args, &out, &diagnostics)
			want := map[string]int{"succeeded": 0, "failed": 7, "cancelled": 1, "interrupt": 130, "detach": 0}[outcome]
			if exitCode(err) != want {
				t.Fatalf("code=%d want=%d err=%v", exitCode(err), want, err)
			}
			snap, err := q.Snapshot()
			if err != nil || len(snap.Jobs) != 1 {
				t.Fatalf("snapshot %v %v", snap, err)
			}
			j := snap.Jobs[0]
			if j.Spec.Needs.CPUCores != 2 || j.Spec.Needs.HostMemoryGB != .5 || !reflect.DeepEqual(j.Spec.Run.Command, []string{"printf", "%s", "two words", "--flag"}) {
				t.Fatalf("spec: %+v", j.Spec)
			}
			if outcome == "interrupt" && (j.State != "cancelled" || j.CancelledAt == nil) {
				t.Fatalf("not durably cancelled: %+v", j)
			}
			if outcome == "detach" && out.String() != j.ID+"\n" {
				t.Fatal("detach did not print exact ID")
			}
			if strings.Contains(out.String()+diagnostics.String(), "test-private-token") {
				t.Fatal("credential exposed")
			}
		})
	}
}

func TestRunInstalledPinnedAuthority(t *testing.T) {
	// An explicit plain command retains installed-command behavior even in a
	// project with an adapter; --entry/--adapter select appended entry arguments.
	t.Chdir(t.TempDir())
	if err := os.Mkdir(".mycelium", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(".mycelium/adapter.yaml", []byte("not a selected adapter"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer installed-private-token" {
			t.Error("missing installed authority")
		}
		switch r.URL.Path {
		case "/api/jobs":
			io.WriteString(w, `{"id":"pinned-job"}`)
		case "/api/jobs/pinned-job":
			io.WriteString(w, `{"id":"pinned-job","state":"succeeded"}`)
		default:
			io.WriteString(w, `[{"seq":1,"text":"hi\n"}]`)
		}
	}))
	defer server.Close()
	hash := sha256.Sum256(server.Certificate().Raw)
	authority := filepath.Join(home, "joining.json")
	if err := myce.WriteInvitation(authority, myce.Invitation{Version: 1, Fingerprint: hex.EncodeToString(hash[:]), Token: "installed-private-token", Address: server.URL}); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(myce.ServiceConfig{Role: "worker", JoiningFile: authority})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	var out, diagnostics bytes.Buffer
	if err := runCommand(context.Background(), []string{"--", "echo", "hi"}, &out, &diagnostics); err != nil || out.String() != "hi\n" {
		t.Fatalf("pinned run: %v output=%q", err, out.String())
	}
	// An explicit endpoint keeps the installed pin, and sends no credential to a
	// different certificate. This is the real TLS client, not a mocked transport.
	requests := 0
	wrong := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
	identity, err := myce.LoadJoiningIdentity(filepath.Join(t.TempDir(), "identity"))
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
	client, err := runClient(config, "", wrong.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.HTTP.CloseIdleConnections()
	if err = client.Call(context.Background(), "GET", "/api/status", nil, nil); err == nil || requests != 0 {
		t.Fatal("installed pin allowed a different certificate or sent credentials")
	}
	if _, err = runClient(config, "", strings.Replace(wrong.URL, "https:", "http:", 1), ""); err == nil {
		t.Fatal("installed trust allowed HTTP downgrade")
	}
}

func TestRunRequirementsAndUsage(t *testing.T) {
	for _, args := range [][]string{{"--gpu", "bogus", "--", "echo"}, {"--cpu", "0", "--", "echo"}, {"--mem", "NaN", "--", "echo"}, {"--mem", "-2G", "--", "echo"}, {"--priority", "deadline", "--", "echo"}, {"--"}} {
		if err := runCommand(context.Background(), args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	t.Setenv("MYCE_TOKEN", "private-help-token")
	var help bytes.Buffer
	_ = runCommand(context.Background(), []string{"--help"}, io.Discard, &help)
	if strings.Contains(help.String(), "private-help-token") {
		t.Fatal("help leaked token")
	}
}

func TestRunGPUAndInstalledQueueConfig(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	server := httptest.NewServer(myce.API{Queue: q, Token: "queue-config-fixture"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	config := filepath.Join(home, ".local", "share", "mycelium", "queue", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	credentials := filepath.Join(home, "fleet.env")
	if err := os.WriteFile(credentials, []byte("MYCE_TOKEN=queue-config-fixture\nMYCE_QUEUE="+server.URL+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(myce.ServiceConfig{Role: "queue", EnvironmentFile: credentials})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runCommand(context.Background(), []string{"--gpu", "cuda", "--mem", "8G", "--host-mem", "4G", "--detach", "--", "nvidia-smi"}, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	snapshot, err := q.Snapshot()
	if err != nil || len(snapshot.Jobs) != 1 {
		t.Fatalf("jobs: %+v %v", snapshot.Jobs, err)
	}
	j := snapshot.Jobs[0]
	if j.Spec.Needs.MemoryGB != 8 || j.Spec.Needs.HostMemoryGB != 4 || j.Spec.Needs.CPUCores != 1 || !reflect.DeepEqual(j.Spec.Needs.Platform.Required, []string{"cuda"}) || out.String() != j.ID+"\n" {
		t.Fatalf("GPU declaration: %+v", j.Spec.Needs)
	}
}
