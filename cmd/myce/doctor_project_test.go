package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func doctorCPUProject(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	t.Chdir(root)
	t.Setenv("MYCE_NOTIFY", "")
	if err := os.MkdirAll("source", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("source/run.sh", []byte("echo ok > result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(".mycelium", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(".mycelium/adapter.yaml", []byte(cpuAdapterYAML("source")), 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestProjectDoctorReadyCPUReadOnly(t *testing.T) {
	root := doctorCPUProject(t)
	q, _, _, _ := waitQueue(t)
	_, err := q.Checkin(myce.Checkin{Machine: myce.Machine{ID: "cpu-worker", Session: "session", GPU: "cpu", CapabilityProfile: &myce.CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH, LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 16 << 30}, Capabilities: []string{"native_isolation_landlock", "native_isolation_seatbelt"}, HostMemoryMeasured: true, HostAvailableMemoryGB: 12}})
	if err != nil {
		t.Fatal(err)
	}
	handler := myce.API{Queue: q, Token: "fixture-secret"}.Handler()
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	before, err := os.ReadFile(".mycelium/adapter.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = projectDoctorWithAuthority(context.Background(), []string{"--project"}, &out, io.Discard, func(clientFlags) (myce.Client, error) {
		return myce.Client{URL: server.URL, Token: "fixture-secret"}, nil
	})
	if err != nil || !strings.HasPrefix(out.String(), "Project ready;") || !strings.Contains(out.String(), "notifier is not configured") {
		t.Fatalf("%v\n%s", err, &out)
	}
	if strings.Join(requests, ",") != "GET /api/ping,POST /api/resolve" {
		t.Fatal(requests)
	}
	if strings.Contains(out.String(), "fixture-secret") || strings.Contains(out.String(), server.URL) {
		t.Fatal(out.String())
	}
	after, _ := os.ReadFile(".mycelium/adapter.yaml")
	if !bytes.Equal(before, after) {
		t.Fatal("doctor changed adapter")
	}
	if _, err := os.Stat(filepath.Join(root, ".mycelium/receipts")); !os.IsNotExist(err) {
		t.Fatal("doctor created receipts", err)
	}
}

func TestProjectDoctorHoldsAndCompletion(t *testing.T) {
	for _, scenario := range []string{"missing authority", "queue lock", "missing notifier", "old server", "missing input", "dirty source"} {
		t.Run(scenario, func(t *testing.T) {
			doctorCPUProject(t)
			body, _ := os.ReadFile(".mycelium/adapter.yaml")
			args := []string{"--project", "--json"}
			want := "queue owner"
			if scenario == "missing notifier" {
				args = append(args, "--notify", "missing-hook")
				want = "consumer"
			}
			if scenario == "missing input" {
				body = []byte(strings.Replace(string(body), "    run:", "    inputs: {data: absent.txt}\n    run:", 1))
				want = "consumer"
			}
			if scenario == "dirty source" {
				body = []byte(strings.Replace(string(body), `source: {directory: "source"}`, `source: {git: HEAD, paths: [source]}`, 1))
				for _, argv := range [][]string{{"init", "--quiet"}, {"add", "source"}, {"-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture"}} {
					cmd := exec.Command("git", argv...)
					if data, err := cmd.CombinedOutput(); err != nil {
						t.Fatal(err, string(data))
					}
				}
				os.WriteFile("source/run.sh", []byte("dirty\n"), 0600)
				want = "consumer"
			}
			if err := os.WriteFile(".mycelium/adapter.yaml", body, 0600); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" && r.URL.Path == "/api/ping" {
					io.WriteString(w, `{"version":1}`)
					return
				}
				if r.Method != "POST" || r.URL.Path != "/api/resolve" {
					t.Errorf("unexpected write %s %s", r.Method, r.URL.Path)
					http.Error(w, "bad", 500)
					return
				}
				if scenario == "old server" {
					http.Error(w, "fixture-secret private authority", 404)
					return
				}
				report := myce.ResolveReport{Targets: []myce.ResolveTarget{{Eligible: true}}}
				if scenario == "queue lock" {
					report.Findings = []myce.ResolveFinding{{Field: "environment.lock_file", Side: "queue", Problem: "queue cannot read lock", Fix: "ask the queue owner to make the lock readable, or use a library recipe", PathDiagnostic: &myce.PathDiagnostic{Side: "queue"}}}
				}
				json.NewEncoder(w).Encode(report)
			}))
			defer server.Close()
			var out bytes.Buffer
			err := projectDoctorWithAuthority(context.Background(), args, &out, io.Discard, func(clientFlags) (myce.Client, error) {
				if scenario == "missing authority" {
					want = "installation owner"
					return myce.Client{}, errors.New("fixture-secret /private/config")
				}
				return myce.Client{URL: server.URL, Token: "fixture-secret"}, nil
			})
			if exitCode(err) != 1 || !strings.Contains(out.String(), `"state":"blocked"`) || !strings.Contains(out.String(), `"owner":"`+want+`"`) {
				t.Fatalf("%v want=%s output=%s", err, want, &out)
			}
			for _, secret := range []string{"fixture-secret", server.URL, "/private/config"} {
				if strings.Contains(out.String(), secret) {
					t.Fatal(out.String())
				}
			}
		})
	}
}
