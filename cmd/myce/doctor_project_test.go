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
	q, clock, _, _ := waitQueue(t)
	_, err := currentCLICheckin(q, clock, myce.Checkin{Machine: myce.Machine{ID: "cpu-worker", Session: "session", GPU: "cpu", CPUIsolation: "select-none", CapabilityProfile: &myce.CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH, LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 16 << 30}, Capabilities: []string{"native_isolation_landlock", "native_isolation_seatbelt"}, HostMemoryMeasured: true, HostAvailableMemoryGB: 12}})
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
	for _, scenario := range []string{"missing authority", "queue lock", "missing notifier", "old server", "missing input", "dirty source", "failed preparation", "resource pressure", "cold environment", "unknown observation", "NAS unverified", "target constraint"} {
		t.Run(scenario, func(t *testing.T) {
			doctorCPUProject(t)
			body, _ := os.ReadFile(".mycelium/adapter.yaml")
			args := []string{"--project", "--json"}
			want := "queue owner"
			if scenario == "missing notifier" {
				args = append(args, "--notify", "missing-hook")
				want = "consumer"
			}
			if scenario == "NAS unverified" {
				body = []byte(strings.Replace(string(body), "    run:", "    shared_inputs: {data: {nas: data.bin, sha256: '"+strings.Repeat("a", 64)+"', size_bytes: 3}}\n    run:", 1))
				want = "storage owner"
			}
			if scenario == "failed preparation" {
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
				switch scenario {
				case "failed preparation":
					report.Hold = &myce.Hold{Kind: "deterministically_failed", Reason: "environment", Owner: "consumer", Next: "fix immutable recipe", Detail: "package version is unavailable"}
				case "resource pressure":
					report.Hold = &myce.Hold{Kind: "resource_pressure", Reason: "host memory", Owner: "queue owner", Next: "wait for memory", Detail: "requires 4 GiB; available 0.5 GiB"}
				case "cold environment":
					report.Hold = &myce.Hold{Kind: "unprepared", Reason: "environment", Owner: "queue", Next: "submit normally"}
				case "unknown observation":
					report.Hold = &myce.Hold{Kind: "unknown_observation", Reason: "inventory", Owner: "queue owner", Next: "restore observations"}
				case "target constraint":
					report.Targets = []myce.ResolveTarget{{Reason: "requires linux/arm64; no matching target"}}
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
			if scenario == "cold environment" {
				if err != nil || !strings.Contains(out.String(), `"state":"ready"`) {
					t.Fatalf("%v %s", err, &out)
				}
				return
			}
			if scenario == "failed preparation" && !strings.Contains(out.String(), "package version is unavailable") {
				t.Fatal(out.String())
			}
			if scenario == "target constraint" && !strings.Contains(out.String(), "requires linux/arm64") {
				t.Fatal(out.String())
			}
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

func TestProjectDoctorHelpDoesNotProbe(t *testing.T) {
	var out bytes.Buffer
	err := projectDoctorWithAuthority(context.Background(), []string{"--project", "--help"}, io.Discard, &out, func(clientFlags) (myce.Client, error) { t.Fatal("help loaded authority"); return myce.Client{}, nil })
	if err != nil || !strings.Contains(out.String(), "-adapter") {
		t.Fatalf("%v %s", err, &out)
	}
}

func doctorReviewFixture(t *testing.T, args []string, resolve func(http.ResponseWriter, *http.Request)) (string, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/api/ping" {
			io.WriteString(w, `{"version":1}`)
			return
		}
		if r.Method != "POST" || r.URL.Path != "/api/resolve" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			http.Error(w, "bad", 500)
			return
		}
		resolve(w, r)
	}))
	defer server.Close()
	var out bytes.Buffer
	err := projectDoctorWithAuthority(context.Background(), append([]string{"--project"}, args...), &out, io.Discard, func(clientFlags) (myce.Client, error) { return myce.Client{URL: server.URL, Token: "CANARY"}, nil })
	return out.String(), err
}
func doctorReadyReport(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(myce.ResolveReport{Targets: []myce.ResolveTarget{{Eligible: true}}})
}
func TestProjectDoctorReviewPathNotifier(t *testing.T) {
	doctorCPUProject(t)
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "review-hook"), []byte("#!/bin/sh\necho invoked > unexpected-notifier-execution\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := doctorReviewFixture(t, []string{"--notify", "review-hook"}, doctorReadyReport)
	if err != nil {
		t.Fatalf("PATH executable accepted by run is rejected: %v\n%s", err, out)
	}
	if _, err := os.Stat("unexpected-notifier-execution"); !os.IsNotExist(err) {
		t.Fatal("doctor executed notifier")
	}
}
func TestProjectDoctorReviewCapacityRemedy(t *testing.T) {
	doctorCPUProject(t)
	out, err := doctorReviewFixture(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(409)
		io.WriteString(w, `{"code":"shared_input_cache_capacity","error":"shared inputs require 200 bytes; largest enabled matching worker cache capacity=100 bytes"}`)
	})
	if exitCode(err) != 1 {
		t.Fatal(err, out)
	}
	if strings.Contains(out, "restore queue resolve service") || !strings.Contains(out, "200") {
		t.Fatalf("healthy queue capacity rejection lost its cause and remedy:\n%s", out)
	}
}
func TestProjectDoctorReviewDiagnosticSecret(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			doctorCPUProject(t)
			var args []string
			if format == "json" {
				args = []string{"--json"}
			}
			out, _ := doctorReviewFixture(t, args, func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(myce.ResolveReport{Targets: []myce.ResolveTarget{{Eligible: true}}, Findings: []myce.ResolveFinding{{Field: "environment", Side: "queue", Problem: "environment build failed: Authorization: Bearer CANARY", Fix: "correct immutable inputs"}}})
			})
			if strings.Contains(out, "CANARY") || !strings.Contains(out, "[redacted]") {
				t.Fatal("authority was not safely redacted")
			}
		})
	}
}
func TestProjectDoctorReviewInvalidAdapterSecret(t *testing.T) {
	doctorCPUProject(t)
	b, err := os.ReadFile(".mycelium/adapter.yaml")
	if err != nil {
		t.Fatal(err)
	}
	b = []byte(strings.Replace(string(b), "command: [sh, run.sh]", "command: CANARY", 1))
	if err = os.WriteFile(".mycelium/adapter.yaml", b, 0600); err != nil {
		t.Fatal(err)
	}
	out, err := doctorReviewFixture(t, nil, doctorReadyReport)
	if strings.Contains(out, "CANARY") || exitCode(err) != 1 {
		t.Fatal("malformed adapter disclosed its scalar or did not block")
	}
}
func TestProjectDoctorReviewBadRequestIsConsumerFix(t *testing.T) {
	doctorCPUProject(t)
	out, err := doctorReviewFixture(t, nil, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unrecognized synthetic private error", 400)
	})
	if exitCode(err) != 1 || strings.Contains(out, "upgrade the queue") || strings.Contains(out, "synthetic private error") || !strings.Contains(out, "correct the adapter") {
		t.Fatalf("%v %s", err, out)
	}
}
