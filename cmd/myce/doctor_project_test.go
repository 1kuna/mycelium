package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
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
	_, err := legacyCLICheckin(q, clock, myce.Checkin{Machine: myce.Machine{ID: "cpu-worker", Session: "session", GPU: "cpu", CPUIsolation: "select-none", CapabilityProfile: &myce.CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH, LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 16 << 30}, Capabilities: []string{"native_isolation_landlock", "native_isolation_seatbelt"}, HostMemoryMeasured: true, HostAvailableMemoryGB: 12}})
	if err != nil {
		t.Fatal(err)
	}
	handler := myce.API{Queue: q, Token: "fixture-secret"}.Handler()
	var forced atomic.Pointer[myce.ResolveReport]
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		if r.Method == "POST" && r.URL.Path == "/api/resolve" {
			if report := forced.Load(); report != nil {
				json.NewEncoder(w).Encode(report)
				return
			}
		}
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

	for _, scenario := range []string{"resource pressure", "cold environment"} {
		report := myce.ResolveReport{Targets: []myce.ResolveTarget{{Eligible: true}}}
		if scenario == "resource pressure" {
			report.Blocker = &myce.Blocker{Code: "resource_pressure", Dependency: "host memory", Owner: "queue owner", Action: "wait for memory", Evidence: "requires 4 GiB; available 0.5 GiB"}
		} else {
			report.Blocker = &myce.Blocker{Code: "unprepared", Dependency: "environment", Owner: "queue", Action: "submit normally"}
		}
		forced.Store(&report)
		var out bytes.Buffer
		err := projectDoctorWithAuthority(context.Background(), []string{"--project", "--json"}, &out, io.Discard, func(clientFlags) (myce.Client, error) {
			return myce.Client{URL: server.URL, Token: "fixture-secret"}, nil
		})
		if scenario == "cold environment" {
			if err != nil || !strings.Contains(out.String(), `"state":"ready"`) {
				t.Fatalf("%v %s", err, &out)
			}
			continue
		}
		want := "queue owner"
		if exitCode(err) != 1 || !strings.Contains(out.String(), `"state":"blocked"`) || !strings.Contains(out.String(), `"owner":"`+want+`"`) {
			t.Fatalf("%v want=%s output=%s", err, want, &out)
		}
		for _, secret := range []string{"fixture-secret", server.URL, "/private/config"} {
			if strings.Contains(out.String(), secret) {
				t.Fatal(out.String())
			}
		}
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
