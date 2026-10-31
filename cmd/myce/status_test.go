package main

import (
	"context"
	"fmt"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestStatusFullIDsAndOneBasedAttempts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"machines":[{"id":"b70-gpu1","disabled":"planned host maintenance"}],"jobs":[{"id":"12345678abcdef","state":"running","attempt":1,"spec":{"name":"work","priority":"production"}},{"id":"abcdefgh123456","state":"queued","attempt":0,"spec":{"name":"waiting","priority":"experiment"}}]}`)
	}))
	defer server.Close()
	saved := os.Stdout
	defer func() { os.Stdout = saved }()
	for _, full := range []bool{false, true} {
		f, e := os.CreateTemp(t.TempDir(), "status")
		if e != nil {
			t.Fatal(e)
		}
		os.Stdout = f
		args := []string{"status", "--queue", server.URL, "--token", "test-only"}
		if full {
			args = append(args, "--full")
		}
		if e = run(args); e != nil {
			t.Fatal(e)
		}
		f.Close()
		data, e := os.ReadFile(f.Name())
		if e != nil {
			t.Fatal(e)
		}
		output := string(data)
		if strings.Contains(output, "12345678abcdef") != full {
			t.Fatal(output)
		}
		if !strings.Contains(output, "disabled by operator") {
			t.Fatal(output)
		}
		if !strings.Contains(output, "running · attempt 1") || strings.Contains(output, "attempt 0") {
			t.Fatal(output)
		}
	}
}

func TestStatusShowsProfileQualificationAndEveryWaitingReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"host_states":{"example":"degraded"},"machines":[{"id":"example","gpu":"cuda","capability_profile":{"benchmark":{"state":"complete","seconds":1,"measurements":[{"resource":"cpu","work":"sha256","rate":123,"unit":"bytes/s"}]},"os":"linux","arch":"arm64","logical_cpus":8,"memory_layout":"unified","system_memory_bytes":17179869184,"accelerators":[{"type":"cuda","memory_bytes":17179869184}],"availability":{"available":false,"reason":"on battery"}},"extension_reports":[{"name":"sample","version":"1","work":"kind:sample","qualified":true}]}],"jobs":[{"id":"running-job","machine":"example","state":"running","spec":{"name":"running sample","priority":"experiment","kind":"sample"}},{"id":"waiting-job","state":"queued","reason":"example: hypha on battery; waiting for external power","spec":{"name":"waiting sample","priority":"experiment"}},{"id":"no-reason","state":"queued","spec":{"name":"new sample","priority":"experiment"}}]}`)
	}))
	defer server.Close()
	saved := os.Stdout
	defer func() { os.Stdout = saved }()
	f, err := os.CreateTemp(t.TempDir(), "status")
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = f
	if err = run([]string{"status", "--queue", server.URL, "--token", "test-only"}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"host example: degraded", "hypha example capability profile v0:", "benchmark complete", "cpu sha256 123.00 bytes/s", "unified memory", "NVIDIA (CUDA): 16.0 GiB", "hypha example:", "hypha on battery; waiting for external power", "sample@1 · kind of work sample · qualified", "running sample", "hypha=example", "waiting sample", "waiting for a hypha to take this job"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestLogsNoneOperatorOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/private-job" {
			fmt.Fprint(w, `{"id":"private-job","state":"succeeded","spec":{"logs":"none"}}`)
			return
		}
		if r.URL.Path == "/api/status" {
			fmt.Fprint(w, `{"jobs":[{"id":"private-job","state":"succeeded","spec":{"name":"private","logs":"none"}},{"id":"default-job","state":"queued","spec":{"name":"default"}}]}`)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer server.Close()
	for _, command := range []string{"status", "logs"} {
		args := []string{command}
		if command == "logs" {
			args = append(args, "private-job", "--follow")
		}
		args = append(args, "--queue", server.URL, "--token", "test-only")
		out, _, err := captureUsageOutput(t, args)
		if err != nil {
			t.Fatal(err)
		}
		if command == "status" && (!strings.Contains(out, "logs=none") || !strings.Contains(out, "logs=keep")) {
			t.Fatal(out)
		}
		if command == "logs" && !strings.Contains(out, "logs are disabled") {
			t.Fatal(out)
		}
	}
}

func TestStatusStoppedWorkerBudgetIsLastReported(t *testing.T) {
	for _, effective := range []bool{false, true} {
		m := &myce.Machine{ID: "stopped", Gone: true, HostMemoryBudgetGB: 22}
		if effective {
			m.EffectiveHostMemoryBudgetGB = 22
			m.HostMemoryAllowanceBytes = 22 << 30
		}
		f, err := os.CreateTemp(t.TempDir(), "status")
		if err != nil {
			t.Fatal(err)
		}
		saved := os.Stdout
		func() {
			os.Stdout = f
			defer func() { os.Stdout = saved }()
			printStatus(myce.Snapshot{Machines: []*myce.Machine{m}}, false)
		}()
		f.Close()
		data, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		want := "host ceiling budget (last reported) 22 GiB"
		if effective {
			want = "host allowance (last reported) 23622320128 bytes (UNQUALIFIED)"
		}
		if !strings.Contains(string(data), want) {
			t.Fatal(string(data))
		}
	}
}

func TestStatusUsabilityStructuredFieldsAndInternalLabel(t *testing.T) {
	output := captureJoiningOutput(t, func() error {
		printStatus(myce.Snapshot{Machines: []*myce.Machine{{ID: "fake-1", Environments: []string{"app"}, EnvironmentBuildSeconds: map[string]float64{"app": 1.25}, ModelCopySeconds: map[string]float64{"model": 2.5}}}, Jobs: []*myce.Job{{ID: "12345678abcdef", State: "failed", Spec: myce.Spec{Name: "extension-check", Project: "extension-check", Class: "batch", Extension: "sample@2", ExtensionCheck: &myce.ExtensionCheckRequest{Work: "extension:sample"}}}}}, false)
		return nil
	})
	for _, expected := range []string{"internal extension check", "checking extension: sample@2", "[extension-check/batch]", "failed", "build_seconds=1.250", "seconds=2.500"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("missing %s: %s", expected, output)
		}
	}
	if strings.Contains(output, "map[") {
		t.Fatal("raw map in status", output)
	}
}

func TestStatusQueueAndWorkerRunningReleases(t *testing.T) {
	q, _, _, _ := waitQueue(t)
	release := myce.RunningRelease{Version: "v1.2.3", Commit: "0123456789abcdef"}
	m := myce.Machine{CapabilityProfile: &myce.CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 16 << 30}, ID: "worker", Session: "running-session", GPU: "fake", FreeMemoryGB: 8, Release: &release}
	if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "test-only"}.Handler())
	defer server.Close()
	snapshot, err := status(context.Background(), myce.Client{URL: server.URL, Token: "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.QueueRelease != myce.CurrentRelease() || len(snapshot.Machines) != 1 || snapshot.Machines[0].Release == nil || *snapshot.Machines[0].Release != release {
		t.Fatalf("running releases missing from API: %+v", snapshot)
	}
	out, _, err := captureUsageOutput(t, []string{"status", "--queue", server.URL, "--token", "test-only"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"queue release: " + myce.CurrentRelease().Version + " (" + myce.CurrentRelease().Commit + ")", "worker worker release: v1.2.3 (0123456789abcdef)"} {
		if !strings.Contains(out, want) {
			t.Errorf("status missing %q: %s", want, out)
		}
	}
}
