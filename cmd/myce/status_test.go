package main

import (
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
	for _, want := range []string{"host example: degraded", "hypha example capability profile v0:", "unified memory", "NVIDIA (CUDA): 16.0 GiB", "hypha example:", "hypha on battery; waiting for external power", "sample@1 · kind of work sample · qualified", "running sample", "hypha=example", "waiting sample", "waiting for a hypha to take this job"} {
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
			// The queue's allowance is part of the rule's capacity term.
			m.EffectiveHostMemoryBudgetGB = 22
			m.HostMemory = &myce.HostMemoryTerms{CapacityGB: 22, ChargedGB: 4, RoomGB: 18}
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
			want = "RAM capacity 22.0, charged 4.0, room 18.0 GiB"
		}
		if !strings.Contains(string(data), want) || effective && strings.Contains(string(data), "ceiling budget") {
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

// Default status for one job has one reason line: its binding reason and next action.
// Evidence, the isolation note, variant rows and the priority advisory once
// filled a dozen lines and 700 characters per read (gap 3); --history keeps them.
func TestStatusJobShowsOneReasonLine(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id":"waiting-job-0123456789","state":"queued","queued":"2026-11-12T00:00:00Z","isolation":"unsandboxed: evidence-isolation",
"hold":{"kind":"native_qualification","owner":"queue","reason":"its native check abc on host-a is queued","clears":"the native check passes","next":"wait; nothing to do","detail":"evidence-detail"},
"variant_eligibility":[{"extension":"x@1","host":"host-b","reasons":["evidence-variant"]}],
"priority_policy":{"tier":"experiment","selector":"*/*","revision":1},
"deadline_guidance":{"deadline_policy":{"tier":"deadline"},"eta":"unknown","message":"evidence-advisory"},
"spec":{"name":"waiting sample","project":"p","class":"experiment"}}`)
	}))
	defer server.Close()
	read := func(args ...string) string {
		t.Helper()
		saved := os.Stdout
		defer func() { os.Stdout = saved }()
		f, err := os.CreateTemp(t.TempDir(), "status")
		if err != nil {
			t.Fatal(err)
		}
		os.Stdout = f
		if err = run(append([]string{"status", "--queue", server.URL, "--token", "test-only"}, args...)); err != nil {
			t.Fatal(err)
		}
		f.Close()
		data, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	out := read("waiting-job-0123456789")
	reasons := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.Contains(line, "its native check abc on host-a is queued; next: wait; nothing to do") {
			reasons++
		} else if !strings.HasPrefix(line, "  authoring origin:") {
			t.Errorf("default status printed more than the job's reason line: %q", line)
		}
	}
	if reasons != 1 {
		t.Fatalf("want one reason line, got:\n%s", out)
	}
	if strings.Contains(out, "evidence-") {
		t.Fatalf("default status printed detail:\n%s", out)
	}
	detail := read("--history", "waiting-job-0123456789")
	for _, want := range []string{"evidence-detail", "evidence-isolation", "evidence-variant", "evidence-advisory"} {
		if !strings.Contains(detail, want) {
			t.Errorf("--history lost %q", want)
		}
	}
}
