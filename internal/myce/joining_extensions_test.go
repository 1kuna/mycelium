package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A trusted joining client must retain both startup facts and qualification
// reports while running the two extension job forms through the real worker.
func TestJoiningExtensionEndToEnd(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "must-not-reach-extension")
	q, _ := fixture(t)
	server, invitation := joiningTestServer(t, API{Queue: q, Token: "throwaway-test-only"}.Handler())
	invitation.Address = ""
	discovery := &fakeDiscovery{addresses: []string{server.URL}}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
	defer cancel()
	client, err := ResolveJoining(ctx, invitation, "", discovery)
	if err != nil || discovery.calls != 1 || client.URL != server.URL {
		t.Fatal("trusted discovery failed", err)
	}
	root, shared, state := t.TempDir(), t.TempDir(), t.TempDir()
	e := fakeExtension(t)
	installQualified(t, root, e, "")
	if err := client.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{
		"input":  "joined-extension-result\n",
		"script": "test -z \"$MYCE_TOKEN\" || exit 99\nprintf joined-program-result > output\n",
	} {
		if err := os.WriteFile(filepath.Join(shared, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	w := Worker{Client: client, StateDir: state, ExtensionRoot: root, JobFileRoot: shared, Machine: machine("joined", "fake"), Interval: 5 * time.Millisecond}
	// Exercise ordinary startup detection rather than the scheduler fixture profile.
	w.Machine.CapabilityProfile = nil
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("joined worker did not exit")
		}
	})
	for _, program := range []bool{false, true} {
		s := Spec{Name: "joined-extension", Project: "test", Priority: "experiment", Kind: "sample", Inputs: map[string]string{"input": "input"}, Outputs: map[string]string{"output": "results"}}
		want := "joined-extension-result\n"
		if program {
			s.Kind = ""
			s.Program = &Program{Type: "sample-script", File: "script"}
			s.Inputs = map[string]string{"script": "script"}
			want = "joined-program-result"
		}
		var submitted Job
		if err := client.Call(ctx, "POST", "/api/jobs", s, &submitted); err != nil {
			t.Fatal(err)
		}
		var completed *Job
		await(t, func() bool {
			var snap Snapshot
			if err := client.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
				t.Fatal(err)
			}
			for _, j := range snap.Jobs {
				if j.ID == submitted.ID && terminal(j.State) {
					if j.State != "succeeded" {
						t.Fatalf("joined extension job: state=%s reason=%s", j.State, j.Reason)
					}
					completed = j
				}
			}
			if completed == nil {
				return false
			}
			if len(snap.Machines) != 1 || snap.Machines[0].CapabilityProfile == nil || len(snap.Machines[0].ExtensionReports) != 2 || snap.Machines[0].CapabilityProfile.Onboarding != "hardware-facts-only; throughput pending" || snap.Machines[0].CapabilityProfile.Benchmark != nil {
				t.Fatal("check-in lost profile or extension reports")
			}
			for _, report := range snap.Machines[0].ExtensionReports {
				if !report.Qualified || report.Digest != e.Digest {
					t.Fatal("check-in lost qualification identity")
				}
			}
			return true
		})
		if completed.Extension == nil || completed.Extension.Digest != e.Digest {
			t.Fatal("attempt lost extension identity")
		}
		data, err := os.ReadFile(filepath.Join(shared, completed.OutputFiles["output"]))
		if err != nil || string(data) != want {
			t.Fatal("joined extension output mismatch", err)
		}
	}
	if _, err := os.Stat(filepath.Join(state, "capability-profile.json")); !os.IsNotExist(err) {
		t.Fatal("default join wrote benchmark cache", err)
	}
}
