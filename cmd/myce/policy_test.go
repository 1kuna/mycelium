package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func TestPolicyCLI(t *testing.T) {
	clock := &bindClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.ConfigureHosts(map[string]myce.HostPolicy{"approved-host": {}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "owner-test"}.Handler())
	defer server.Close()
	t.Setenv("MYCE_QUEUE", server.URL)
	t.Setenv("MYCE_TOKEN", "owner-test")
	var out bytes.Buffer
	if err := policyCommand([]string{"get", "--yaml"}, &out); err != nil {
		t.Fatal(err)
	}
	policy, err := myce.ReadPriorityPolicy(strings.NewReader(out.String()))
	if err != nil {
		t.Fatal(err)
	}
	policy.Projects = map[string]myce.ProjectPolicy{"test": {Weight: 1, ProductionParityHosts: map[string][]string{"kind:sample": {"approved-host"}}}}
	out.Reset()
	if err := yaml.NewEncoder(&out).Encode(policy); err != nil {
		t.Fatal(err)
	}
	// The parity list is project-owned: queue policy cannot widen it.
	refused := filepath.Join(t.TempDir(), "refused.yaml")
	if err := os.WriteFile(refused, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := policyCommand([]string{"set", refused, "--if-revision", "0"}, &out); err == nil || !strings.Contains(err.Error(), "parity_hosts_project_owned") || !strings.Contains(err.Error(), "myce parity add") {
		t.Fatal("policy set edited a project's parity list:", err)
	}
	policy.Projects = map[string]myce.ProjectPolicy{"test": {Weight: 1}}
	out.Reset()
	if err := yaml.NewEncoder(&out).Encode(policy); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(file, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{out.String(), "version: 1\ntiers:\n  - name: shared\n    selectors: ['*/*']\nprojects:\n  test: {}\n"} {
		if _, err := myce.ReadPriorityPolicy(strings.NewReader(body)); err != nil {
			t.Fatal(err)
		}
	}
	out.Reset()
	if err := policyCommand([]string{"set", file, "--if-revision", "0"}, &out); err != nil {
		t.Fatal(err)
	}
	var record myce.PriorityPolicyRecord
	if err := json.Unmarshal(out.Bytes(), &record); err != nil || record.Revision != 1 || !reflect.DeepEqual(record.Policy.Projects["test"], myce.ProjectPolicy{Weight: 1}) {
		t.Fatalf("%s %v", out.String(), err)
	}
	if err := policyCommand([]string{"set", file, "--if-revision", "0"}, &out); err == nil {
		t.Fatal("stale CLI overwrite accepted")
	}
	if err := policyCommand([]string{"set", file}, &out); err == nil {
		t.Fatal("missing CAS accepted")
	}
	if _, err := myce.ReadPriorityPolicy(strings.NewReader("version: 1\ntires: []")); err == nil {
		t.Fatal("unknown YAML accepted")
	}

	s := myce.Spec{Project: "test", Name: "review", Run: myce.RunSpec{Command: []string{"echo", "done"}}}
	s.Needs.Platform.Any = true
	s.Needs.MemoryGB = 1
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	until := clock.Now().Add(time.Hour).Format(time.RFC3339)
	if err = pauseCommand(context.Background(), "pause", []string{j.ID, "--reason", "review input", "--until", until, "--queue", server.URL}, &out); err != nil {
		t.Fatal(err)
	}
	var paused myce.Job
	if err = json.Unmarshal(out.Bytes(), &paused); err != nil || paused.State != "paused" || paused.Pause == nil || paused.Pause.Reason != "review input" || paused.Pause.Until.Format(time.RFC3339) != until {
		t.Fatal("bad pause reply", out.String(), err)
	}
	out.Reset()
	if err = pauseCommand(context.Background(), "resume", []string{j.ID, "--queue", server.URL}, &out); err != nil {
		t.Fatal(err)
	}
	var resumed myce.Job
	if err = json.Unmarshal(out.Bytes(), &resumed); err != nil || resumed.ID != j.ID || resumed.State != "queued" || resumed.Pause != nil || resumed.Queued != j.Queued {
		t.Fatal("bad resume reply", out.String(), err)
	}
}

// Rolling upgrade, CLI first: a policy exported from an older queue sets there
// unchanged, and re-applying it after the queue upgrade keeps host holds.
func TestPolicyCLIRollingUpgradeKeepsHostHolds(t *testing.T) {
	older := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			ExpectedRevision uint64 `json:"expected_revision"`
			Policy           struct {
				Version  int                           `json:"version"`
				Tiers    []myce.PriorityTier           `json:"tiers"`
				Projects map[string]myce.ProjectPolicy `json:"projects"`
			} `json:"policy"`
		}
		if r.Method == "PUT" {
			d := json.NewDecoder(r.Body)
			d.DisallowUnknownFields()
			if err := d.Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"revision": 0, "policy": map[string]any{"version": 1, "tiers": []map[string]any{{"name": "shared", "selectors": []string{"*/*"}}}, "projects": map[string]any{}}})
	}))
	defer older.Close()
	t.Setenv("MYCE_QUEUE", older.URL)
	t.Setenv("MYCE_TOKEN", "owner-test")
	var out bytes.Buffer
	if err := policyCommand([]string{"get", "--yaml"}, &out); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(file, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := policyCommand([]string{"set", file, "--if-revision", "0"}, &out); err != nil {
		t.Fatal("older queue rejected an unchanged export:", err)
	}

	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), &bindClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.ConfigureHosts(map[string]myce.HostPolicy{"gpu-host": {ProductionHold: true, ProductionHoldScope: "device"}}); err != nil {
		t.Fatal(err)
	}
	upgraded := httptest.NewServer(myce.API{Queue: q, Token: "owner-test"}.Handler())
	defer upgraded.Close()
	t.Setenv("MYCE_QUEUE", upgraded.URL)
	if err := policyCommand([]string{"set", file, "--if-revision", "0"}, &out); err != nil {
		t.Fatal(err)
	}
	if got := q.PriorityPolicy().Policy.Hosts["gpu-host"]; !got.ProductionHold {
		t.Fatalf("re-applied export cleared the host hold: %+v", q.PriorityPolicy().Policy.Hosts)
	}
}

func TestPolicyCLIExplicitConfigChangeAfterRestart(t *testing.T) {
	for _, tc := range []struct{ name, config string }{
		{"edit projects", "sample:\n  weight: 5\n"},
		{"clear projects", "{}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			hostFile := filepath.Join(dir, "hosts.yaml")
			if err := os.WriteFile(hostFile, []byte("hosts:\n  gpu-host: {production_hold: true}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(dir, "projects.yaml")
			if err := os.WriteFile(config, []byte("sample:\n  weight: 1\n"), 0600); err != nil {
				t.Fatal(err)
			}
			start := func() *myce.Dev {
				t.Helper()
				policies, err := myce.ReadProjectPolicies(config)
				if err != nil {
					t.Fatal(err)
				}
				hosts, err := myce.ReadHostPolicies(hostFile)
				if err != nil {
					t.Fatal(err)
				}
				d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(dir, "state", "queue.db"), Checkpoints: filepath.Join(dir, "checkpoints"), Token: "policy-test", Workers: 1, GPU: "cpu", HostPolicies: hosts, ProjectPolicies: policies, Interval: 10 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { d.Close() })
				return d
			}
			d := start()
			initial := d.Queue.PriorityPolicy()
			if initial.Revision != 1 || initial.Policy.Projects["sample"].Weight != 1 {
				t.Fatalf("bootstrap: %+v", initial)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			// Startup files no longer act as live edits, even if their contents change.
			if err := os.WriteFile(config, []byte(tc.config), 0600); err != nil {
				t.Fatal(err)
			}
			d = start()
			if got := d.Queue.PriorityPolicy(); !reflect.DeepEqual(got, initial) {
				t.Fatalf("startup changed stored policy: %+v", got)
			}
			t.Setenv("MYCE_QUEUE", d.URL)
			t.Setenv("MYCE_TOKEN", "policy-test")
			var out bytes.Buffer
			if err := policyCommand([]string{"get", "--yaml"}, &out); err != nil {
				t.Fatal(err)
			}
			policy, err := myce.ReadPriorityPolicy(&out)
			if err != nil {
				t.Fatal(err)
			}
			// Import the deliberate config edit into the exported current policy.
			policy.Projects, err = myce.ReadProjectPolicies(config)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := yaml.Marshal(policy)
			if err != nil {
				t.Fatal(err)
			}
			file := filepath.Join(dir, "policy.yaml")
			if err := os.WriteFile(file, raw, 0600); err != nil {
				t.Fatal(err)
			}
			out.Reset()
			if err := policyCommand([]string{"set", file, "--if-revision", "1"}, &out); err != nil {
				t.Fatal(err)
			}
			if got := d.Queue.PriorityPolicy(); got.Revision != 2 || !reflect.DeepEqual(got.Policy, policy) {
				t.Fatalf("explicit config edit not applied: %+v", got)
			}
			if err := policyCommand([]string{"set", file, "--if-revision", "1"}, &out); err == nil {
				t.Fatal("stale config edit accepted")
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d = start()
			if got := d.Queue.PriorityPolicy(); got.Revision != 2 || !reflect.DeepEqual(got.Policy, policy) {
				t.Fatalf("explicit config edit lost on restart: %+v", got)
			}
			t.Setenv("MYCE_QUEUE", d.URL)
			out.Reset()
			if err := policyCommand([]string{"get", "--yaml"}, &out); err != nil {
				t.Fatal(err)
			}
			exported, err := myce.ReadPriorityPolicy(&out)
			if err != nil || !reflect.DeepEqual(exported, policy) {
				t.Fatalf("export lost explicit project settings: %+v, %v", exported, err)
			}

			// Export, change hold scope, set and restart with unchanged host file.
			if tc.name == "edit projects" {
				out.Reset()
				if err := policyCommand([]string{"get", "--yaml"}, &out); err != nil {
					t.Fatal(err)
				}
				exported := strings.Replace(out.String(), "production_hold: true", "production_hold: true\n        production_hold_scope: device", 1)
				if exported == out.String() {
					t.Fatalf("host hold missing from export:\n%s", out.String())
				}
				file = filepath.Join(dir, "policy.yaml")
				if err := os.WriteFile(file, []byte(exported), 0600); err != nil {
					t.Fatal(err)
				}
				revision := strconv.FormatUint(d.Queue.PriorityPolicy().Revision, 10)
				if err := policyCommand([]string{"set", file, "--if-revision", revision}, &out); err != nil {
					t.Fatal(err)
				}
				if err := d.Close(); err != nil {
					t.Fatal(err)
				}
				d = start()
				if got := d.Queue.PriorityPolicy().Policy.Hosts["gpu-host"]; got != (myce.HostPolicy{ProductionHold: true, ProductionHoldScope: "device"}) {
					t.Fatalf("scope lost on restart: %+v", got)
				}
			}
		})
	}
}
