package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func TestPolicyCLI(t *testing.T) {
	q, err := myce.Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), &bindClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)})
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
	if err := json.Unmarshal(out.Bytes(), &record); err != nil || record.Revision != 1 || !reflect.DeepEqual(record.Policy.Projects["test"].ProductionParityHosts, map[string][]string{"kind:sample": {"approved-host"}}) {
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
}

func TestPolicyCLIExplicitConfigChangeAfterRestart(t *testing.T) {
	for _, tc := range []struct{ name, config string }{
		{"edit projects", "sample:\n  weight: 5\n"},
		{"clear projects", "{}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
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
				d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(dir, "state", "queue.db"), Checkpoints: filepath.Join(dir, "checkpoints"), Token: "policy-test", Workers: 1, GPU: "cpu", ProjectPolicies: policies})
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
		})
	}
}
