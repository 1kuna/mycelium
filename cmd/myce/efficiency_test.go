package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestEfficiencyCommand(t *testing.T) {
	report := &myce.EfficiencyReport{CPUStatus: "unknown", Stranded: []myce.StrandedDemand{{JobID: "waiting", Host: "peer", Fields: []string{"targets"}}},
		Restrictions: []myce.ProjectRestrictions{{Project: "demo", Jobs: 2, HostRestricted: 1, StrandedSeconds: 3600, Advice: []string{"drop allowed_machines and excluded_machines"}}, {Project: "other", Jobs: 1}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/efficiency" {
			t.Errorf("broad read: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(report)
	}))
	defer server.Close()
	t.Setenv("HOME", t.TempDir())
	for _, asJSON := range []bool{false, true} {
		args := []string{"efficiency", "--queue", server.URL, "--token", "test"}
		if asJSON {
			args = append(args, "--json")
		}
		out, _, err := captureUsageOutput(t, args)
		if err != nil {
			t.Fatal(err)
		}
		if asJSON {
			var got myce.EfficiencyReport
			if err := json.Unmarshal([]byte(out), &got); err != nil || len(got.Stranded) != 1 {
				t.Fatal(out, err)
			}
		} else if !strings.Contains(out, "excluded by targets") || !strings.Contains(out, "CPU use unknown") {
			t.Fatal(out)
		}
	}
	out, _, err := captureUsageOutput(t, []string{"efficiency", "--queue", server.URL, "--token", "test", "--project", "demo"})
	if err != nil || !strings.Contains(out, "hosts named 1/2 (50%)") || !strings.Contains(out, "advice: drop allowed_machines") || strings.Contains(out, "peer") || strings.Contains(out, "other") {
		t.Fatal(out, err)
	}
}

// Consumers never export MYCE_TOKEN (the skill forbids it); efficiency and
// usage resolve the installed worker authority like status does.
func TestEfficiencyAndUsageUseInstalledAuthority(t *testing.T) {
	const token = "installed-fixture-token"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/efficiency":
			_ = json.NewEncoder(w).Encode(&myce.EfficiencyReport{CPUStatus: "unknown"})
		case "/api/usage":
			_ = json.NewEncoder(w).Encode(myce.UsageReport{})
		default:
			t.Errorf("unexpected read: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, name := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"} {
		t.Setenv(name, "")
	}
	joining := filepath.Join(home, "joining.json")
	hash := sha256.Sum256(server.Certificate().Raw)
	if err := myce.WriteInvitation(joining, myce.Invitation{Version: 1, Token: token, Address: server.URL, Fingerprint: hex.EncodeToString(hash[:])}); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(myce.ServiceConfig{Role: "worker", JoiningFile: joining})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"efficiency"}, {"usage"}} {
		out, stderr, err := captureUsageOutput(t, args)
		if err != nil || strings.Contains(out+stderr, token) {
			t.Fatalf("%v: %v %s %s", args, err, out, stderr)
		}
	}
}
