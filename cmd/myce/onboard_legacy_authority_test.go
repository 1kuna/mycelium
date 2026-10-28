package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestEnrollmentLegacyEnvironmentFileUpgradePath(t *testing.T) {
	c, probe := savedInstallFixture(t, "worker")
	c.Enrollment = false
	c.EnvironmentFile = filepath.Join(c.NAS, "fleet.env")
	const secret = "synthetic-legacy-upgrade-secret"
	credentials := []byte("MYCE_TOKEN=" + secret + "\nMYCE_QUEUE=" + c.Queue + "\n")
	if err := os.WriteFile(c.EnvironmentFile, credentials, 0600); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(c)
	if err := os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"install", "worker", "--enroll", "--config", c.Config}
	err := enrollmentCommand(args, probe, c.NAS, nil)
	if err == nil || !strings.Contains(err.Error(), "legacy EnvironmentFile authority") || !strings.Contains(err.Error(), "install worker --config "+shellQuote(c.Config)+" --dry-run") || strings.Contains(err.Error(), secret) {
		t.Fatalf("missing safe supported upgrade instruction: %v", err)
	}
	// A real platform mismatch must not suggest using this incompatible candidate.
	wrongPlatform := probe
	wrongPlatform.Arch = "arm64"
	if err = enrollmentCommand(args, wrongPlatform, c.NAS, nil); err == nil || !strings.Contains(err.Error(), "platform does not match") || strings.Contains(err.Error(), "legacy EnvironmentFile") {
		t.Fatalf("platform and authority refusal conflated: %v", err)
	}
	installArgs := []string{"install", "worker", "--config", c.Config}
	preview := captureJoiningOutput(t, func() error {
		return setupCommandWithProbe(append(installArgs, "--dry-run"), probe, c.NAS)
	})
	if !strings.Contains(preview, "preserve saved EnvironmentFile credential reference") || strings.Contains(preview, secret) {
		t.Fatal("preview did not safely identify retained authority")
	}
	got, err := os.ReadFile(c.Config)
	if err != nil || string(got) != string(data) {
		t.Fatal("preview changed saved config", err)
	}
	output := captureJoiningOutput(t, func() error {
		return setupCommandWithProbe(installArgs, probe, c.NAS)
	})
	if strings.Contains(output, secret) {
		t.Fatal("installer exposed credential")
	}
	installed, err := myce.ReadServiceConfig(c.Config)
	if err != nil || !reflect.DeepEqual(installed, c) {
		t.Fatal("in-place upgrade changed saved worker settings", err)
	}
	got, err = os.ReadFile(c.EnvironmentFile)
	if err != nil || string(got) != string(credentials) {
		t.Fatal("in-place upgrade changed credential reference bytes", err)
	}
}
