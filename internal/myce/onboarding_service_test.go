package myce

import (
	"encoding/json"
	"html"
	"os"
	"strings"
	"testing"
)

func completeLaunchdProbe(t *testing.T, path string, c ServiceConfig) error {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	text := string(b)
	for _, want := range []string{"<key>RunAtLoad</key><true/>", "<string>detect</string>", "<string>--config</string>", html.EscapeString(c.Path)} {
		if !strings.Contains(text, want) {
			t.Fatalf("probe does not use saved service environment: missing %s", want)
		}
	}
	_, rest, ok := strings.Cut(text, "<key>StandardOutPath</key><string>")
	if !ok {
		t.Fatal("probe has no output path")
	}
	out, _, _ := strings.Cut(rest, "</string>")
	data, _ := json.Marshal(HardwarePreflight{Detection: c.Detection, Devices: c.MachineDevices})
	return os.WriteFile(html.UnescapeString(out), data, 0600)
}

// Backend availability is checked only when a recipe needs it. Retained tool
// PATH entries on upgrades must not make an offline CPU service unusable.
