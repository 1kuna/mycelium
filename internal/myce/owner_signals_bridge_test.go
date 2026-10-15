package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mycelium/internal/hostbridge"
)

func TestWSLHostBridgeEndToEnd(t *testing.T) {
	signals := hostbridge.Signals{IdleKnown: true, IdleSeconds: 400, Power: "ac", Heat: "unknown", GPUHeat: "nominal", HeatCoverage: "gpu-only"}
	c := hostbridge.Config{Address: "127.0.0.1:47353", Key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	handler, err := hostbridge.Handler(c, func(context.Context) hostbridge.Signals { return signals })
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	c.Address = server.Listener.Addr().String()
	path := filepath.Join(t.TempDir(), "bridge.json")
	raw, _ := json.Marshal(c)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_HOST_BRIDGE_CONFIG", path)
	p, files := linuxSignalsFixture()
	files["/proc/sys/kernel/osrelease"] = "microsoft-standard-WSL2"
	p.command = func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("WSL queried guest sessions")
		return nil, nil
	}
	p.glob = func(string) ([]string, error) { t.Fatal("WSL queried guest power/heat"); return nil, nil }
	p.host = configuredHostOwnerSignals
	clock := &dockerTestClock{now: time.Unix(1000, 0)}
	for _, tc := range []struct {
		name      string
		change    func()
		available bool
		reason    string
	}{
		{"undeclared GPU-only", func() {}, false, "host CPU thermal coverage unavailable"},
		{"desktop GPU-only", func() {}, true, "CPU thermal coverage waived for desktop"},
		{"owner returns", func() { signals.IdleSeconds = 0 }, false, "waiting for 5 idle minutes"},
		{"session coverage lost", func() { signals.IdleKnown = false }, false, "owner activity unavailable"},
		{"battery", func() { signals.Laptop = true; signals.Power = "battery" }, false, "on battery"},
		{"detected laptop", func() { signals.Laptop = true }, false, "host CPU thermal coverage unavailable"},
		{"unknown GPU heat", func() { signals.SetGPUHeat("unknown") }, false, "heat sensing unavailable"},
		{"unsafe GPU heat", func() { signals.SetGPUHeat("serious") }, false, "cooling down"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signals = hostbridge.Signals{IdleKnown: true, IdleSeconds: 400, Power: "ac", Heat: "unknown", GPUHeat: "nominal", HeatCoverage: "gpu-only"}
			policy := availabilityPolicy{clock: clock, daily: true, desktop: tc.name != "undeclared GPU-only"}
			tc.change()
			s := p.detect(context.Background())
			a := policy.evaluate(s)
			if a.Available != tc.available || a.Reason != tc.reason {
				t.Fatal(a)
			}
		})
	}
	policy := availabilityPolicy{clock: clock, daily: true, desktop: true}
	// Credential revocation or a stopped host must never fall back to guest data.
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s := p.detect(context.Background())
	if s.IdleKnown || s.Power != "unknown" || s.Heat != "unknown" {
		t.Fatal("revocation admitted guest data", s)
	}
	if a := policy.evaluate(s); a.Available {
		t.Fatal("revoked bridge admitted work")
	}
	raw, _ = json.Marshal(c)
	os.WriteFile(path, raw, 0600)
	server.Close()
	if s = p.detect(context.Background()); s.IdleKnown || s.Power != "unknown" {
		t.Fatal("stopped helper admitted work", s)
	}
	// Non-WSL Linux keeps its existing probes even if a bridge is configured.
	normal, _ := linuxSignalsFixture()
	normal.host = func(context.Context) OwnerSignals { t.Fatal("Linux queried Windows"); return OwnerSignals{} }
	if s = normal.detect(context.Background()); !s.IdleKnown || s.Heat != "nominal" {
		t.Fatal(s)
	}
}
