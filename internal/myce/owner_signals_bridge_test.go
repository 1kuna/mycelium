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
	signals := hostbridge.Signals{IdleKnown: true, IdleSeconds: 400, Power: "ac", Heat: "nominal"}
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
	policy := availabilityPolicy{clock: clock, daily: true}
	for _, tc := range []struct {
		name      string
		change    func()
		available bool
		reason    string
	}{
		{"idle known safe", func() {}, true, ""},
		{"owner returns", func() { signals.IdleSeconds = 0 }, false, "waiting for 5 idle minutes"},
		{"session coverage lost", func() { signals.IdleSeconds = 0; signals.IdleKnown = false }, false, "owner activity unavailable"},
		{"battery", func() {
			signals.IdleKnown = true
			signals.IdleSeconds = 400
			signals.Laptop = true
			signals.Power = "battery"
		}, false, "on battery"},
		{"missing physical heat", func() { signals.Power = "ac"; signals.Heat = "unknown" }, false, "heat sensing unavailable"},
		{"unsafe physical heat", func() { signals.Heat = "serious" }, false, "cooling down"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.change()
			s := p.detect(context.Background())
			a := policy.evaluate(s)
			if a.Available != tc.available || a.Reason != tc.reason {
				t.Fatal(a)
			}
		})
	}
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
