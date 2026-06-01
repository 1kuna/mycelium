//go:build smoke

package smoke

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/lease"
	"mycelium/internal/membership"
	nodeagent "mycelium/internal/node"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestLANInstanceProxyThroughAuthenticatedTunnel(t *testing.T) {
	var backendAuth string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendAuth = r.Header.Get("Authorization")
		if r.URL.Path != "/v1/chat/completions" || r.URL.RawQuery != "probe=1" {
			t.Fatalf("backend url = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, `{"proxied":true}`)
	}))
	defer backend.Close()

	agent := nodeagent.NewAgent(
		fixtures.MakeNode(),
		mocks.NewBackendAdapter(),
		mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)),
		nodeagent.WithListenAddr(backend.URL),
		nodeagent.WithAllocator(lease.NewAllocator()),
	)
	remote := httptest.NewServer(nodeagent.HTTPServer{Agent: agent, AuthToken: "rpc-secret"})
	defer remote.Close()

	tunnel := membership.NewLANTunnel()
	tunnel.AuthToken = "rpc-secret"
	loopback, err := tunnel.Open(context.Background(), domain.Node{ID: "node_test", Address: remote.URL})
	if err != nil {
		t.Fatalf("Open tunnel: %v", err)
	}
	defer tunnel.Close(context.Background(), "node_test")

	client := nodeagent.NewHTTPClient("http://" + loopback)
	client.AuthToken = "rpc-secret"
	preset := fixtures.MakePreset()
	inst, err := client.Load(context.Background(), domain.LoadRequest{Preset: preset, Claim: fixtures.MakeClaim(1, 1), AcceleratorSet: []int{0}})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.HasPrefix(inst.Addr, "http://"+loopback+"/instances/") {
		t.Fatalf("proxied addr = %s", inst.Addr)
	}
	snap, err := client.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap.Instances) != 1 || snap.Instances[0].Addr != inst.Addr {
		t.Fatalf("snapshot instances = %+v addr=%s", snap.Instances, inst.Addr)
	}

	resp, err := http.Post(inst.Addr+"/v1/chat/completions?probe=1", "application/json", strings.NewReader(`{"model":"tiny"}`))
	if err != nil {
		t.Fatalf("proxy post: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read proxy body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "proxied") {
		t.Fatalf("proxy response = %s %s", resp.Status, body)
	}
	if backendAuth != "" {
		t.Fatalf("backend saw peer auth header %q", backendAuth)
	}
}
