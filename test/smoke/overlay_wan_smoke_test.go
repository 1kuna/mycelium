//go:build smoke

package smoke

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/membership"
)

func TestLibp2pWANOverlaySmoke(t *testing.T) {
	bootstrap := splitSmokeList(os.Getenv("MYCELIUM_WAN_OVERLAY_BOOTSTRAP"))
	remoteID := strings.TrimSpace(os.Getenv("MYCELIUM_WAN_OVERLAY_REMOTE_ID"))
	if len(bootstrap) == 0 || remoteID == "" {
		t.Skip("set MYCELIUM_WAN_OVERLAY_BOOTSTRAP and MYCELIUM_WAN_OVERLAY_REMOTE_ID for WAN overlay smoke")
	}
	listen := splitSmokeList(os.Getenv("MYCELIUM_WAN_OVERLAY_LISTEN"))
	if len(listen) == 0 {
		listen = []string{"/ip4/0.0.0.0/tcp/0"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	backend, err := membership.NewLibp2pOverlayBackend(ctx, membership.Libp2pOverlayConfig{
		ListenAddrs:    listen,
		BootstrapPeers: bootstrap,
		Token:          os.Getenv("MYCELIUM_WAN_OVERLAY_TOKEN"),
	})
	if err != nil {
		t.Fatalf("new WAN overlay backend: %v", err)
	}
	defer backend.CloseHost()
	localID := strings.TrimSpace(os.Getenv("MYCELIUM_WAN_OVERLAY_LOCAL_ID"))
	if localID == "" {
		localID = "wan-overlay-smoke"
	}
	if err := backend.Advertise(ctx, domain.Peer{ID: localID, Addresses: []string{"127.0.0.1:1"}, Compute: false}); err != nil {
		t.Fatalf("advertise local peer: %v", err)
	}
	remote := waitForOverlayPeer(t, ctx, backend, remoteID)
	target := strings.TrimSpace(os.Getenv("MYCELIUM_WAN_OVERLAY_REMOTE_ADDR"))
	if target == "" && len(remote.Addresses) > 0 {
		target = remote.Addresses[0]
	}
	if target == "" {
		t.Fatalf("remote peer %q has no address and MYCELIUM_WAN_OVERLAY_REMOTE_ADDR is unset", remoteID)
	}
	loopback, err := backend.Open(ctx, domain.Node{ID: remoteID, Address: target})
	if err != nil {
		t.Fatalf("open WAN overlay tunnel: %v", err)
	}
	defer backend.Close(ctx, remoteID)
	path := strings.TrimSpace(os.Getenv("MYCELIUM_WAN_OVERLAY_PROBE_PATH"))
	if path == "" {
		path = "/peer/health"
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Get("http://" + loopback + path)
	if err != nil {
		t.Fatalf("probe WAN overlay tunnel: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("WAN overlay probe status = %s", resp.Status)
	}
}

func waitForOverlayPeer(t *testing.T, ctx context.Context, backend *membership.Libp2pOverlayBackend, id string) domain.Peer {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		peers, err := backend.Peers(ctx)
		if err != nil {
			t.Fatalf("overlay peers: %v", err)
		}
		for _, peer := range peers {
			if peer.ID == id {
				return peer
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for overlay peer %q", id)
		case <-ticker.C:
		}
	}
}

func splitSmokeList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
