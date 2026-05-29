//go:build smoke

package smoke

import (
	"net"
	"os"
	"testing"
	"time"
)

func TestFleetMacMiniSmokeRequiresAddress(t *testing.T) {
	addr := os.Getenv("MYCELIUM_REMOTE_PEER_ADDR")
	if addr == "" {
		t.Skip("set MYCELIUM_REMOTE_PEER_ADDR for Phase 1 fleet smoke")
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("second peer address %s is not reachable: %v", addr, err)
	}
	_ = conn.Close()
	t.Fatalf("second peer is reachable, but the Phase 1 remote-node run harness still needs to be completed")
}
