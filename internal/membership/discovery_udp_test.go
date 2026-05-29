package membership

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"mycelium/internal/domain"
)

func TestUDPDiscoveryAnnouncesNode(t *testing.T) {
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	node := domain.Node{ID: "node-a", Address: "127.0.0.1:1"}

	if err := (UDPDiscovery{BroadcastAddr: conn.LocalAddr().String()}).Announce(ctx, node); err != nil {
		t.Fatalf("Announce: %v", err)
	}
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	var got domain.Node
	if err := json.Unmarshal(buf[:n], &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != node.ID || got.Address != node.Address {
		t.Fatalf("got = %+v", got)
	}
}

func TestUDPDiscoveryDiscoversNode(t *testing.T) {
	holder, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	addr := holder.LocalAddr().String()
	if err := holder.Close(); err != nil {
		t.Fatalf("close reserve: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	discovery := UDPDiscovery{ListenAddr: addr, BroadcastAddr: addr, MaxPackets: 1}
	results := make(chan []domain.Node, 1)
	errs := make(chan error, 1)
	go func() {
		nodes, err := discovery.Discover(ctx)
		if err != nil {
			errs <- err
			return
		}
		results <- nodes
	}()
	node := domain.Node{ID: "node-a", Address: "127.0.0.1:1"}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-errs:
			t.Fatalf("Discover: %v", err)
		case nodes := <-results:
			if len(nodes) != 1 || nodes[0].ID != node.ID {
				t.Fatalf("nodes = %+v", nodes)
			}
			return
		case <-ticker.C:
			if err := discovery.Announce(ctx, node); err != nil {
				t.Fatalf("Announce: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestUDPDiscoveryRejectsBadInput(t *testing.T) {
	defaulted := NewUDPDiscovery("", "")
	if defaulted.ListenAddr != ":51849" || defaulted.BroadcastAddr != DefaultUDPDiscoveryAddr || defaulted.MaxPackets != 16 {
		t.Fatalf("defaulted = %+v", defaulted)
	}
	if err := (UDPDiscovery{BroadcastAddr: "127.0.0.1:1"}).Announce(context.Background(), domain.Node{}); err == nil {
		t.Fatal("missing node id accepted")
	}
	if err := (UDPDiscovery{BroadcastAddr: "%"}).Announce(context.Background(), domain.Node{ID: "node-a"}); err == nil {
		t.Fatal("bad broadcast address accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	nodes, err := (UDPDiscovery{ListenAddr: "127.0.0.1:0", MaxPackets: 1}).Discover(ctx)
	if err != nil {
		t.Fatalf("Discover timeout: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestUDPDiscoveryRejectsMalformedPackets(t *testing.T) {
	holder, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	addr := holder.LocalAddr().String()
	if err := holder.Close(); err != nil {
		t.Fatalf("close reserve: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	discovery := UDPDiscovery{ListenAddr: addr, MaxPackets: 1}
	errs := make(chan error, 1)
	go func() {
		_, err := discovery.Discover(ctx)
		errs <- err
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("expected malformed packet error")
			}
			return
		case <-ticker.C:
			conn, err := net.Dial("udp", addr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			_, _ = conn.Write([]byte(`{"address":"missing-id"}`))
			_ = conn.Close()
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
