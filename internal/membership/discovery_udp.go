package membership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const DefaultUDPDiscoveryAddr = "255.255.255.255:51849"

type UDPDiscovery struct {
	ListenAddr    string
	BroadcastAddr string
	MaxPackets    int
}

func NewUDPDiscovery(listenAddr, broadcastAddr string) UDPDiscovery {
	if listenAddr == "" {
		listenAddr = ":51849"
	}
	if broadcastAddr == "" {
		broadcastAddr = DefaultUDPDiscoveryAddr
	}
	return UDPDiscovery{ListenAddr: listenAddr, BroadcastAddr: broadcastAddr, MaxPackets: 16}
}

func (d UDPDiscovery) Announce(ctx context.Context, node domain.Node) error {
	if node.ID == "" {
		return fmt.Errorf("node id is required")
	}
	addr := d.BroadcastAddr
	if addr == "" {
		addr = DefaultUDPDiscoveryAddr
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

func (d UDPDiscovery) Discover(ctx context.Context) ([]domain.Node, error) {
	listenAddr := d.ListenAddr
	if listenAddr == "" {
		listenAddr = ":51849"
	}
	conn, err := net.ListenPacket("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	defer close(done)

	max := d.MaxPackets
	if max == 0 {
		max = 16
	}
	nodes := make([]domain.Node, 0, max)
	buf := make([]byte, 64*1024)
	for len(nodes) < max {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nodes, nil
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				return nodes, nil
			}
			return nil, err
		}
		var node domain.Node
		if err := json.Unmarshal(buf[:n], &node); err != nil {
			return nil, err
		}
		if node.ID == "" {
			return nil, fmt.Errorf("discovered node missing id")
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

var _ ports.Discovery = UDPDiscovery{}
