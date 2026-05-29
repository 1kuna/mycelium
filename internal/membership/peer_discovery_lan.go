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

const DefaultPeerDiscoveryAddr = "255.255.255.255:51850"

type PeerLANDiscovery struct {
	ListenAddr    string
	BroadcastAddr string
	MaxPackets    int
}

func NewPeerLANDiscovery(listenAddr, broadcastAddr string) PeerLANDiscovery {
	if listenAddr == "" {
		listenAddr = ":51850"
	}
	if broadcastAddr == "" {
		broadcastAddr = DefaultPeerDiscoveryAddr
	}
	return PeerLANDiscovery{ListenAddr: listenAddr, BroadcastAddr: broadcastAddr, MaxPackets: 16}
}

func (d PeerLANDiscovery) Advertise(ctx context.Context, self domain.Peer) error {
	if err := validatePeer(self); err != nil {
		return err
	}
	addr := d.BroadcastAddr
	if addr == "" {
		addr = DefaultPeerDiscoveryAddr
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
	data, err := json.Marshal(self)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}

func (d PeerLANDiscovery) Peers(ctx context.Context) ([]domain.Peer, error) {
	conn, err := d.listen(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	max := d.MaxPackets
	if max == 0 {
		max = 16
	}
	peers := map[string]domain.Peer{}
	for len(peers) < max {
		peer, err := readPeer(ctx, conn)
		if err != nil {
			if peerReadDone(ctx, err) {
				return peerList(peers), nil
			}
			return nil, err
		}
		peers[peer.ID] = peer
	}
	return peerList(peers), nil
}

func (d PeerLANDiscovery) WatchPeers(ctx context.Context) (<-chan domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := d.listen(ctx)
	if err != nil {
		return nil, err
	}
	ch := make(chan domain.Peer, 16)
	go func() {
		defer close(ch)
		defer conn.Close()
		for {
			peer, err := readPeer(ctx, conn)
			if err != nil {
				return
			}
			select {
			case ch <- peer:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (d PeerLANDiscovery) listen(ctx context.Context) (net.PacketConn, error) {
	listenAddr := d.ListenAddr
	if listenAddr == "" {
		listenAddr = ":51850"
	}
	conn, err := net.ListenPacket("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	return conn, nil
}

func readPeer(ctx context.Context, conn net.PacketConn) (domain.Peer, error) {
	buf := make([]byte, 64*1024)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		return domain.Peer{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.Peer{}, err
	}
	var peer domain.Peer
	if err := json.Unmarshal(buf[:n], &peer); err != nil {
		return domain.Peer{}, err
	}
	if err := validatePeer(peer); err != nil {
		return domain.Peer{}, err
	}
	return peer, nil
}

func validatePeer(peer domain.Peer) error {
	if peer.ID == "" {
		return fmt.Errorf("peer id is required")
	}
	if len(peer.Addresses) == 0 {
		return fmt.Errorf("peer %q has no reachable address", peer.ID)
	}
	return nil
}

func peerReadDone(ctx context.Context, err error) bool {
	if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func peerList(peers map[string]domain.Peer) []domain.Peer {
	out := make([]domain.Peer, 0, len(peers))
	for _, peer := range peers {
		out = append(out, peer)
	}
	return out
}

var _ ports.PeerDiscovery = PeerLANDiscovery{}
