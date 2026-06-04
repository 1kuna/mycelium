package membership

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

const DefaultPeerDiscoveryAddr = "255.255.255.255:51850"

type PeerLANDiscovery struct {
	ListenAddr    string
	BroadcastAddr string
	MaxPackets    int
	Token         string
	TokenManager  *TokenManager
	ScanDuration  time.Duration
	AdvertTTL     time.Duration
	Clock         ports.Clock
	PacketFactory PacketFactory
}

type PacketFactory interface {
	ListenPacket(ctx context.Context, network, address string, broadcast bool) (net.PacketConn, error)
}

type peerAdvertisement struct {
	Peer      domain.Peer `json:"peer"`
	TokenHash string      `json:"token_hash,omitempty"`
	ExpiresAt int64       `json:"expires_at,omitempty"`
	Nonce     string      `json:"nonce,omitempty"`
	Signature string      `json:"signature,omitempty"`
}

type signedAdvertisementPayload struct {
	PeerID    string   `json:"peer_id"`
	Addresses []string `json:"addresses"`
	Compute   bool     `json:"compute"`
	Version   string   `json:"version,omitempty"`
	TokenHash string   `json:"token_hash"`
	ExpiresAt int64    `json:"expires_at"`
	Nonce     string   `json:"nonce"`
}

func NewPeerLANDiscovery(listenAddr, broadcastAddr string) PeerLANDiscovery {
	if listenAddr == "" {
		listenAddr = ":51850"
	}
	if broadcastAddr == "" {
		broadcastAddr = DefaultPeerDiscoveryAddr
	}
	return PeerLANDiscovery{ListenAddr: listenAddr, BroadcastAddr: broadcastAddr, MaxPackets: 16, ScanDuration: 250 * time.Millisecond}
}

func (d PeerLANDiscovery) Advertise(ctx context.Context, self domain.Peer) error {
	if err := validatePeer(self); err != nil {
		return err
	}
	addr := d.BroadcastAddr
	if addr == "" {
		addr = DefaultPeerDiscoveryAddr
	}
	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return err
	}
	factory := d.packetFactory()
	conn, err := factory.ListenPacket(ctx, "udp4", "0.0.0.0:0", true)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}
	data, err := d.marshal(self)
	if err != nil {
		return err
	}
	_, err = conn.WriteTo(data, udpAddr)
	return err
}

func enableBroadcast(_, _ string, raw syscall.RawConn) error {
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
	}); err != nil {
		return err
	}
	return sockErr
}

func (d PeerLANDiscovery) Peers(ctx context.Context) ([]domain.Peer, error) {
	conn, err := d.listen(ctx, true)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	max := d.MaxPackets
	if max == 0 {
		max = 16
	}
	peers := map[string]domain.Peer{}
	seenNonces := map[string]struct{}{}
	for len(peers) < max {
		peer, accepted, err := d.readPeer(ctx, conn, seenNonces)
		if err != nil {
			if peerReadDone(ctx, err) {
				return peerList(peers), nil
			}
			return nil, err
		}
		if !accepted {
			continue
		}
		peers[peer.ID] = peer
	}
	return peerList(peers), nil
}

func (d PeerLANDiscovery) WatchPeers(ctx context.Context) (<-chan domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := d.listen(ctx, false)
	if err != nil {
		return nil, err
	}
	ch := make(chan domain.Peer, 16)
	go func() {
		defer close(ch)
		defer conn.Close()
		seenNonces := map[string]struct{}{}
		for {
			peer, accepted, err := d.readPeer(ctx, conn, seenNonces)
			if err != nil {
				return
			}
			if !accepted {
				continue
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

func (d PeerLANDiscovery) listen(ctx context.Context, bounded bool) (net.PacketConn, error) {
	listenAddr := d.ListenAddr
	if listenAddr == "" {
		listenAddr = ":51850"
	}
	conn, err := d.packetFactory().ListenPacket(ctx, "udp4", listenAddr, false)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, err
		}
	} else if bounded {
		duration := d.ScanDuration
		if duration == 0 {
			duration = 250 * time.Millisecond
		}
		clk := d.Clock
		if clk == nil {
			clk = clock.System{}
		}
		if err := conn.SetReadDeadline(clk.Now().Add(duration)); err != nil {
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

func (d PeerLANDiscovery) packetFactory() PacketFactory {
	if d.PacketFactory != nil {
		return d.PacketFactory
	}
	return netPacketFactory{}
}

type netPacketFactory struct{}

func (netPacketFactory) ListenPacket(ctx context.Context, network, address string, broadcast bool) (net.PacketConn, error) {
	if broadcast {
		listener := net.ListenConfig{Control: enableBroadcast}
		return listener.ListenPacket(ctx, network, address)
	}
	return net.ListenPacket(network, address)
}

func (d PeerLANDiscovery) marshal(peer domain.Peer) ([]byte, error) {
	if d.TokenManager != nil {
		hash, secret, err := d.TokenManager.CurrentSecret()
		if err != nil {
			return nil, err
		}
		return d.marshalSigned(peer, hash, secret)
	}
	if d.Token == "" {
		return json.Marshal(peer)
	}
	return d.marshalSigned(peer, tokenHash(d.Token), d.Token)
}

func (d PeerLANDiscovery) marshalSigned(peer domain.Peer, tokenHashValue, secret string) ([]byte, error) {
	nonce, err := randomNonce()
	if err != nil {
		return nil, err
	}
	ttl := d.AdvertTTL
	if ttl == 0 {
		ttl = 5 * time.Second
	}
	expiresAt := d.now().Add(ttl).Unix()
	payload, err := canonicalAdvertisementPayload(peer, tokenHashValue, expiresAt, nonce)
	if err != nil {
		return nil, err
	}
	return json.Marshal(peerAdvertisement{
		Peer:      peer,
		TokenHash: tokenHashValue,
		ExpiresAt: expiresAt,
		Nonce:     nonce,
		Signature: signAdvertisement(secret, payload),
	})
}

func (d PeerLANDiscovery) readPeer(ctx context.Context, conn net.PacketConn, seenNonces map[string]struct{}) (domain.Peer, bool, error) {
	buf := make([]byte, 64*1024)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		return domain.Peer{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return domain.Peer{}, false, err
	}
	advert, wrapped, err := decodePeerAdvertisement(buf[:n])
	if err != nil {
		return domain.Peer{}, false, nil
	}
	peer := advert.Peer
	if d.TokenManager != nil {
		if !wrapped || advert.Signature == "" {
			return domain.Peer{}, false, nil
		}
		secret, ok, err := d.TokenManager.SecretForHash(advert.TokenHash)
		if err != nil || !ok {
			return domain.Peer{}, false, nil
		}
		if !d.verifyAdvertisement(advert, secret, seenNonces) {
			return domain.Peer{}, false, nil
		}
	} else if d.Token != "" {
		if !wrapped || advert.Signature == "" || advert.TokenHash != tokenHashValue(d.Token) {
			return domain.Peer{}, false, nil
		}
		if !d.verifyAdvertisement(advert, d.Token, seenNonces) {
			return domain.Peer{}, false, nil
		}
	}
	if err := validatePeer(peer); err != nil {
		return domain.Peer{}, false, nil
	}
	return peer, true, nil
}

func (d PeerLANDiscovery) verifyAdvertisement(advert peerAdvertisement, secret string, seenNonces map[string]struct{}) bool {
	if advert.Nonce == "" || advert.ExpiresAt <= d.now().Unix() {
		return false
	}
	if _, seen := seenNonces[advert.Nonce]; seen {
		return false
	}
	payload, err := canonicalAdvertisementPayload(advert.Peer, advert.TokenHash, advert.ExpiresAt, advert.Nonce)
	if err != nil {
		return false
	}
	expected := signAdvertisement(secret, payload)
	if !hmac.Equal([]byte(expected), []byte(advert.Signature)) {
		return false
	}
	seenNonces[advert.Nonce] = struct{}{}
	return true
}

func decodePeerAdvertisement(data []byte) (peerAdvertisement, bool, error) {
	var advert peerAdvertisement
	if err := json.Unmarshal(data, &advert); err == nil && advert.Peer.ID != "" {
		return advert, true, nil
	}
	var peer domain.Peer
	if err := json.Unmarshal(data, &peer); err != nil {
		return peerAdvertisement{}, false, err
	}
	return peerAdvertisement{Peer: peer}, false, nil
}

func tokenHashValue(token string) string {
	if token == "" {
		return ""
	}
	return tokenHash(token)
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

func (d PeerLANDiscovery) now() time.Time {
	if d.Clock != nil {
		return d.Clock.Now().UTC()
	}
	return clock.System{}.Now().UTC()
}

func canonicalAdvertisementPayload(peer domain.Peer, tokenHashValue string, expiresAt int64, nonce string) ([]byte, error) {
	return json.Marshal(signedAdvertisementPayload{
		PeerID:    peer.ID,
		Addresses: append([]string(nil), peer.Addresses...),
		Compute:   peer.Compute,
		Version:   peer.Version,
		TokenHash: tokenHashValue,
		ExpiresAt: expiresAt,
		Nonce:     nonce,
	})
}

func signAdvertisement(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func randomNonce() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
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
