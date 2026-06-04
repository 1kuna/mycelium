package membership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/mocks"
)

func TestPeerLANDiscoveryAdvertisesPeer(t *testing.T) {
	network := newPacketNetwork()
	conn, err := network.ListenPacket(context.Background(), "udp4", "127.0.0.1:61001", false)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer conn.Close()
	peer := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:51846"}, Compute: true}

	if err := (PeerLANDiscovery{BroadcastAddr: "127.0.0.1:61001", PacketFactory: network}).Advertise(context.Background(), peer); err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	buf := make([]byte, 4096)
	n, _, err := conn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	var got domain.Peer
	if err := json.Unmarshal(buf[:n], &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != peer.ID || got.Addresses[0] != peer.Addresses[0] || !got.Compute {
		t.Fatalf("got = %+v", got)
	}
}

func TestPeerLANDiscoveryPeersAndWatch(t *testing.T) {
	network := newPacketNetwork()
	addr := "127.0.0.1:61002"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	discovery := PeerLANDiscovery{ListenAddr: addr, BroadcastAddr: addr, MaxPackets: 2, PacketFactory: network}
	peerA := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	peerB := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}}
	if err := discovery.Advertise(ctx, peerA); err != nil {
		t.Fatalf("Advertise A: %v", err)
	}
	if err := discovery.Advertise(ctx, peerB); err != nil {
		t.Fatalf("Advertise B: %v", err)
	}
	peers, err := discovery.Peers(ctx)
	assertPeerList(t, peers, err, peerA.ID, peerB.ID)

	watchAddr := "127.0.0.1:61003"
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	watchDiscovery := PeerLANDiscovery{ListenAddr: watchAddr, BroadcastAddr: watchAddr, PacketFactory: network}
	ch, err := watchDiscovery.WatchPeers(watchCtx)
	if err != nil {
		t.Fatalf("WatchPeers: %v", err)
	}
	peerC := domain.Peer{ID: "peer-c", Addresses: []string{"127.0.0.1:3"}}
	if err := watchDiscovery.Advertise(watchCtx, peerC); err != nil {
		t.Fatalf("watch Advertise: %v", err)
	}
	got := readPeer(t, ch)
	if got.ID != peerC.ID {
		t.Fatalf("watch peer = %+v", got)
	}
	watchCancel()
	if _, ok := <-ch; ok {
		t.Fatal("watch channel stayed open after cancel")
	}
}

func TestPeerLANDiscoveryFiltersByJoinToken(t *testing.T) {
	network := newPacketNetwork()
	addr := "127.0.0.1:61004"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener := PeerLANDiscovery{ListenAddr: addr, BroadcastAddr: addr, MaxPackets: 1, Token: "secret", PacketFactory: network}
	matching := PeerLANDiscovery{BroadcastAddr: addr, Token: "secret", PacketFactory: network}
	other := PeerLANDiscovery{BroadcastAddr: addr, Token: "other", PacketFactory: network}
	peerA := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	peerB := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}, Compute: true}
	if err := other.Advertise(ctx, peerB); err != nil {
		t.Fatalf("Advertise other: %v", err)
	}
	if err := matching.Advertise(ctx, peerA); err != nil {
		t.Fatalf("Advertise matching: %v", err)
	}
	peers, err := listener.Peers(ctx)
	assertPeerList(t, peers, err, peerA.ID)
}

func TestPeerLANDiscoveryRejectsTamperedExpiredAndReplayedSignedAdverts(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC))
	discovery := PeerLANDiscovery{Token: "secret", Clock: clock, AdvertTTL: time.Minute}
	peer := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true, Version: "test"}
	data, err := discovery.marshal(peer)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var advert peerAdvertisement
	if err := json.Unmarshal(data, &advert); err != nil {
		t.Fatalf("unmarshal advert: %v", err)
	}
	if advert.TokenHash == "" || advert.Nonce == "" || advert.Signature == "" || advert.ExpiresAt <= clock.Now().Unix() {
		t.Fatalf("advert = %+v", advert)
	}

	tampered := advert
	tampered.Peer.Addresses = []string{"127.0.0.1:9"}
	tamperedData, err := json.Marshal(tampered)
	if err != nil {
		t.Fatalf("marshal tampered: %v", err)
	}
	seen := map[string]struct{}{}
	conn := &fakePacketConn{readCh: make(chan packet, 4), closed: make(chan struct{})}
	conn.readCh <- packet{data: tamperedData, addr: packetAddr("sender")}
	if _, accepted, err := discovery.readPeer(context.Background(), conn, seen); err != nil || accepted {
		t.Fatalf("tampered accepted=%t err=%v", accepted, err)
	}

	conn.readCh <- packet{data: data, addr: packetAddr("sender")}
	got, accepted, err := discovery.readPeer(context.Background(), conn, seen)
	if err != nil || !accepted || got.ID != peer.ID {
		t.Fatalf("signed accepted=%t peer=%+v err=%v", accepted, got, err)
	}
	conn.readCh <- packet{data: data, addr: packetAddr("sender")}
	if _, accepted, err := discovery.readPeer(context.Background(), conn, seen); err != nil || accepted {
		t.Fatalf("replay accepted=%t err=%v", accepted, err)
	}

	expired := advert
	expired.ExpiresAt = clock.Now().Add(-time.Second).Unix()
	expiredData, err := json.Marshal(expired)
	if err != nil {
		t.Fatalf("marshal expired: %v", err)
	}
	conn.readCh <- packet{data: expiredData, addr: packetAddr("sender")}
	if _, accepted, err := discovery.readPeer(context.Background(), conn, map[string]struct{}{}); err != nil || accepted {
		t.Fatalf("expired accepted=%t err=%v", accepted, err)
	}
}

func TestPeerLANDiscoveryFiltersWithPersistentTokenManager(t *testing.T) {
	network := newPacketNetwork()
	addr := "127.0.0.1:61005"
	manager, err := NewTokenManager("secret")
	if err != nil {
		t.Fatalf("NewTokenManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener := PeerLANDiscovery{ListenAddr: addr, BroadcastAddr: addr, MaxPackets: 1, TokenManager: manager, PacketFactory: network}
	matching := PeerLANDiscovery{BroadcastAddr: addr, TokenManager: manager, PacketFactory: network}
	other := PeerLANDiscovery{BroadcastAddr: addr, Token: "other", PacketFactory: network}
	peerA := domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}, Compute: true}
	peerB := domain.Peer{ID: "peer-b", Addresses: []string{"127.0.0.1:2"}, Compute: true}
	if err := other.Advertise(ctx, peerB); err != nil {
		t.Fatalf("Advertise other: %v", err)
	}
	if err := matching.Advertise(ctx, peerA); err != nil {
		t.Fatalf("Advertise matching: %v", err)
	}
	peers, err := listener.Peers(ctx)
	assertPeerList(t, peers, err, peerA.ID)
	if err := manager.Revoke("secret"); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if err := matching.Advertise(context.Background(), peerA); err == nil {
		t.Fatal("advertise with revoked current token succeeded")
	}
}

func TestPeerLANDiscoveryValidationAndTimeout(t *testing.T) {
	defaulted := NewPeerLANDiscovery("", "")
	if defaulted.ListenAddr != ":51850" || defaulted.BroadcastAddr != DefaultPeerDiscoveryAddr || defaulted.MaxPackets != 16 || defaulted.ScanDuration == 0 {
		t.Fatalf("defaulted = %+v", defaulted)
	}
	if err := (PeerLANDiscovery{BroadcastAddr: "127.0.0.1:1"}).Advertise(context.Background(), domain.Peer{}); err == nil || !strings.Contains(err.Error(), "peer id") {
		t.Fatalf("missing id err = %v", err)
	}
	if err := (PeerLANDiscovery{BroadcastAddr: "127.0.0.1:1"}).Advertise(context.Background(), domain.Peer{ID: "peer-a"}); err == nil || !strings.Contains(err.Error(), "reachable address") {
		t.Fatalf("missing address err = %v", err)
	}
	if err := (PeerLANDiscovery{BroadcastAddr: "%"}).Advertise(context.Background(), domain.Peer{ID: "peer-a", Addresses: []string{"127.0.0.1:1"}}); err == nil {
		t.Fatal("bad broadcast address accepted")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (PeerLANDiscovery{}).WatchPeers(canceled); err == nil {
		t.Fatal("canceled WatchPeers succeeded")
	}
	timeoutFactory := packetFactoryFunc(func(context.Context, string, string, bool) (net.PacketConn, error) {
		return &fakePacketConn{addr: packetAddr("timeout"), readErr: timeoutErr{}}, nil
	})
	peers, err := (PeerLANDiscovery{ListenAddr: "timeout", MaxPackets: 1, PacketFactory: timeoutFactory}).Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers timeout: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("peers = %+v", peers)
	}
	badFactory := packetFactoryFunc(func(context.Context, string, string, bool) (net.PacketConn, error) {
		return nil, errors.New("listen failed")
	})
	if _, err := (PeerLANDiscovery{ListenAddr: "bad", PacketFactory: badFactory}).Peers(context.Background()); err == nil {
		t.Fatal("bad listen accepted")
	}
	if _, err := (PeerLANDiscovery{ListenAddr: "bad", PacketFactory: badFactory}).WatchPeers(context.Background()); err == nil {
		t.Fatal("bad watch listen accepted")
	}
	if peers, err := (PeerLANDiscovery{ListenAddr: "deadline", MaxPackets: 1, ScanDuration: time.Millisecond, Clock: mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)), PacketFactory: timeoutFactory}).Peers(context.Background()); err != nil || len(peers) != 0 {
		t.Fatalf("Peers default deadline = %+v %v", peers, err)
	}
}

func TestPeerLANDiscoveryIgnoresMalformedPackets(t *testing.T) {
	network := newPacketNetwork()
	addr := "127.0.0.1:61006"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	discovery := PeerLANDiscovery{ListenAddr: addr, MaxPackets: 1, PacketFactory: network}
	results := make(chan struct {
		peers []domain.Peer
		err   error
	}, 1)
	go func() {
		peers, err := discovery.Peers(ctx)
		results <- struct {
			peers []domain.Peer
			err   error
		}{peers: peers, err: err}
	}()
	network.send(addr, []byte(`{"addresses":["127.0.0.1:1"]}`))
	network.send(addr, []byte(`{`))
	network.send(addr, []byte(`{"id":"peer-a","addresses":["127.0.0.1:1"]}`))
	result := readPeerResult(t, results)
	assertPeerList(t, result.peers, result.err, "peer-a")
}

func readPeerResult(t *testing.T, results <-chan struct {
	peers []domain.Peer
	err   error
}) struct {
	peers []domain.Peer
	err   error
} {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("peer result was not ready")
	}
	return struct {
		peers []domain.Peer
		err   error
	}{}
}

func TestPeerLANDiscoveryHelperBranches(t *testing.T) {
	if _, ok := (PeerLANDiscovery{}).packetFactory().(netPacketFactory); !ok {
		t.Fatal("default packet factory is not net-backed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !peerReadDone(ctx, errors.New("boom")) {
		t.Fatal("canceled context did not finish peer read")
	}
	if peerReadDone(context.Background(), errors.New("boom")) {
		t.Fatal("ordinary read error ended peer scan")
	}
}

func assertPeerList(t *testing.T, peers []domain.Peer, err error, ids ...string) {
	t.Helper()
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != len(ids) {
		t.Fatalf("peers = %+v", peers)
	}
	seen := map[string]bool{}
	for _, peer := range peers {
		seen[peer.ID] = true
	}
	for _, id := range ids {
		if !seen[id] {
			t.Fatalf("peers = %+v", peers)
		}
	}
}

func readPeer(t *testing.T, ch <-chan domain.Peer) domain.Peer {
	t.Helper()
	for i := 0; i < 1000; i++ {
		select {
		case peer, ok := <-ch:
			if !ok {
				t.Fatal("peer channel closed")
			}
			return peer
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("peer was not ready")
	return domain.Peer{}
}

func readErr(t *testing.T, errs <-chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("error was not ready")
	}
	return nil
}

type packetFactoryFunc func(context.Context, string, string, bool) (net.PacketConn, error)

func (f packetFactoryFunc) ListenPacket(ctx context.Context, network, address string, broadcast bool) (net.PacketConn, error) {
	return f(ctx, network, address, broadcast)
}

type packetNetwork struct {
	mu      sync.Mutex
	next    int
	conns   map[string]*fakePacketConn
	pending map[string][]packet
}

func newPacketNetwork() *packetNetwork {
	return &packetNetwork{conns: map[string]*fakePacketConn{}, pending: map[string][]packet{}}
}

func (n *packetNetwork) ListenPacket(_ context.Context, _, address string, _ bool) (net.PacketConn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if strings.HasSuffix(address, ":0") || address == "0.0.0.0:0" {
		n.next++
		address = fmt.Sprintf("ephemeral-%d", n.next)
	}
	conn := &fakePacketConn{network: n, addr: packetAddr(address), readCh: make(chan packet, 16), closed: make(chan struct{})}
	n.conns[address] = conn
	for _, packet := range n.pending[address] {
		conn.readCh <- packet
	}
	delete(n.pending, address)
	return conn, nil
}

func (n *packetNetwork) send(address string, data []byte) {
	n.mu.Lock()
	conn := n.conns[address]
	if conn == nil {
		n.pending[address] = append(n.pending[address], packet{data: append([]byte(nil), data...), addr: packetAddr("sender")})
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()
	conn.readCh <- packet{data: append([]byte(nil), data...), addr: packetAddr("sender")}
}

func (n *packetNetwork) close(address string) {
	n.mu.Lock()
	delete(n.conns, address)
	n.mu.Unlock()
}

type packet struct {
	data []byte
	addr net.Addr
}

type fakePacketConn struct {
	network *packetNetwork
	addr    net.Addr
	readCh  chan packet
	closed  chan struct{}
	once    sync.Once
	readErr error
}

func (c *fakePacketConn) ReadFrom(buf []byte) (int, net.Addr, error) {
	if c.readErr != nil {
		return 0, nil, c.readErr
	}
	select {
	case packet := <-c.readCh:
		return copy(buf, packet.data), packet.addr, nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}

func (c *fakePacketConn) WriteTo(data []byte, addr net.Addr) (int, error) {
	if c.network == nil {
		return 0, net.ErrClosed
	}
	c.network.send(addr.String(), data)
	return len(data), nil
}

func (c *fakePacketConn) Close() error {
	c.once.Do(func() {
		if c.closed != nil {
			close(c.closed)
		}
		if c.network != nil {
			c.network.close(c.addr.String())
		}
	})
	return nil
}

func (c *fakePacketConn) LocalAddr() net.Addr {
	return c.addr
}

func (c *fakePacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *fakePacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *fakePacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

type packetAddr string

func (a packetAddr) Network() string { return "udp" }
func (a packetAddr) String() string  { return string(a) }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
