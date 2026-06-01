package membership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"

	"mycelium/internal/domain"
)

const (
	libp2pDiscoveryProtocol protocol.ID = "/mycelium/overlay/discovery/1.0.0"
	libp2pTunnelProtocol    protocol.ID = "/mycelium/overlay/tunnel/1.0.0"
)

type Libp2pOverlayConfig struct {
	ListenAddrs    []string
	BootstrapPeers []string
	LocalTarget    string
	Token          string
	TokenManager   *TokenManager
}

type Libp2pOverlayBackend struct {
	host         host.Host
	localTarget  string
	tokenHash    string
	tokenManager *TokenManager
	bootstrap    []peer.AddrInfo

	mu        sync.Mutex
	peers     map[string]libp2pOverlayAnnouncement
	byPeerID  map[peer.ID]string
	watchers  map[int]chan domain.Peer
	nextWatch int
	tunnels   map[string]*libp2pTunnelEntry
}

type libp2pOverlayAnnouncement struct {
	Peer        domain.Peer `json:"peer"`
	Libp2pID    string      `json:"libp2p_id"`
	Libp2pAddrs []string    `json:"libp2p_addrs"`
	TokenHash   string      `json:"token_hash,omitempty"`
}

type libp2pTunnelEntry struct {
	target   peer.ID
	listener net.Listener
}

func NewLibp2pOverlayBackend(ctx context.Context, cfg Libp2pOverlayConfig) (*Libp2pOverlayBackend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	listen := cfg.ListenAddrs
	if len(listen) == 0 {
		listen = []string{"/ip4/0.0.0.0/tcp/0"}
	}
	h, err := libp2p.New(libp2p.ListenAddrStrings(listen...))
	if err != nil {
		return nil, err
	}
	backend, err := NewLibp2pOverlayBackendWithHost(ctx, h, cfg)
	if err != nil {
		_ = h.Close()
		return nil, err
	}
	return backend, nil
}

func NewLibp2pOverlayBackendWithHost(ctx context.Context, h host.Host, cfg Libp2pOverlayConfig) (*Libp2pOverlayBackend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h == nil {
		return nil, fmt.Errorf("libp2p overlay host is required")
	}
	bootstrap, err := parseBootstrapPeers(cfg.BootstrapPeers)
	if err != nil {
		return nil, err
	}
	backend := &Libp2pOverlayBackend{
		host:         h,
		localTarget:  strings.TrimSpace(cfg.LocalTarget),
		tokenHash:    tokenHashValue(cfg.Token),
		tokenManager: cfg.TokenManager,
		bootstrap:    bootstrap,
		peers:        map[string]libp2pOverlayAnnouncement{},
		byPeerID:     map[peer.ID]string{},
		watchers:     map[int]chan domain.Peer{},
		tunnels:      map[string]*libp2pTunnelEntry{},
	}
	h.SetStreamHandler(libp2pDiscoveryProtocol, backend.handleDiscoveryStream)
	h.SetStreamHandler(libp2pTunnelProtocol, backend.handleTunnelStream)
	return backend, nil
}

func (b *Libp2pOverlayBackend) Advertise(ctx context.Context, self domain.Peer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validatePeer(self); err != nil {
		return err
	}
	announcement, err := b.announcement(self)
	if err != nil {
		return err
	}
	b.mergeAnnouncement(announcement)
	if err := b.connectBootstrap(ctx); err != nil {
		return err
	}
	return b.broadcastAnnouncement(ctx, announcement)
}

func (b *Libp2pOverlayBackend) Peers(ctx context.Context) ([]domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]domain.Peer, 0, len(b.peers))
	for _, announcement := range b.peers {
		out = append(out, clonePeer(announcement.Peer))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (b *Libp2pOverlayBackend) WatchPeers(ctx context.Context) (<-chan domain.Peer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.nextWatch
	b.nextWatch++
	ch := make(chan domain.Peer, len(b.peers)+16)
	ids := make([]string, 0, len(b.peers))
	for id := range b.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		ch <- clonePeer(b.peers[id].Peer)
	}
	b.watchers[id] = ch
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		if watcher, ok := b.watchers[id]; ok {
			delete(b.watchers, id)
			close(watcher)
		}
		b.mu.Unlock()
	}()
	return ch, nil
}

func (b *Libp2pOverlayBackend) Open(ctx context.Context, node domain.Node) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if node.ID == "" {
		return "", fmt.Errorf("node id is required")
	}
	target, err := b.libp2pPeerForNode(node.ID)
	if err != nil {
		return "", err
	}
	b.mu.Lock()
	if old := b.tunnels[node.ID]; old != nil && old.target == target {
		addr := old.listener.Addr().String()
		b.mu.Unlock()
		return addr, nil
	}
	b.mu.Unlock()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	entry := &libp2pTunnelEntry{target: target, listener: listener}
	b.mu.Lock()
	old := b.tunnels[node.ID]
	b.tunnels[node.ID] = entry
	b.mu.Unlock()
	if old != nil {
		_ = old.listener.Close()
	}
	go b.serveTunnel(listener, target)
	return listener.Addr().String(), nil
}

func (b *Libp2pOverlayBackend) Close(ctx context.Context, nodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	entry := b.tunnels[nodeID]
	delete(b.tunnels, nodeID)
	b.mu.Unlock()
	if entry == nil {
		return nil
	}
	return entry.listener.Close()
}

func (b *Libp2pOverlayBackend) CloseHost() error {
	b.mu.Lock()
	tunnels := make([]*libp2pTunnelEntry, 0, len(b.tunnels))
	for nodeID, entry := range b.tunnels {
		tunnels = append(tunnels, entry)
		delete(b.tunnels, nodeID)
	}
	b.mu.Unlock()
	var err error
	for _, entry := range tunnels {
		err = errors.Join(err, entry.listener.Close())
	}
	err = errors.Join(err, b.host.Close())
	return err
}

func (b *Libp2pOverlayBackend) Addrs() []string {
	addrs := make([]string, 0, len(b.host.Addrs()))
	for _, addr := range b.host.Addrs() {
		addrs = append(addrs, addr.Encapsulate(ma.StringCast("/p2p/"+b.host.ID().String())).String())
	}
	sort.Strings(addrs)
	return addrs
}

func (b *Libp2pOverlayBackend) announcement(peer domain.Peer) (libp2pOverlayAnnouncement, error) {
	tokenHash, err := b.currentTokenHash()
	if err != nil {
		return libp2pOverlayAnnouncement{}, err
	}
	return libp2pOverlayAnnouncement{
		Peer:        clonePeer(peer),
		Libp2pID:    b.host.ID().String(),
		Libp2pAddrs: b.Addrs(),
		TokenHash:   tokenHash,
	}, nil
}

func (b *Libp2pOverlayBackend) currentTokenHash() (string, error) {
	if b.tokenManager != nil {
		return b.tokenManager.CurrentHash()
	}
	return b.tokenHash, nil
}

func (b *Libp2pOverlayBackend) connectBootstrap(ctx context.Context) error {
	for _, info := range b.bootstrap {
		if err := b.host.Connect(ctx, info); err != nil {
			return fmt.Errorf("connect libp2p bootstrap %s: %w", info.ID, err)
		}
	}
	return nil
}

func (b *Libp2pOverlayBackend) broadcastAnnouncement(ctx context.Context, announcement libp2pOverlayAnnouncement) error {
	targets := b.knownLibp2pPeers()
	for _, target := range targets {
		if target == b.host.ID() {
			continue
		}
		if err := b.sendAnnouncement(ctx, target, announcement); err != nil {
			return err
		}
	}
	return nil
}

func (b *Libp2pOverlayBackend) knownLibp2pPeers() []peer.ID {
	seen := map[peer.ID]bool{}
	for _, info := range b.bootstrap {
		seen[info.ID] = true
	}
	b.mu.Lock()
	for id := range b.byPeerID {
		seen[id] = true
	}
	b.mu.Unlock()
	out := make([]peer.ID, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func (b *Libp2pOverlayBackend) sendAnnouncement(ctx context.Context, target peer.ID, announcement libp2pOverlayAnnouncement) error {
	stream, err := b.host.NewStream(ctx, target, libp2pDiscoveryProtocol)
	if err != nil {
		return fmt.Errorf("open libp2p discovery stream to %s: %w", target, err)
	}
	defer stream.Close()
	if err := json.NewEncoder(stream).Encode(announcement); err != nil {
		_ = stream.Reset()
		return fmt.Errorf("write libp2p discovery stream to %s: %w", target, err)
	}
	var response []libp2pOverlayAnnouncement
	if err := json.NewDecoder(stream).Decode(&response); err != nil {
		return fmt.Errorf("read libp2p discovery stream from %s: %w", target, err)
	}
	for _, remote := range response {
		b.mergeAnnouncement(remote)
	}
	return nil
}

func (b *Libp2pOverlayBackend) handleDiscoveryStream(stream network.Stream) {
	defer stream.Close()
	var announcement libp2pOverlayAnnouncement
	if err := json.NewDecoder(stream).Decode(&announcement); err != nil {
		_ = stream.Reset()
		return
	}
	if !b.mergeAnnouncement(announcement) {
		_ = stream.Reset()
		return
	}
	if err := json.NewEncoder(stream).Encode(b.announcements()); err != nil {
		_ = stream.Reset()
	}
}

func (b *Libp2pOverlayBackend) handleTunnelStream(stream network.Stream) {
	if b.localTarget == "" {
		_ = stream.Reset()
		return
	}
	conn, err := net.Dial("tcp", b.localTarget)
	if err != nil {
		_ = stream.Reset()
		return
	}
	proxyConnAndStream(conn, stream)
}

func (b *Libp2pOverlayBackend) serveTunnel(listener net.Listener, target peer.ID) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func(conn net.Conn) {
			stream, err := b.host.NewStream(context.Background(), target, libp2pTunnelProtocol)
			if err != nil {
				_ = conn.Close()
				return
			}
			proxyConnAndStream(conn, stream)
		}(conn)
	}
}

func proxyConnAndStream(conn net.Conn, stream network.Stream) {
	var once sync.Once
	closeBoth := func() {
		_ = conn.Close()
		_ = stream.Close()
	}
	go func() {
		_, _ = io.Copy(stream, conn)
		once.Do(closeBoth)
	}()
	go func() {
		_, _ = io.Copy(conn, stream)
		once.Do(closeBoth)
	}()
}

func (b *Libp2pOverlayBackend) announcements() []libp2pOverlayAnnouncement {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]libp2pOverlayAnnouncement, 0, len(b.peers))
	ids := make([]string, 0, len(b.peers))
	for id := range b.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		out = append(out, cloneAnnouncement(b.peers[id]))
	}
	return out
}

func (b *Libp2pOverlayBackend) mergeAnnouncement(announcement libp2pOverlayAnnouncement) bool {
	if announcement.Peer.ID == "" || announcement.Libp2pID == "" {
		return false
	}
	if b.tokenManager != nil {
		if err := b.tokenManager.ValidateHash(announcement.TokenHash); err != nil {
			return false
		}
	} else if b.tokenHash != "" && announcement.TokenHash != b.tokenHash {
		return false
	}
	info, err := addrInfoFromStrings(announcement.Libp2pID, announcement.Libp2pAddrs)
	if err != nil {
		return false
	}
	b.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	b.mu.Lock()
	defer b.mu.Unlock()
	cloned := cloneAnnouncement(announcement)
	b.peers[cloned.Peer.ID] = cloned
	b.byPeerID[info.ID] = cloned.Peer.ID
	for _, watcher := range b.watchers {
		select {
		case watcher <- clonePeer(cloned.Peer):
		default:
		}
	}
	return true
}

func (b *Libp2pOverlayBackend) libp2pPeerForNode(nodeID string) (peer.ID, error) {
	b.mu.Lock()
	announcement, ok := b.peers[nodeID]
	b.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("overlay peer %q is not known", nodeID)
	}
	info, err := addrInfoFromStrings(announcement.Libp2pID, announcement.Libp2pAddrs)
	if err != nil {
		return "", err
	}
	b.host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	return info.ID, nil
}

func parseBootstrapPeers(raw []string) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, 0, len(raw))
	for _, entry := range raw {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(entry)
		if err != nil {
			return nil, fmt.Errorf("parse libp2p bootstrap peer %q: %w", entry, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return nil, fmt.Errorf("parse libp2p bootstrap peer %q: %w", entry, err)
		}
		out = append(out, *info)
	}
	return out, nil
}

func addrInfoFromStrings(id string, addrs []string) (peer.AddrInfo, error) {
	peerID, err := peer.Decode(id)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	info := peer.AddrInfo{ID: peerID}
	for _, raw := range addrs {
		addr, err := ma.NewMultiaddr(raw)
		if err != nil {
			return peer.AddrInfo{}, err
		}
		parsed, err := peer.AddrInfoFromP2pAddr(addr)
		if err == nil {
			if parsed.ID != peerID {
				return peer.AddrInfo{}, fmt.Errorf("libp2p peer address id %s does not match announcement id %s", parsed.ID, peerID)
			}
			info.Addrs = append(info.Addrs, parsed.Addrs...)
			continue
		}
		info.Addrs = append(info.Addrs, addr)
	}
	return info, nil
}

func cloneAnnouncement(announcement libp2pOverlayAnnouncement) libp2pOverlayAnnouncement {
	announcement.Peer = clonePeer(announcement.Peer)
	announcement.Libp2pAddrs = append([]string(nil), announcement.Libp2pAddrs...)
	return announcement
}

var _ OverlayBackend = (*Libp2pOverlayBackend)(nil)
