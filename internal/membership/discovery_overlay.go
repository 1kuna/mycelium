package membership

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync"

	"mycelium/internal/domain"
	"mycelium/internal/ports"

	host "github.com/libp2p/go-libp2p/core/host"
	network "github.com/libp2p/go-libp2p/core/network"
	peer "github.com/libp2p/go-libp2p/core/peer"
	peerstore "github.com/libp2p/go-libp2p/core/peerstore"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	DefaultOverlayDiscoveryProtocol protocol.ID = "/mycelium/discovery/1.0.0"
	DefaultOverlayTunnelProtocol    protocol.ID = "/mycelium/tunnel/1.0.0"
	LabelOverlayPeerID                          = "mycelium.overlay.peer_id"
	LabelOverlayAddrs                           = "mycelium.overlay.addrs"
)

type OverlayDiscovery struct {
	Host     host.Host
	Peers    []peer.AddrInfo
	Protocol protocol.ID

	once  sync.Once
	mu    sync.Mutex
	nodes map[string]domain.Node
}

type overlayDiscoveryMessage struct {
	Type  string        `json:"type"`
	Node  domain.Node   `json:"node,omitempty"`
	Nodes []domain.Node `json:"nodes,omitempty"`
}

func NewOverlayDiscovery(h host.Host, peers ...peer.AddrInfo) *OverlayDiscovery {
	return &OverlayDiscovery{Host: h, Peers: append([]peer.AddrInfo(nil), peers...)}
}

func (d *OverlayDiscovery) Announce(ctx context.Context, node domain.Node) error {
	if err := d.ensure(); err != nil {
		return err
	}
	node = d.withOverlayLabels(node)
	d.remember(node)
	msg := overlayDiscoveryMessage{Type: "announce", Node: node}
	for _, p := range d.Peers {
		resp, err := d.exchange(ctx, p, msg)
		if err != nil {
			return err
		}
		d.rememberAll(resp.Nodes)
	}
	return nil
}

func (d *OverlayDiscovery) Discover(ctx context.Context) ([]domain.Node, error) {
	if err := d.ensure(); err != nil {
		return nil, err
	}
	for _, p := range d.Peers {
		resp, err := d.exchange(ctx, p, overlayDiscoveryMessage{Type: "discover"})
		if err != nil {
			return nil, err
		}
		d.rememberAll(resp.Nodes)
	}
	return d.snapshot(), nil
}

func (d *OverlayDiscovery) ensure() error {
	if d.Host == nil {
		return fmt.Errorf("overlay discovery host is required")
	}
	d.mu.Lock()
	if d.Protocol == "" {
		d.Protocol = DefaultOverlayDiscoveryProtocol
	}
	if d.nodes == nil {
		d.nodes = map[string]domain.Node{}
	}
	protocolID := d.Protocol
	d.mu.Unlock()
	d.once.Do(func() {
		d.Host.SetStreamHandler(protocolID, d.handleStream)
	})
	return nil
}

func (d *OverlayDiscovery) handleStream(s network.Stream) {
	defer s.Close()
	var msg overlayDiscoveryMessage
	if err := json.NewDecoder(s).Decode(&msg); err != nil {
		_ = s.Reset()
		return
	}
	switch msg.Type {
	case "announce":
		d.remember(msg.Node)
		_ = json.NewEncoder(s).Encode(overlayDiscoveryMessage{Type: "nodes", Nodes: d.snapshot()})
	case "discover":
		_ = json.NewEncoder(s).Encode(overlayDiscoveryMessage{Type: "nodes", Nodes: d.snapshot()})
	default:
		_ = s.Reset()
	}
}

func (d *OverlayDiscovery) exchange(ctx context.Context, p peer.AddrInfo, msg overlayDiscoveryMessage) (overlayDiscoveryMessage, error) {
	if p.ID == "" {
		return overlayDiscoveryMessage{}, fmt.Errorf("overlay peer id is required")
	}
	d.Host.Peerstore().AddAddrs(p.ID, p.Addrs, peerstore.PermanentAddrTTL)
	s, err := d.Host.NewStream(ctx, p.ID, d.Protocol)
	if err != nil {
		return overlayDiscoveryMessage{}, err
	}
	defer s.Close()
	if err := json.NewEncoder(s).Encode(msg); err != nil {
		_ = s.Reset()
		return overlayDiscoveryMessage{}, err
	}
	var resp overlayDiscoveryMessage
	if err := json.NewDecoder(s).Decode(&resp); err != nil {
		_ = s.Reset()
		return overlayDiscoveryMessage{}, err
	}
	return resp, nil
}

func (d *OverlayDiscovery) withOverlayLabels(node domain.Node) domain.Node {
	node.Labels = withOverlayLabels(d.Host, node.Labels)
	return node
}

func (d *OverlayDiscovery) remember(node domain.Node) {
	if node.ID == "" {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes[node.ID] = node
}

func (d *OverlayDiscovery) rememberAll(nodes []domain.Node) {
	for _, node := range nodes {
		d.remember(node)
	}
}

func (d *OverlayDiscovery) snapshot() []domain.Node {
	d.mu.Lock()
	defer d.mu.Unlock()
	nodes := make([]domain.Node, 0, len(d.nodes))
	for _, node := range d.nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

type OverlayTunnel struct {
	Host     host.Host
	Protocol protocol.ID

	once      sync.Once
	mu        sync.Mutex
	listeners map[string]net.Listener
}

func NewOverlayTunnel(h host.Host) *OverlayTunnel {
	return &OverlayTunnel{Host: h}
}

func (t *OverlayTunnel) Open(ctx context.Context, node domain.Node) (string, error) {
	if err := t.ensure(); err != nil {
		return "", err
	}
	if node.ID == "" {
		return "", fmt.Errorf("node id is required")
	}
	info, err := overlayPeerInfo(node)
	if err != nil {
		return "", err
	}
	target := node.Address
	if target == "" {
		return "", fmt.Errorf("node address is required")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	t.mu.Lock()
	old := t.listeners[node.ID]
	if t.listeners == nil {
		t.listeners = map[string]net.Listener{}
	}
	t.listeners[node.ID] = listener
	t.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	go t.accept(ctx, listener, info, target)
	return listener.Addr().String(), nil
}

func (t *OverlayTunnel) Close(_ context.Context, nodeID string) error {
	t.mu.Lock()
	listener := t.listeners[nodeID]
	delete(t.listeners, nodeID)
	t.mu.Unlock()
	if listener == nil {
		return nil
	}
	return listener.Close()
}

func (t *OverlayTunnel) ensure() error {
	if t.Host == nil {
		return fmt.Errorf("overlay tunnel host is required")
	}
	t.mu.Lock()
	if t.Protocol == "" {
		t.Protocol = DefaultOverlayTunnelProtocol
	}
	protocolID := t.Protocol
	t.mu.Unlock()
	t.once.Do(func() {
		t.Host.SetStreamHandler(protocolID, t.handleStream)
	})
	return nil
}

func (t *OverlayTunnel) accept(ctx context.Context, listener net.Listener, info peer.AddrInfo, target string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go t.proxy(ctx, conn, info, target)
	}
}

func (t *OverlayTunnel) proxy(ctx context.Context, conn net.Conn, info peer.AddrInfo, target string) {
	defer conn.Close()
	t.Host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	s, err := t.Host.NewStream(ctx, info.ID, t.Protocol)
	if err != nil {
		return
	}
	defer s.Close()
	if _, err := fmt.Fprintln(s, target); err != nil {
		_ = s.Reset()
		return
	}
	copyBoth(conn, s)
}

func (t *OverlayTunnel) handleStream(s network.Stream) {
	reader := bufio.NewReader(s)
	target, err := reader.ReadString('\n')
	if err != nil {
		_ = s.Reset()
		return
	}
	conn, err := net.Dial("tcp", strings.TrimSpace(target))
	if err != nil {
		_ = s.Reset()
		return
	}
	defer conn.Close()
	copyBoth(conn, readWriter{Reader: io.MultiReader(reader, s), Writer: s, Closer: s})
}

type readWriter struct {
	io.Reader
	io.Writer
	io.Closer
}

func copyBoth(a net.Conn, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(b, a)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(a, b)
		done <- struct{}{}
	}()
	<-done
	_ = a.Close()
	_ = b.Close()
}

func withOverlayLabels(h host.Host, labels map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range labels {
		out[key] = value
	}
	out[LabelOverlayPeerID] = h.ID().String()
	addrs := make([]string, 0, len(h.Addrs()))
	p2p, err := ma.NewMultiaddr("/p2p/" + h.ID().String())
	if err == nil {
		for _, addr := range h.Addrs() {
			addrs = append(addrs, addr.Encapsulate(p2p).String())
		}
	}
	sort.Strings(addrs)
	out[LabelOverlayAddrs] = strings.Join(addrs, ",")
	return out
}

func overlayPeerInfo(node domain.Node) (peer.AddrInfo, error) {
	if node.Labels == nil || node.Labels[LabelOverlayPeerID] == "" {
		return peer.AddrInfo{}, fmt.Errorf("node %q missing overlay peer id", node.ID)
	}
	id, err := peer.Decode(node.Labels[LabelOverlayPeerID])
	if err != nil {
		return peer.AddrInfo{}, err
	}
	info := peer.AddrInfo{ID: id}
	for _, raw := range strings.Split(node.Labels[LabelOverlayAddrs], ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(raw)
		if err != nil {
			return peer.AddrInfo{}, err
		}
		addrInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return peer.AddrInfo{}, err
		}
		info.Addrs = append(info.Addrs, addrInfo.Addrs...)
	}
	if len(info.Addrs) == 0 {
		return peer.AddrInfo{}, fmt.Errorf("node %q missing overlay peer addresses", node.ID)
	}
	return info, nil
}

var _ ports.Discovery = (*OverlayDiscovery)(nil)
var _ ports.Tunnel = (*OverlayTunnel)(nil)
