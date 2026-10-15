package myce

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/grandcat/zeroconf"
)

const discoveryService = "_mycelium._tcp"

type MDNSDiscovery struct{}

func (MDNSDiscovery) Browse(ctx context.Context) ([]string, error) {
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	entries := make(chan *zeroconf.ServiceEntry, 32)
	if err = resolver.Browse(ctx, discoveryService, "local.", entries); err != nil {
		return nil, err
	}
	addresses := []string{}
	for {
		select {
		case <-ctx.Done():
			return addresses, nil
		case entry, ok := <-entries:
			if !ok {
				return addresses, nil
			}
			if len(addresses) >= 32 {
				continue
			}
			compatible := false
			for _, field := range entry.Text {
				if field == "version=1" {
					compatible = true
				}
			}
			if !compatible {
				continue
			}
			for _, ip := range append(entry.AddrIPv4, entry.AddrIPv6...) {
				if ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() {
					continue
				}
				addresses = append(addresses, "https://"+net.JoinHostPort(ip.String(), strconv.Itoa(entry.Port)))
			}
		}
	}
}
func AdvertiseJoining(address net.Addr, fingerprint string) (func(), error) {
	host, port, err := net.SplitHostPort(address.String())
	if err != nil {
		return nil, err
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}
	ips := []string{}
	if ip := net.ParseIP(host); ip != nil && !ip.IsUnspecified() {
		ips = append(ips, host)
	} else {
		interfaces, err := net.InterfaceAddrs()
		if err != nil {
			return nil, err
		}
		for _, a := range interfaces {
			if n, ok := a.(*net.IPNet); ok && !n.IP.IsLoopback() && !n.IP.IsLinkLocalUnicast() {
				ips = append(ips, n.IP.String())
			}
		}
	}
	// Opaque names and protocol hints only; no hostname, credential or profile.
	name := "myce-" + fingerprint[:16]
	server, err := zeroconf.RegisterProxy(name, discoveryService, "local", number, name+".local.", ips, []string{"version=1"}, nil)
	if err != nil {
		return nil, err
	}
	return server.Shutdown, nil
}
