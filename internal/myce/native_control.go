package myce

import (
	"net"
	"net/url"
	"slices"
)

// Deny these ports on every destination, including alternative addresses of a
// local listener and DNS changes. Reserved sidecars must never overlap them.
func (w Worker) nativeControlPorts() []string {
	var ports []string
	if u, err := url.Parse(w.Client.URL); err == nil && u.Host != "" {
		port := u.Port()
		if port == "" {
			port = "80"
			if u.Scheme == "https" {
				port = "443"
			}
		}
		ports = append(ports, port)
	}
	if _, port, err := net.SplitHostPort(w.ModelListen); err == nil && port != "0" && !slices.Contains(ports, port) {
		ports = append(ports, port)
	}
	return ports
}
