package myce

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Invitation is privately provisioned authority. Neither DNS-SD nor a fetched
// certificate may change its pin. The shared token remains provisional (#32).
type Invitation struct {
	NASIdentity NASIdentity `json:"nas_identity,omitempty"`
	Version     int         `json:"version"`
	Fingerprint string      `json:"fingerprint"`
	Token       string      `json:"token"`
	Address     string      `json:"address,omitempty"`
}

var errJoiningIdentity = errors.New("queue identity does not match invitation")

// ErrLocalNetworkAccess identifies a blocked route without disclosing its address.
var ErrLocalNetworkAccess = errors.New("local network access unavailable")

func (i Invitation) Validate() error {
	pin, err := hex.DecodeString(i.Fingerprint)
	if i.Version != 1 || err != nil || len(pin) != sha256.Size || i.Token == "" {
		return errors.New("invalid private invitation")
	}
	if i.Address != "" {
		return joiningURL(i.Address)
	}
	return nil
}
func joiningURL(address string) error {
	u, err := url.Parse(address)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("joining requires an HTTPS queue origin")
	}
	return nil
}
func ReadInvitation(path string) (Invitation, error) {
	var i Invitation
	info, err := os.Lstat(path)
	if err != nil {
		return i, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return i, errors.New("invitation must be a private regular file (chmod 600)")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return i, err
	}
	if json.Unmarshal(data, &i) != nil {
		return i, errors.New("invalid private invitation")
	}
	return i, i.Validate()
}
func WriteInvitation(path string, i Invitation) error {
	if err := i.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(i)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".myce-invitation-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(file.Name(), path); err != nil {
		return err
	}
	return syncWorkerDir(filepath.Dir(path))
}

func (i Invitation) Client(address string) (Client, error) {
	if err := i.Validate(); err != nil {
		return Client{}, err
	}
	if err := joiningURL(address); err != nil {
		return Client{}, err
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{
		MinVersion: tls.VersionTLS12,
		// A provisioned exact certificate pin replaces public CA/hostname trust.
		InsecureSkipVerify: true,
		VerifyConnection: func(s tls.ConnectionState) error {
			if len(s.PeerCertificates) == 0 {
				return errors.New("queue identity missing")
			}
			cert := s.PeerCertificates[0]
			hash := sha256.Sum256(cert.Raw)
			now := time.Now() // TLS validity, never queue or scheduling time.
			if hex.EncodeToString(hash[:]) != i.Fingerprint || now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
				return errJoiningIdentity
			}
			return nil
		},
	}}
	return Client{URL: address, Token: i.Token, HTTP: &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Discovery finds addresses only; callers authenticate every candidate.
type Discovery interface {
	Browse(context.Context) ([]string, error)
}

func ResolveJoining(ctx context.Context, i Invitation, explicit string, d Discovery) (Client, error) {
	if err := i.Validate(); err != nil {
		return Client{}, err
	}
	if explicit != "" {
		c, err := verifiedJoining(ctx, i, explicit)
		if localNetworkUnavailable(err) {
			return Client{}, ErrLocalNetworkAccess
		}
		return c, err
	}
	addresses, browseErr := d.Browse(ctx)
	failures := []string{}
	if browseErr != nil {
		failures = append(failures, "mDNS browse failed")
	} else if len(addresses) == 0 {
		failures = append(failures, "mDNS browse returned no compatible addresses")
	}
	if i.Address != "" {
		addresses = append(addresses, i.Address)
	}
	seen := map[string]bool{}
	localNetworkBlocked := false
	for _, address := range addresses {
		if seen[address] {
			continue
		}
		seen[address] = true
		// Allow TLS and an older queue's status headers to arrive under load.
		// The incident's healthy status request took 2.55s; no body is drained.
		attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
		c, err := verifiedJoining(attempt, i, address)
		cancel()
		if err == nil {
			return c, nil
		}
		if localNetworkUnavailable(err) && (address == i.Address || i.Address == "") {
			localNetworkBlocked = true
		}
		stage := "connection failed"
		var response *HTTPError
		if errors.Is(err, errJoiningIdentity) {
			stage = "identity verification rejected a candidate"
		} else if errors.As(err, &response) {
			if response.Status == http.StatusUnauthorized || response.Status == http.StatusForbidden {
				stage = "credential rejected"
			} else {
				stage = "queue probe request failed"
			}
		}
		if !seen[stage] {
			failures = append(failures, stage)
			seen[stage] = true
		}
	}
	// Fixed stage labels disclose neither candidate addresses nor credentials.
	if localNetworkBlocked {
		return Client{}, ErrLocalNetworkAccess
	}
	return Client{}, fmt.Errorf("cannot find your mycelium: %s; supply its saved HTTPS address", strings.Join(failures, "; "))
}

func localNetworkUnavailable(err error) bool {
	return errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH)
}
func verifiedJoining(ctx context.Context, i Invitation, address string) (Client, error) {
	c, err := i.Client(address)
	if err != nil {
		return c, err
	}
	err = c.probeJoining(ctx)
	if err != nil {
		c.HTTP.CloseIdleConnections()
	}
	return c, err
}

// probeJoining authenticates via pinned TLS and the normal bearer credential.
// A 404 or 403 permits one attempt at the existing status route during a rolling
// upgrade. The status route must still accept the same credential.
// Neither response body is read: status size must never govern joining latency.
func (c Client) probeJoining(ctx context.Context) error {
	for _, path := range []string{"/api/ping", "/api/status"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.URL, "/")+path, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.Token)
		// Avoid spending queue CPU compressing a body this probe will not read.
		req.Header.Set("Accept-Encoding", "identity")
		res, err := c.HTTP.Do(req)
		if err != nil {
			return err
		}
		_ = res.Body.Close()
		if path == "/api/ping" && (res.StatusCode == http.StatusNotFound || res.StatusCode == http.StatusForbidden) {
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return &HTTPError{Status: res.StatusCode, Message: "queue probe request failed"}
		}
		return nil
	}
	return &HTTPError{Status: http.StatusNotFound, Message: "queue probe request failed"}
}

type JoiningIdentity struct {
	Certificate []byte `json:"certificate"`
	Key         []byte `json:"key"`
}

func (i JoiningIdentity) TLS() (tls.Certificate, error) { return tls.X509KeyPair(i.Certificate, i.Key) }
func (i JoiningIdentity) Fingerprint() string {
	p, _ := pem.Decode(i.Certificate)
	if p == nil {
		return ""
	}
	h := sha256.Sum256(p.Bytes)
	return hex.EncodeToString(h[:])
}
func LoadJoiningIdentity(dir string) (JoiningIdentity, error) {
	if err := PrepareStateRoots(dir); err != nil {
		return JoiningIdentity{}, err
	}
	var i JoiningIdentity
	path := filepath.Join(dir, "identity.json")
	b, err := os.ReadFile(path)
	if err == nil {
		info, e := os.Lstat(path)
		if e != nil {
			return i, e
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return i, errors.New("joining identity must be a private regular file")
		}
		if json.Unmarshal(b, &i) != nil {
			return i, errors.New("invalid joining identity")
		}
		_, err = i.TLS()
		return i, err
	}
	if !errors.Is(err, os.ErrNotExist) {
		return i, err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return i, err
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return i, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return i, err
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Mycelium joining"}, NotBefore: now.Add(-time.Minute), NotAfter: now.AddDate(10, 0, 0), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		return i, err
	}
	rawKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return i, err
	}
	i.Certificate = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	i.Key = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: rawKey})
	// Never replace an identity created concurrently by another queue.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return i, err
	}
	b, err = json.Marshal(i)
	if err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	return i, err
}

// ListenJoining preserves the selected bind in installed settings, or beside
// the durable identity for a foreground queue. Only a saved bind may fall back;
// an explicit command-line address retains ordinary listen error semantics.
func ListenJoining(queueAddress net.Addr, explicit, configPath, identityDir string, listen func(string) (net.Listener, error), warn func(string, ...any)) (net.Listener, error) {
	var config ServiceConfig
	saved := struct {
		Listen string `json:"listen"`
	}{}
	path := filepath.Join(identityDir, "listen.json")
	var err error
	if configPath != "" {
		config, err = ReadServiceConfig(configPath)
		if err != nil {
			return nil, err
		}
		if config.Role != "queue" {
			return nil, errors.New("joining listener settings require a queue config")
		}
		path, saved.Listen = configPath, config.DiscoveryListen
	} else {
		data, e := os.ReadFile(path)
		if e == nil {
			if err := json.Unmarshal(data, &saved); err != nil {
				return nil, err
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return nil, e
		}
	}
	host, _, err := net.SplitHostPort(queueAddress.String())
	if err != nil {
		return nil, err
	}
	automatic := net.JoinHostPort(host, "0")
	bind := explicit
	if bind == "" {
		bind = saved.Listen
	}
	if bind == "" {
		bind = automatic
	}
	listener, err := listen(bind)
	if explicit == "" && saved.Listen != "" && errors.Is(err, syscall.EADDRINUSE) {
		warn("Saved TLS joining port is busy; selecting and saving an automatic port. Saved HTTPS addresses may need repair with myce install worker --config FILE --queue HTTPS_ORIGIN")
		bind = automatic
		listener, err = listen(automatic)
	}
	if err != nil {
		return nil, err
	}
	// Keep an explicitly configured interface/hostname, replacing only port 0.
	host, _, err = net.SplitHostPort(bind)
	if err == nil {
		_, port, e := net.SplitHostPort(listener.Addr().String())
		err = e
		saved.Listen = net.JoinHostPort(host, port)
	}
	var data []byte
	if err == nil {
		if configPath != "" {
			config.DiscoveryListen = saved.Listen
			data, err = json.MarshalIndent(config, "", "  ")
		} else {
			data, err = json.Marshal(saved)
		}
	}
	if err == nil {
		err = writeInstallFile(path, data, 0600, installFiles{}, nil)
	}
	if err != nil {
		listener.Close()
		return nil, fmt.Errorf("save TLS joining listen address: %w", err)
	}
	return listener, nil
}

// ServeJoining shares the existing API, with authenticated transport for discovery.
func ServeJoining(listener net.Listener, handler http.Handler, identity JoiningIdentity) (*http.Server, <-chan error, error) {
	cert, err := identity.TLS()
	if err != nil {
		return nil, nil, err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	errs := make(chan error, 1)
	go func() {
		errs <- server.Serve(tls.NewListener(listener, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}))
	}()
	return server, errs, nil
}
