package myce

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// #218/#221: the API may use HTTP/2 and a provisioned TLS authority. File
// transfers retain that authority and use a separate connection. Custom TLS
// dialers own ALPN; ordinary transfers must still negotiate HTTP/1.1 (#221).
func TestFileTransferHTTP2AuthorityComposition(t *testing.T) {
	for _, mode := range []string{"transport", "tls-context", "tls-legacy", "tls-context-h2", "tls-legacy-h2"} {
		t.Run(mode, func(t *testing.T) {
			body := []byte("verified shared input")
			var requests, checks, dials atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				wantProtocol := 1
				if mode == "tls-context-h2" || mode == "tls-legacy-h2" {
					wantProtocol = 2
				}
				if r.ProtoMajor != wantProtocol {
					t.Errorf("file transfer negotiated %s", r.Proto)
				}
				if r.Header.Get("Authorization") != "Bearer fixture" {
					t.Error("missing file authority")
				}
				if r.Method == "PATCH" {
					got, err := io.ReadAll(r.Body)
					if err != nil || !bytes.Equal(got, body) {
						t.Errorf("streaming upload: %q, %v", got, err)
					}
				}
				w.Write(body)
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			transport := server.Client().Transport.(*http.Transport).Clone()
			defer transport.CloseIdleConnections()
			transport.ForceAttemptHTTP2 = true
			transport.TLSClientConfig.NextProtos = []string{"h2", "http/1.1"}
			transport.TLSClientConfig.VerifyConnection = func(tls.ConnectionState) error {
				checks.Add(1)
				return nil
			}
			if mode != "transport" {
				// A custom dialer owns its protocol and pin. Its internal TLS
				// configuration is intentionally separate from the API transport.
				config := transport.TLSClientConfig.Clone()
				if mode == "tls-context" || mode == "tls-legacy" {
					config.NextProtos = []string{"http/1.1"}
				}
				dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
					dials.Add(1)
					return (&tls.Dialer{Config: config}).DialContext(ctx, network, addr)
				}
				if mode == "tls-context" || mode == "tls-context-h2" {
					transport.DialTLSContext = dial
				} else {
					transport.DialTLS = func(network, addr string) (net.Conn, error) {
						return dial(context.Background(), network, addr)
					}
				}
			}
			c := Client{URL: server.URL, Token: "fixture", HTTP: &http.Client{Transport: transport}}
			res, err := c.fileRequest(context.Background(), "GET", "/input", nil, nil, int64(len(body)))
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || string(got) != string(body) || requests.Load() != 1 || checks.Load() != 1 {
				t.Fatalf("file transfer: body=%q err=%v requests=%d authority checks=%d", got, err, requests.Load(), checks.Load())
			}
			if mode != "transport" && dials.Load() != 1 {
				t.Fatal("custom TLS authority dialer was bypassed")
			}
			res, err = c.fileRequest(context.Background(), "PATCH", "/upload", io.LimitReader(bytes.NewReader(body), int64(len(body))), nil, int64(len(body)))
			if err != nil {
				t.Fatal(err)
			}
			got, err = io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || !bytes.Equal(got, body) || requests.Load() != 2 || checks.Load() != 2 {
				t.Fatalf("upload: body=%q err=%v requests=%d authority checks=%d", got, err, requests.Load(), checks.Load())
			}
			if transport.TLSClientConfig.NextProtos[0] != "h2" {
				t.Fatal("file request changed API transport TLS configuration")
			}
		})
	}
}

func TestFileTransferLegacyDialRefusal(t *testing.T) {
	var requests, dials atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	transport := &http.Transport{Dial: func(string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, os.ErrPermission
	}}
	c := Client{URL: server.URL, Token: "fixture", HTTP: &http.Client{Transport: transport}}
	if _, err := c.fileRequest(context.Background(), "HEAD", "/input", nil, nil); err == nil || dials.Load() != 1 || requests.Load() != 0 {
		t.Fatal("legacy TCP refusal was bypassed", err, dials.Load(), requests.Load())
	}
}

func TestFileTransferCustomTLSSetupStalls(t *testing.T) {
	for _, mode := range []string{"context", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			done := make(chan struct{})
			dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
				defer close(done)
				conn := tls.Client(client, &tls.Config{ServerName: "fixture.invalid"})
				if err := conn.HandshakeContext(ctx); err != nil {
					conn.Close()
					return nil, err
				}
				return conn, nil
			}
			transport := &http.Transport{}
			if mode == "context" {
				transport.DialTLSContext = dial
			} else {
				transport.DialTLS = func(network, addr string) (net.Conn, error) {
					return dial(context.Background(), network, addr)
				}
			}
			c := Client{URL: "https://fixture.invalid", Token: "fixture", HTTP: &http.Client{Transport: transport}, transferIdle: 100 * time.Millisecond}
			result := make(chan error, 1)
			go func() {
				_, err := c.fileRequest(context.Background(), "HEAD", "/input", nil, nil)
				result <- err
			}()
			select {
			case err := <-result:
				if !errors.Is(err, errFileTransferStalled) {
					t.Fatal("TLS setup did not fail typed", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("custom TLS setup was unbounded")
			}
			peer.Close() // Release the uncancellable legacy hook and its late result.
			<-done
		})
	}
}

// Verify that rejecting a custom TLS authority cannot fall back to the API
// transport's otherwise-trusted certificate and disclose a bearer token.
func TestFileTransferCustomTLSRefusal(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()
	transport := server.Client().Transport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	transport.DialTLSContext = func(context.Context, string, string) (net.Conn, error) {
		return nil, os.ErrPermission
	}
	c := Client{URL: server.URL, Token: "fixture", HTTP: &http.Client{Transport: transport}}
	ref := FileRef{SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SizeBytes: 1}
	if err := c.DownloadFile(context.Background(), ref, filepath.Join(t.TempDir(), "input")); err == nil || requests.Load() != 0 {
		t.Fatal("custom TLS refusal was bypassed", err, requests.Load())
	}
}
