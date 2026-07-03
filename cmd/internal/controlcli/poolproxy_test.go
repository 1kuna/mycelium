package controlcli

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/gateway"
)

type proxyGateway struct {
	mu       sync.Mutex
	requests []proxyGatewayRequest
	refuse   map[string]int // workload id -> status to refuse with
}

type proxyGatewayRequest struct {
	path     string
	auth     string
	workload string
	lease    string
	hold     string
	body     string
}

func (g *proxyGateway) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		body, _ := io.ReadAll(r.Body)
		req := proxyGatewayRequest{
			path:     r.URL.Path,
			auth:     r.Header.Get("Authorization"),
			workload: r.Header.Get(gateway.HeaderWorkload),
			lease:    r.Header.Get(gateway.HeaderLease),
			hold:     r.Header.Get(gateway.HeaderLeaseHold),
			body:     string(body),
		}
		g.requests = append(g.requests, req)
		status, refused := g.refuse[req.workload]
		g.mu.Unlock()
		if refused {
			http.Error(w, "lease refused", status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(gateway.HeaderLeaseHold, "true")
		_, _ = w.Write([]byte(`{"ok":true,"workload":"` + req.workload + `"}`))
	})
}

func heldTestWorkers() []poolWorker {
	return []poolWorker{
		{WorkloadID: "w1", LeaseID: "l1", NodeID: "node_a", Status: string(domain.JobRunning)},
		{WorkloadID: "w2", LeaseID: "l2", NodeID: "node_b", Status: string(domain.JobRunning)},
	}
}

func newTestPoolProxy(t *testing.T, g *proxyGateway, bearer string) (*httptest.Server, *httptest.Server) {
	t.Helper()
	backend := httptest.NewServer(g.handler())
	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend url: %v", err)
	}
	proxy := httptest.NewServer(newPoolProxyHandler(heldTestWorkers(), target, bearer, backend.Client()))
	return proxy, backend
}

func TestPoolProxyInjectsLeaseHeadersAndRotatesWorkers(t *testing.T) {
	g := &proxyGateway{}
	proxy, backend := newTestPoolProxy(t, g, "gw-secret")
	defer proxy.Close()
	defer backend.Close()

	for i := 0; i < 4; i++ {
		resp, err := http.Post(proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"chandra"}`))
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"ok":true`) {
			t.Fatalf("post %d: status=%d body=%s", i, resp.StatusCode, body)
		}
	}
	if len(g.requests) != 4 {
		t.Fatalf("gateway requests = %d", len(g.requests))
	}
	seen := map[string]int{}
	for _, req := range g.requests {
		seen[req.workload]++
		if req.auth != "Bearer gw-secret" {
			t.Fatalf("auth = %q", req.auth)
		}
		if req.hold != "true" {
			t.Fatalf("hold header = %q", req.hold)
		}
		if req.path != "/v1/chat/completions" {
			t.Fatalf("path = %q", req.path)
		}
		if (req.workload == "w1" && req.lease != "l1") || (req.workload == "w2" && req.lease != "l2") {
			t.Fatalf("workload/lease pairing broken: %+v", req)
		}
		if req.body != `{"model":"chandra"}` {
			t.Fatalf("body = %q", req.body)
		}
	}
	if seen["w1"] != 2 || seen["w2"] != 2 {
		t.Fatalf("round robin distribution = %+v", seen)
	}
}

func TestPoolProxyFailsOverToNextWorkerOnLeaseRefusal(t *testing.T) {
	g := &proxyGateway{refuse: map[string]int{"w1": http.StatusConflict}}
	proxy, backend := newTestPoolProxy(t, g, "")
	defer proxy.Close()
	defer backend.Close()

	resp, err := http.Post(proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"workload":"w2"`) {
		t.Fatalf("failover response status=%d body=%s", resp.StatusCode, body)
	}

	// When every worker refuses, the last refusal is returned loudly.
	g.mu.Lock()
	g.refuse["w2"] = http.StatusNotFound
	g.mu.Unlock()
	resp, err = http.Post(proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("post all-refused: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict && resp.StatusCode != http.StatusNotFound {
		t.Fatalf("all-refused status = %d body=%s", resp.StatusCode, body)
	}
}

func TestPoolProxyStripsCallerLeaseHeadersAndKeepsOthers(t *testing.T) {
	g := &proxyGateway{}
	proxy, backend := newTestPoolProxy(t, g, "")
	defer proxy.Close()
	defer backend.Close()

	req, err := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(gateway.HeaderWorkload, "spoofed")
	req.Header.Set(gateway.HeaderLease, "spoofed")
	req.Header.Set("X-Custom", "kept")
	req.Header.Set("Authorization", "Bearer consumer-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()
	got := g.requests[0]
	if got.workload == "spoofed" || got.lease == "spoofed" {
		t.Fatalf("caller lease headers must be overridden: %+v", got)
	}
	// Without a configured gateway token the consumer's own auth passes through.
	if got.auth != "Bearer consumer-key" {
		t.Fatalf("auth = %q", got.auth)
	}
}

func TestResolvePoolProxyToken(t *testing.T) {
	if tok, err := resolvePoolProxyToken("direct", "", ""); err != nil || tok != "direct" {
		t.Fatalf("direct token: %q %v", tok, err)
	}
	t.Setenv("POOL_PROXY_TOKEN", " from-env ")
	if tok, err := resolvePoolProxyToken("", "POOL_PROXY_TOKEN", ""); err != nil || tok != "from-env" {
		t.Fatalf("env token: %q %v", tok, err)
	}
	t.Setenv("POOL_PROXY_TOKEN", "")
	if _, err := resolvePoolProxyToken("", "POOL_PROXY_TOKEN", ""); err == nil {
		t.Fatalf("empty env must fail")
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "token")
	if err := os.WriteFile(file, []byte("from-file\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	if tok, err := resolvePoolProxyToken("", "", file); err != nil || tok != "from-file" {
		t.Fatalf("file token: %q %v", tok, err)
	}
	if _, err := resolvePoolProxyToken("", "", filepath.Join(dir, "missing")); err == nil {
		t.Fatalf("missing file must fail")
	}
	if tok, err := resolvePoolProxyToken("", "", ""); err != nil || tok != "" {
		t.Fatalf("no token config: %q %v", tok, err)
	}
}

func TestRunPoolProxyValidation(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pool.json")
	noHeld := poolState{Name: "ocr", Model: "chandra", Workers: []poolWorker{
		{WorkloadID: "w1", NodeID: "node_a", Status: string(domain.JobFailed)},
	}}
	if err := writePrettyJSONFile(statePath, noHeld); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	cases := [][]string{
		{"pool", "proxy"},
		{"pool", "proxy", "--state", filepath.Join(dir, "missing.json"), "--url", "http://x"},
		{"pool", "proxy", "--state", statePath, "--url", "http://x"},  // no held workers
		{"pool", "proxy", "--state", statePath, "--url", "not a url"}, // bad url
		{"pool", "proxy", "--state", statePath, "--url", "http://x", "--gateway-token-file", filepath.Join(dir, "missing-token")},
	}
	for _, args := range cases {
		if err := RunWithClient(context.Background(), args, http.DefaultClient); err == nil {
			t.Fatalf("expected error for %v", args)
		}
	}
}

func TestServePoolProxyShutsDownOnContextCancel(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- servePoolProxy(ctx, ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
	}()
	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	if err != nil {
		t.Fatalf("probe proxy: %v", err)
	}
	resp.Body.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("servePoolProxy returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("servePoolProxy did not shut down after cancel")
	}
}
