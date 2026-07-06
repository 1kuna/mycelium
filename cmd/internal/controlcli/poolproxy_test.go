package controlcli

import (
	"context"
	"encoding/json"
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
	"mycelium/internal/workload"
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
		{WorkloadID: "w1", LeaseID: "l1", InstanceID: "inst-w1", NodeID: "node_a", Status: string(domain.JobRunning)},
		{WorkloadID: "w2", LeaseID: "l2", InstanceID: "inst-w2", NodeID: "node_b", Status: string(domain.JobRunning)},
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

func TestPreflightPoolProxyGatewayAuth(t *testing.T) {
	cases := []struct {
		name            string
		bearer          string
		poolProject     string
		status          int
		reportedProject string
		wantErr         string
	}{
		{
			name:        "missing token",
			poolProject: "project-a",
			status:      http.StatusOK,
			wantErr:     "requires a gateway token",
		},
		{
			name:        "invalid token",
			bearer:      "bad-token",
			poolProject: "project-a",
			status:      http.StatusUnauthorized,
			wantErr:     "returned 401",
		},
		{
			name:        "missing project echo",
			bearer:      "valid-token",
			poolProject: "project-a",
			status:      http.StatusOK,
			wantErr:     "did not report X-Myc-Project",
		},
		{
			name:            "project mismatch",
			bearer:          "valid-token",
			poolProject:     "project-a",
			status:          http.StatusOK,
			reportedProject: "project-b",
			wantErr:         "project mismatch",
		},
		{
			name:            "valid project token",
			bearer:          "valid-token",
			poolProject:     "project-a",
			status:          http.StatusOK,
			reportedProject: "project-a",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/models" {
					t.Fatalf("path = %s", r.URL.Path)
				}
				if tc.bearer != "" && r.Header.Get("Authorization") != "Bearer "+tc.bearer {
					t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
				}
				if tc.reportedProject != "" {
					w.Header().Set(gateway.HeaderProject, tc.reportedProject)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
			}))
			defer srv.Close()
			target, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("parse target: %v", err)
			}
			err = preflightPoolProxyGateway(context.Background(), srv.Client(), target, tc.bearer, tc.poolProject)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("preflight returned %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("preflight error = %v want %q", err, tc.wantErr)
			}
		})
	}
}

func TestPoolProxyModelsURL(t *testing.T) {
	cases := map[string]string{
		"http://gateway":             "http://gateway/v1/models",
		"http://gateway/":            "http://gateway/v1/models",
		"http://gateway/v1":          "http://gateway/v1/models",
		"http://gateway/custom/v1":   "http://gateway/custom/v1/models",
		"http://gateway/custom/base": "http://gateway/custom/base/v1/models",
	}
	for raw, want := range cases {
		target, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if got := poolProxyModelsURL(target); got != want {
			t.Fatalf("models url for %s = %s want %s", raw, got, want)
		}
	}
}

func TestPoolProxyPeerURLStripsV1Suffix(t *testing.T) {
	cases := map[string]string{
		"http://gateway":             "http://gateway",
		"http://gateway/":            "http://gateway",
		"http://gateway/v1":          "http://gateway",
		"http://gateway/custom/v1":   "http://gateway/custom",
		"http://gateway/custom/base": "http://gateway/custom/base",
	}
	for raw, want := range cases {
		target, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if got := poolProxyPeerURL(target); got != want {
			t.Fatalf("peer url for %s = %s want %s", raw, got, want)
		}
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

	heldPath := filepath.Join(dir, "held.json")
	held := poolState{Name: "ocr", Model: "chandra", Project: "project-a", Workers: []poolWorker{
		{WorkloadID: "w1", NodeID: "node_a", Status: string(domain.JobRunning), InstanceID: "inst-1", LeaseID: "lease-1"},
	}}
	if err := writePrettyJSONFile(heldPath, held); err != nil {
		t.Fatalf("seed held state: %v", err)
	}
	if err := RunWithClient(context.Background(), []string{"pool", "proxy", "--state", heldPath, "--url", "http://x"}, http.DefaultClient); err == nil || !strings.Contains(err.Error(), "requires an RPC token") {
		t.Fatalf("missing rpc token error = %v", err)
	}
	if err := RunWithClient(context.Background(), []string{"pool", "proxy", "--state", heldPath, "--url", "http://x", "--gateway-token", "gw"}, http.DefaultClient); err == nil || !strings.Contains(err.Error(), "requires an RPC token") {
		t.Fatalf("missing rpc token error = %v", err)
	}
}

func TestPoolProxyDropsWorkersThatFailLiveLeaseValidation(t *testing.T) {
	g := &proxyGateway{}
	backend := httptest.NewServer(g.handler())
	defer backend.Close()
	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend url: %v", err)
	}
	workers := []poolWorker{
		{WorkloadID: "stale", LeaseID: "lease-stale", InstanceID: "inst-stale", NodeID: "node_a", Status: string(domain.JobRunning)},
		{WorkloadID: "fresh", LeaseID: "lease-fresh", InstanceID: "inst-fresh", NodeID: "node_b", Status: string(domain.JobRunning)},
	}
	handler := newPoolProxyHandler(workers, target, "gw-secret", backend.Client())
	handler.validator = func(ctx context.Context, worker *poolWorker) error {
		if worker.WorkloadID == "stale" {
			markPoolWorkerLeaseInactive(worker, "instance has no active admission lease")
		}
		return nil
	}
	proxy := httptest.NewServer(handler)
	defer proxy.Close()

	resp, err := http.Post(proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"chandra"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"workload":"fresh"`) {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if len(g.requests) != 1 || g.requests[0].workload != "fresh" {
		t.Fatalf("proxy forwarded stale worker: %+v", g.requests)
	}

	handler.validator = func(ctx context.Context, worker *poolWorker) error {
		markPoolWorkerLeaseInactive(worker, "lease inactive")
		return nil
	}
	resp, err = http.Post(proxy.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"chandra"}`))
	if err != nil {
		t.Fatalf("post after stale: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), "no gateway-usable held workers") {
		t.Fatalf("all stale status=%d body=%s", resp.StatusCode, body)
	}
}

func TestRefreshPoolProxyWorkersRequiresGatewayUsableLease(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/workloads/fresh", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(workload.StatusResponse{
			Job: domain.Job{ID: "fresh", Status: domain.JobRunning},
			Result: &domain.JobResult{
				LeaseID:  "lease-fresh",
				Instance: domain.ModelInstance{ID: "inst-fresh"},
				NodeID:   "node_b",
			},
			Execution: workload.ExecutionEvidence{LeaseActive: true},
		})
	})
	mux.HandleFunc("/workloads/stale", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(workload.StatusResponse{
			Job: domain.Job{ID: "stale", Status: domain.JobRunning},
			Result: &domain.JobResult{
				LeaseID:  "lease-stale",
				Instance: domain.ModelInstance{ID: "inst-stale"},
				NodeID:   "node_a",
			},
			Execution: workload.ExecutionEvidence{LeaseError: "lease_not_active"},
		})
	})
	mux.HandleFunc("/admission/lease-by-instance", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("instance_id") == "inst-fresh" {
			_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{
				Found: true,
				Lease: domain.Lease{ID: "lease-fresh", JobID: "fresh", InstanceID: "inst-fresh"},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	workers, err := refreshPoolProxyWorkers(context.Background(), srv.Client(), srv.URL, "rpc", []poolWorker{
		{WorkloadID: "stale", LeaseID: "lease-stale", InstanceID: "inst-stale", NodeID: "node_a", Status: string(domain.JobRunning)},
		{WorkloadID: "fresh", LeaseID: "lease-fresh", InstanceID: "inst-fresh", NodeID: "node_b", Status: string(domain.JobRunning)},
	})
	if err != nil {
		t.Fatalf("refresh workers: %v", err)
	}
	if len(workers) != 1 || workers[0].WorkloadID != "fresh" {
		t.Fatalf("workers = %+v", workers)
	}
	if _, err := refreshPoolProxyWorkers(context.Background(), srv.Client(), srv.URL, "rpc", []poolWorker{
		{WorkloadID: "stale", LeaseID: "lease-stale", InstanceID: "inst-stale", NodeID: "node_a", Status: string(domain.JobRunning)},
	}); err == nil || !strings.Contains(err.Error(), "no gateway-usable held workers") {
		t.Fatalf("expected no-usable-workers error, got %v", err)
	}
}

func TestRunPoolProxyRefusesInactiveHeldLeaseBeforeServing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(gateway.HeaderProject, "project-a")
		_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
	})
	mux.HandleFunc("/workloads/w-stale", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(workload.StatusResponse{
			Job: domain.Job{ID: "w-stale", Status: domain.JobRunning},
			Result: &domain.JobResult{
				LeaseID:  "lease-stale",
				Instance: domain.ModelInstance{ID: "inst-stale"},
				NodeID:   "node_a",
			},
			Execution: workload.ExecutionEvidence{LeaseError: "lease_not_active"},
		})
	})
	mux.HandleFunc("/admission/lease-by-instance", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pool.json")
	if err := writePrettyJSONFile(statePath, poolState{Name: "ocr", Model: "chandra", Project: "project-a", Workers: []poolWorker{
		{WorkloadID: "w-stale", LeaseID: "lease-stale", InstanceID: "inst-stale", NodeID: "node_a", Status: string(domain.JobRunning)},
	}}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	err := RunWithClient(context.Background(), []string{
		"pool", "proxy",
		"--state", statePath,
		"--url", srv.URL,
		"--rpc-token", "rpc-secret",
		"--gateway-token", "gateway-secret",
	}, srv.Client())
	if err == nil || !strings.Contains(err.Error(), "no gateway-usable held workers") {
		t.Fatalf("expected stale lease refusal, got %v", err)
	}
}

func TestMergePoolProxyWorkerValidationKeepsOriginalOrdering(t *testing.T) {
	merged := mergePoolProxyWorkerValidation([]poolWorker{
		{WorkloadID: "stale", Status: string(domain.JobRunning), LeaseID: "old"},
		{WorkloadID: "fresh", Status: string(domain.JobRunning), LeaseID: "old"},
	}, []poolWorker{
		{WorkloadID: "fresh", Status: string(domain.JobRunning), LeaseID: "new", InstanceID: "inst-new"},
	})
	if len(merged) != 2 || merged[0].LeaseID != "old" || merged[1].LeaseID != "new" || merged[1].InstanceID != "inst-new" {
		t.Fatalf("merged = %+v", merged)
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
