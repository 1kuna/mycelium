package controlcli

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"

	"mycelium/internal/gateway"
)

// myce pool proxy — the bridge that lets header-unaware OpenAI-compatible
// consumers (an OCR CLI, any tool with only a base-URL setting) use held pool
// workers. It forwards requests to the gateway with X-Myc-Workload,
// X-Myc-Lease, and X-Myc-Lease-Hold injected, rotating across held workers,
// so consumer calls ride reserved capacity instead of normal admission.

func runPoolProxy(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("pool proxy", flag.ContinueOnError)
	statePath := fs.String("state", "", "pool state JSON from `myce pool up --state`")
	gatewayURL := fs.String("url", "", "gateway base URL the proxy forwards to")
	listen := fs.String("listen", "127.0.0.1:0", "local address the proxy listens on")
	token := fs.String("gateway-token", "", "gateway bearer token to attach to forwarded requests")
	tokenEnv := fs.String("gateway-token-env", "", "environment variable holding the gateway bearer token")
	tokenFile := fs.String("gateway-token-file", "", "file holding the gateway bearer token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *statePath == "" || *gatewayURL == "" {
		return fmt.Errorf("usage: myce pool proxy --state pool.json --url <gateway-url> [--listen 127.0.0.1:0] [--gateway-token ... | --gateway-token-env NAME | --gateway-token-file path]")
	}
	state, err := loadPoolState(*statePath)
	if err != nil {
		return err
	}
	workers := heldPoolWorkers(state)
	if len(workers) == 0 {
		return fmt.Errorf("pool state %q has no held running workers; run `myce pool up` and `myce pool status` first", *statePath)
	}
	target, err := url.Parse(strings.TrimRight(*gatewayURL, "/"))
	if err != nil || target.Scheme == "" || target.Host == "" {
		return fmt.Errorf("invalid gateway url %q", *gatewayURL)
	}
	bearer, err := resolvePoolProxyToken(*token, *tokenEnv, *tokenFile)
	if err != nil {
		return err
	}
	if err := preflightPoolProxyGateway(ctx, client, target, bearer, state.Project); err != nil {
		return err
	}
	handler := newPoolProxyHandler(workers, target, bearer, client)
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	fmt.Printf("pool proxy\tlistening=http://%s\tbase_url=http://%s/v1\tworkers=%d\tmodel=%s\n",
		ln.Addr(), ln.Addr(), len(workers), state.Model)
	fmt.Printf("pool proxy\tpoint the consumer's OpenAI-compatible base URL at http://%s/v1; requests ride held leases and never enter normal admission\n", ln.Addr())
	return servePoolProxy(ctx, ln, handler)
}

func preflightPoolProxyGateway(ctx context.Context, client *http.Client, target *url.URL, bearer, project string) error {
	bearer = strings.TrimSpace(bearer)
	if bearer == "" {
		return fmt.Errorf("pool proxy requires a gateway token; pass --gateway-token, --gateway-token-env, or --gateway-token-file")
	}
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, poolProxyModelsURL(target), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("pool proxy gateway token preflight failed: %w", err)
	}
	defer resp.Body.Close()
	body, readErr := readControlHTTPBody(resp.Body, "pool proxy gateway preflight body")
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("pool proxy gateway token preflight returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	project = strings.TrimSpace(project)
	if project == "" {
		return nil
	}
	reported := strings.TrimSpace(resp.Header.Get(gateway.HeaderProject))
	if reported == "" {
		return fmt.Errorf("pool proxy gateway token preflight did not report %s; cannot verify token project %q", gateway.HeaderProject, project)
	}
	if reported != project {
		return fmt.Errorf("pool proxy gateway token project mismatch: pool project %q, gateway reported %q", project, reported)
	}
	return nil
}

func poolProxyModelsURL(target *url.URL) string {
	out := *target
	path := strings.TrimRight(out.Path, "/")
	if path == "" {
		out.Path = "/v1/models"
	} else if path == "/v1" || strings.HasSuffix(path, "/v1") {
		out.Path = path + "/models"
	} else {
		out.Path = path + "/v1/models"
	}
	return out.String()
}

func servePoolProxy(ctx context.Context, ln net.Listener, handler http.Handler) error {
	server := &http.Server{Handler: handler}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-done:
		}
	}()
	err := server.Serve(ln)
	close(done)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func heldPoolWorkers(state poolState) []poolWorker {
	var held []poolWorker
	for _, worker := range state.Workers {
		if worker.WorkloadID != "" && poolWorkerHeld(worker) {
			held = append(held, worker)
		}
	}
	return held
}

func resolvePoolProxyToken(token, tokenEnv, tokenFile string) (string, error) {
	if token != "" {
		return token, nil
	}
	if tokenEnv != "" {
		value := strings.TrimSpace(os.Getenv(tokenEnv))
		if value == "" {
			return "", fmt.Errorf("gateway token environment variable %q is empty", tokenEnv)
		}
		return value, nil
	}
	if tokenFile != "" {
		data, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("read gateway token file: %w", err)
		}
		value := strings.TrimSpace(string(data))
		if value == "" {
			return "", fmt.Errorf("gateway token file %q is empty", tokenFile)
		}
		return value, nil
	}
	return "", nil
}

type poolProxyHandler struct {
	workers []poolWorker
	target  *url.URL
	bearer  string
	client  *http.Client
	next    atomic.Uint64
}

func newPoolProxyHandler(workers []poolWorker, target *url.URL, bearer string, client *http.Client) *poolProxyHandler {
	if client == nil {
		client = http.DefaultClient
	}
	return &poolProxyHandler{workers: workers, target: target, bearer: bearer, client: client}
}

func (h *poolProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("pool proxy: read request body: %v", err), http.StatusBadGateway)
		return
	}
	start := h.next.Add(1) - 1
	var lastResp *pooledResponse
	for attempt := 0; attempt < len(h.workers); attempt++ {
		worker := h.workers[(start+uint64(attempt))%uint64(len(h.workers))]
		resp, err := h.forward(r, worker, body)
		if err != nil {
			http.Error(w, fmt.Sprintf("pool proxy: forward to gateway: %v", err), http.StatusBadGateway)
			return
		}
		// 404/409 are lease-validation refusals (worker finished, lease
		// released, mismatch); try the next held worker before giving up.
		if resp.status == http.StatusNotFound || resp.status == http.StatusConflict {
			lastResp = resp
			continue
		}
		resp.write(w)
		return
	}
	if lastResp != nil {
		lastResp.write(w)
		return
	}
	http.Error(w, "pool proxy: no held workers available", http.StatusBadGateway)
}

type pooledResponse struct {
	status int
	header http.Header
	body   []byte
}

func (p *pooledResponse) write(w http.ResponseWriter) {
	for key, values := range p.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(p.status)
	_, _ = w.Write(p.body)
}

func (h *poolProxyHandler) forward(r *http.Request, worker poolWorker, body []byte) (*pooledResponse, error) {
	out := h.target.JoinPath(r.URL.Path)
	out.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(r.Context(), r.Method, out.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	copyPoolProxyHeaders(req.Header, r.Header)
	if h.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+h.bearer)
	}
	req.Header.Set(gateway.HeaderWorkload, worker.WorkloadID)
	req.Header.Set(gateway.HeaderLease, worker.LeaseID)
	req.Header.Set(gateway.HeaderLeaseHold, "true")
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &pooledResponse{status: resp.StatusCode, header: resp.Header.Clone(), body: respBody}, nil
}

// copyPoolProxyHeaders copies client headers minus hop-by-hop fields and any
// caller-supplied Mycelium lease headers, which the proxy owns.
func copyPoolProxyHeaders(dst, src http.Header) {
	skip := map[string]struct{}{
		"Connection":            {},
		"Keep-Alive":            {},
		"Proxy-Authenticate":    {},
		"Proxy-Authorization":   {},
		"Proxy-Connection":      {},
		"Te":                    {},
		"Trailer":               {},
		"Transfer-Encoding":     {},
		"Upgrade":               {},
		"Content-Length":        {},
		gateway.HeaderWorkload:  {},
		gateway.HeaderLease:     {},
		gateway.HeaderLeaseHold: {},
	}
	for key, values := range src {
		if _, ok := skip[http.CanonicalHeaderKey(key)]; ok {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
