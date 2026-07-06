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
	"sync"
	"sync/atomic"
	"time"

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
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token used to validate held leases")
	rpcTokenEnv := fs.String("rpc-token-env", "", "environment variable holding the peer RPC bearer token")
	rpcTokenFile := fs.String("rpc-token-file", "", "file holding the peer RPC bearer token")
	token := fs.String("gateway-token", "", "gateway bearer token to attach to forwarded requests")
	tokenEnv := fs.String("gateway-token-env", "", "environment variable holding the gateway bearer token")
	tokenFile := fs.String("gateway-token-file", "", "file holding the gateway bearer token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *statePath == "" || *gatewayURL == "" {
		return fmt.Errorf("usage: myce pool proxy --state pool.json --url <gateway-url> --rpc-token <token> [--listen 127.0.0.1:0] [--gateway-token ... | --gateway-token-env NAME | --gateway-token-file path]")
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
	resolvedRPC, err := resolvePoolProxyToken(*rpcToken, *rpcTokenEnv, *rpcTokenFile)
	if err != nil {
		return fmt.Errorf("resolve pool proxy RPC token: %w", err)
	}
	if strings.TrimSpace(resolvedRPC) == "" {
		return fmt.Errorf("pool proxy requires an RPC token to validate held leases; pass --rpc-token, --rpc-token-env, or --rpc-token-file")
	}
	if err := preflightPoolProxyGateway(ctx, client, target, bearer, state.Project); err != nil {
		return err
	}
	peerURL := poolProxyPeerURL(target)
	workers, err = refreshPoolProxyWorkers(ctx, client, peerURL, resolvedRPC, workers)
	if err != nil {
		return err
	}
	state.Workers = mergePoolProxyWorkerValidation(state.Workers, workers)
	if err := writePrettyJSONFile(*statePath, state); err != nil {
		return err
	}
	handler := newPoolProxyHandler(workers, target, bearer, client)
	handler.validator = func(ctx context.Context, worker *poolWorker) error {
		return refreshPoolWorkerGatewayUsability(ctx, client, peerURL, resolvedRPC, worker)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	fmt.Printf("pool proxy\tlistening=http://%s\tbase_url=http://%s/v1\tworkers=%d\tmodel=%s\n",
		ln.Addr(), ln.Addr(), len(workers), state.Model)
	fmt.Printf("pool proxy\tpoint the consumer's OpenAI-compatible base URL at http://%s/v1; requests ride held leases and never enter normal admission\n", ln.Addr())
	return servePoolProxy(ctx, ln, handler)
}

func runPoolServe(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("pool serve", flag.ContinueOnError)
	planPath := fs.String("plan", "", "optional pool plan JSON from `myce pool plan --out`; if omitted, --model plans from live/config state")
	model := fs.String("model", "", "model alias, model ref, or preset id to serve")
	configPath := fs.String("config", "", "peer config JSON; adds configured presets and supplies live peer defaults")
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	contextLen := fs.Int("context", 0, "context length per replica; 0 uses each preset's context")
	concurrency := fs.Int("concurrency", 0, "expected concurrency envelope per replica; 0 means 1")
	maxPerNode := fs.Int("max-per-node", 0, "cap additional replicas per node; 0 means no cap")
	maxTotal := fs.Int("max-total", 0, "cap additional replicas across the fleet; 0 means no cap")
	live := fs.Bool("live", false, "refresh node and instance state from peers in --config before planning")
	peerURL := fs.String("url", "", "peer/gateway base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token used for admission, validation, and release")
	rpcTokenEnv := fs.String("rpc-token-env", "", "environment variable holding the peer RPC bearer token")
	rpcTokenFile := fs.String("rpc-token-file", "", "file holding the peer RPC bearer token")
	statePath := fs.String("state", "", "optional debug state JSON path; defaults to a temporary file")
	name := fs.String("name", "pool", "pool name used in worker job ids and the default project label")
	project := fs.String("project", "", "project label for pool workers; must match the consumer's project-scoped gateway token")
	runID := fs.String("run-id", "", "explicit run id for worker job ids; defaults to a UTC timestamp")
	admitTimeout := fs.Duration("admit-timeout", 15*time.Minute, "per-worker admission timeout; cold loads legitimately take minutes; 0 disables")
	listen := fs.String("listen", "127.0.0.1:0", "local address the proxy listens on")
	token := fs.String("gateway-token", "", "gateway bearer token to attach to forwarded requests")
	tokenEnv := fs.String("gateway-token-env", "", "environment variable holding the gateway bearer token")
	tokenFile := fs.String("gateway-token-file", "", "file holding the gateway bearer token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *peerURL == "" {
		return fmt.Errorf("usage: myce pool serve (--model <model> --config peer.json --live | --plan plan.json) --url <peer-url> --rpc-token <token> [--listen 127.0.0.1:0] [--gateway-token ... | --gateway-token-env NAME | --gateway-token-file path]")
	}
	resolvedRPC, err := resolvePoolProxyToken(*rpcToken, *rpcTokenEnv, *rpcTokenFile)
	if err != nil {
		return fmt.Errorf("resolve pool serve RPC token: %w", err)
	}
	if strings.TrimSpace(resolvedRPC) == "" {
		return fmt.Errorf("pool serve requires an RPC token; pass --rpc-token, --rpc-token-env, or --rpc-token-file")
	}
	state := strings.TrimSpace(*statePath)
	var cleanup []string
	defer func() {
		for _, path := range cleanup {
			_ = os.Remove(path)
		}
	}()
	if state == "" {
		tmp, err := os.CreateTemp("", "myce-pool-serve-*.json")
		if err != nil {
			return err
		}
		state = tmp.Name()
		if err := tmp.Close(); err != nil {
			return err
		}
		cleanup = append(cleanup, state)
	}
	planned := strings.TrimSpace(*planPath)
	if planned == "" {
		if strings.TrimSpace(*model) == "" {
			return fmt.Errorf("pool serve requires --model when --plan is omitted")
		}
		tmp, err := os.CreateTemp("", "myce-pool-serve-plan-*.json")
		if err != nil {
			return err
		}
		planned = tmp.Name()
		if err := tmp.Close(); err != nil {
			return err
		}
		cleanup = append(cleanup, planned)
		planArgs := []string{"--model", *model, "--out", planned}
		if poolFlagProvided(fs, "db") {
			planArgs = append(planArgs, "--db", *dbPath)
		}
		if strings.TrimSpace(*configPath) != "" {
			planArgs = append(planArgs, "--config", *configPath)
		}
		if *live {
			planArgs = append(planArgs, "--live")
		}
		if *contextLen > 0 {
			planArgs = append(planArgs, "--context", fmt.Sprint(*contextLen))
		}
		if *concurrency > 0 {
			planArgs = append(planArgs, "--concurrency", fmt.Sprint(*concurrency))
		}
		if *maxPerNode > 0 {
			planArgs = append(planArgs, "--max-per-node", fmt.Sprint(*maxPerNode))
		}
		if *maxTotal > 0 {
			planArgs = append(planArgs, "--max-total", fmt.Sprint(*maxTotal))
		}
		if resolvedRPC != "" {
			planArgs = append(planArgs, "--rpc-token", resolvedRPC)
		}
		if err := runPoolPlan(ctx, planArgs); err != nil {
			return fmt.Errorf("pool serve plan: %w", err)
		}
	}
	upArgs := []string{"--plan", planned, "--url", *peerURL, "--rpc-token", resolvedRPC, "--state", state, "--name", *name, "--admit-timeout", admitTimeout.String()}
	if *project != "" {
		upArgs = append(upArgs, "--project", *project)
	}
	if *runID != "" {
		upArgs = append(upArgs, "--run-id", *runID)
	}
	if err := runPoolUp(ctx, upArgs, client); err != nil {
		return err
	}
	defer func() {
		downCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = runPoolDown(downCtx, []string{"--state", state, "--url", *peerURL, "--rpc-token", resolvedRPC, "--reason", "pool serve stopped"}, client)
	}()
	proxyArgs := []string{"--state", state, "--url", *peerURL, "--rpc-token", resolvedRPC, "--listen", *listen}
	if *token != "" {
		proxyArgs = append(proxyArgs, "--gateway-token", *token)
	}
	if *tokenEnv != "" {
		proxyArgs = append(proxyArgs, "--gateway-token-env", *tokenEnv)
	}
	if *tokenFile != "" {
		proxyArgs = append(proxyArgs, "--gateway-token-file", *tokenFile)
	}
	return runPoolProxy(ctx, proxyArgs, client)
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
	mu        sync.Mutex
	workers   []poolWorker
	target    *url.URL
	bearer    string
	client    *http.Client
	validator func(context.Context, *poolWorker) error
	next      atomic.Uint64
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
	workers := h.snapshotWorkers()
	if len(workers) == 0 {
		http.Error(w, "pool proxy: no gateway-usable held workers available", http.StatusServiceUnavailable)
		return
	}
	start := h.next.Add(1) - 1
	var lastResp *pooledResponse
	var lastValidation string
	for attempt := 0; attempt < len(workers); attempt++ {
		worker := workers[(start+uint64(attempt))%uint64(len(workers))]
		if err := h.validateLiveWorker(r.Context(), worker); err != nil {
			lastValidation = err.Error()
			continue
		}
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
	if lastValidation != "" {
		http.Error(w, "pool proxy: no gateway-usable held workers available: "+lastValidation, http.StatusServiceUnavailable)
		return
	}
	http.Error(w, "pool proxy: no gateway-usable held workers available", http.StatusServiceUnavailable)
}

func (h *poolProxyHandler) snapshotWorkers() []poolWorker {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]poolWorker(nil), h.workers...)
}

func (h *poolProxyHandler) validateLiveWorker(ctx context.Context, worker poolWorker) error {
	if h.validator == nil {
		return nil
	}
	refreshed := worker
	if err := h.validator(ctx, &refreshed); err != nil {
		return err
	}
	if !poolWorkerHeld(refreshed) {
		h.removeWorker(worker.WorkloadID)
		if refreshed.Error != "" {
			return fmt.Errorf("worker %s is not gateway-usable: %s", worker.WorkloadID, refreshed.Error)
		}
		return fmt.Errorf("worker %s is not gateway-usable: status %s", worker.WorkloadID, refreshed.Status)
	}
	h.replaceWorker(refreshed)
	return nil
}

func (h *poolProxyHandler) removeWorker(workloadID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var kept []poolWorker
	for _, worker := range h.workers {
		if worker.WorkloadID != workloadID {
			kept = append(kept, worker)
		}
	}
	h.workers = kept
}

func (h *poolProxyHandler) replaceWorker(updated poolWorker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.workers {
		if h.workers[i].WorkloadID == updated.WorkloadID {
			h.workers[i] = updated
			return
		}
	}
	h.workers = append(h.workers, updated)
}

type pooledResponse struct {
	status int
	header http.Header
	body   []byte
}

func refreshPoolProxyWorkers(ctx context.Context, client *http.Client, peerURL, rpcToken string, workers []poolWorker) ([]poolWorker, error) {
	var usable []poolWorker
	var reasons []string
	for _, worker := range workers {
		refreshed := worker
		if err := refreshPoolWorkerGatewayUsability(ctx, client, peerURL, rpcToken, &refreshed); err != nil {
			reasons = append(reasons, fmt.Sprintf("%s: %v", worker.WorkloadID, err))
			continue
		}
		if poolWorkerHeld(refreshed) {
			usable = append(usable, refreshed)
			continue
		}
		if refreshed.Error != "" {
			reasons = append(reasons, fmt.Sprintf("%s: %s", worker.WorkloadID, refreshed.Error))
		} else {
			reasons = append(reasons, fmt.Sprintf("%s: status %s", worker.WorkloadID, refreshed.Status))
		}
	}
	if len(usable) == 0 {
		if len(reasons) == 0 {
			return nil, fmt.Errorf("pool has no gateway-usable held workers")
		}
		return nil, fmt.Errorf("pool has no gateway-usable held workers: %s", strings.Join(reasons, "; "))
	}
	return usable, nil
}

func mergePoolProxyWorkerValidation(existing, usable []poolWorker) []poolWorker {
	byID := map[string]poolWorker{}
	for _, worker := range usable {
		byID[worker.WorkloadID] = worker
	}
	out := append([]poolWorker(nil), existing...)
	for i := range out {
		if refreshed, ok := byID[out[i].WorkloadID]; ok {
			out[i] = refreshed
		}
	}
	return out
}

func poolProxyPeerURL(target *url.URL) string {
	out := *target
	path := strings.TrimRight(out.Path, "/")
	if path == "/v1" || strings.HasSuffix(path, "/v1") {
		out.Path = strings.TrimSuffix(path, "/v1")
	}
	return strings.TrimRight(out.String(), "/")
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
