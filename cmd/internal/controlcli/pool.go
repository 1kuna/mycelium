package controlcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/pool"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/internal/workload"
)

// myce pool <plan|up|status|down> — replica worker pools for one model.
//
// plan computes the maximum safe replicas per node from the control store's
// fleet facts using the same allocator math owner admission enforces.
// up launches the planned replicas through /workloads admission and HOLDS the
// leases so the capacity stays reserved (unlike prewarm, which releases).
// status refreshes worker state; down finishes/cancels every worker so the
// reservations are released.

var poolStatusPollInterval = 2 * time.Second
var poolDownVerifyTimeout = 10 * time.Second

func runPool(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce pool <plan|up|status|proxy|down>")
	}
	if client == nil {
		client = http.DefaultClient
	}
	switch args[0] {
	case "plan":
		return runPoolPlan(ctx, args[1:])
	case "up":
		return runPoolUp(ctx, args[1:], client)
	case "status":
		return runPoolStatus(ctx, args[1:], client)
	case "proxy":
		return runPoolProxy(ctx, args[1:], client)
	case "down":
		return runPoolDown(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce pool <plan|up|status|proxy|down>")
	}
}

func runPoolPlan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pool plan", flag.ContinueOnError)
	model := fs.String("model", "", "model alias, model ref, or preset id")
	configPath := fs.String("config", "", "peer config JSON; adds configured presets and supplies live peer defaults")
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	contextLen := fs.Int("context", 0, "context length per replica; 0 uses each preset's context")
	concurrency := fs.Int("concurrency", 0, "expected concurrency envelope per replica; 0 means 1")
	maxPerNode := fs.Int("max-per-node", 0, "cap additional replicas per node; 0 means no cap")
	maxTotal := fs.Int("max-total", 0, "cap additional replicas across the fleet; 0 means no cap")
	outPath := fs.String("out", "", "write the plan JSON to this file")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token for live snapshot refresh")
	live := fs.Bool("live", false, "refresh node and instance state from the peers in --config before planning")
	var peerURLs repeatedString
	fs.Var(&peerURLs, "peer-url", "compute peer URL to snapshot before planning; may be repeated")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || strings.TrimSpace(*model) == "" {
		return fmt.Errorf("usage: myce pool plan --model <model> [--config peer.json] [--live] [--db path] [--context n] [--concurrency n] [--max-per-node n] [--max-total n] [--out plan.json] [--json]")
	}
	var cfg operatorPeerConfig
	if strings.TrimSpace(*configPath) != "" {
		loaded, err := loadOperatorConfig(*configPath)
		if err != nil {
			return err
		}
		cfg = loaded.Config
		if !poolFlagProvided(fs, "db") && cfg.StorePath != "" {
			*dbPath = cfg.StorePath
		}
		if strings.TrimSpace(*rpcToken) == "" {
			*rpcToken = cfg.RPCToken
		}
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	presets, err := resolvePoolPresets(ctx, store, *model, cfg.Presets)
	if err != nil {
		return err
	}
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		return err
	}
	instances, err := store.ListInstances(ctx)
	if err != nil {
		return err
	}
	snapshotURLs := append([]string(nil), peerURLs...)
	if *live {
		cfgURLs := poolSnapshotURLsFromConfig(cfg)
		if len(cfgURLs) == 0 && len(snapshotURLs) == 0 {
			return fmt.Errorf("pool plan --live requires --config with seed peers or explicit --peer-url values")
		}
		snapshotURLs = append(snapshotURLs, cfgURLs...)
	}
	if len(snapshotURLs) > 0 {
		nodes, instances, err = overlayPoolSnapshots(ctx, http.DefaultClient, nodes, instances, snapshotURLs, *rpcToken)
		if err != nil {
			return err
		}
	}
	plan, err := pool.BuildPlan(ctx, estimate.NewBackendAware(estimate.NewInMemory(), estimate.NewInMemory()), lease.NewAllocator(), *model, presets, domain.FleetSnapshot{Nodes: nodes, Instances: instances}, pool.Options{
		ContextLength: *contextLen,
		Concurrency:   *concurrency,
		MaxPerNode:    *maxPerNode,
		MaxTotal:      *maxTotal,
	})
	if err != nil {
		return err
	}
	if *outPath != "" {
		if err := writePrettyJSONFile(*outPath, plan); err != nil {
			return err
		}
	}
	if *jsonOut {
		return writePrettyJSON(os.Stdout, plan)
	}
	printPoolPlan(plan)
	return nil
}

func poolFlagProvided(fs *flag.FlagSet, name string) bool {
	provided := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			provided = true
		}
	})
	return provided
}

func poolSnapshotURLsFromConfig(cfg operatorPeerConfig) []string {
	seen := map[string]struct{}{}
	var urls []string
	add := func(raw string) {
		base := strings.TrimRight(catalogPeerBaseURL(raw), "/")
		if base == "" {
			return
		}
		if _, ok := seen[base]; ok {
			return
		}
		seen[base] = struct{}{}
		urls = append(urls, base)
	}
	for _, seed := range cfg.SeedPeers {
		add(seed)
	}
	if cfg.Compute {
		add(cfg.Listen)
	}
	return urls
}

func overlayPoolSnapshots(ctx context.Context, client *http.Client, nodes []domain.Node, instances []domain.ModelInstance, peerURLs []string, rpcToken string) ([]domain.Node, []domain.ModelInstance, error) {
	if client == nil {
		client = http.DefaultClient
	}
	nodeByID := make(map[string]domain.Node, len(nodes))
	order := make([]string, 0, len(nodes)+len(peerURLs))
	for _, node := range nodes {
		if node.ID == "" {
			continue
		}
		if _, ok := nodeByID[node.ID]; !ok {
			order = append(order, node.ID)
		}
		nodeByID[node.ID] = node
	}
	liveInstancesByNode := map[string][]domain.ModelInstance{}
	for _, peerURL := range peerURLs {
		snap, err := fetchNodeSnapshot(ctx, client, peerURL, rpcToken)
		if err != nil {
			return nil, nil, err
		}
		if snap.Node.ID == "" {
			return nil, nil, fmt.Errorf("snapshot %s returned empty node id", peerURL)
		}
		if err := annotatePoolSnapshotLeases(ctx, client, strings.TrimRight(peerURL, "/"), rpcToken, &snap); err != nil {
			return nil, nil, err
		}
		if _, ok := nodeByID[snap.Node.ID]; !ok {
			order = append(order, snap.Node.ID)
		}
		nodeByID[snap.Node.ID] = snap.Node
		liveInstancesByNode[snap.Node.ID] = append([]domain.ModelInstance(nil), snap.Instances...)
	}
	mergedNodes := make([]domain.Node, 0, len(order))
	for _, id := range order {
		mergedNodes = append(mergedNodes, nodeByID[id])
	}
	mergedInstances := make([]domain.ModelInstance, 0, len(instances))
	for _, inst := range instances {
		if _, replaced := liveInstancesByNode[inst.NodeID]; replaced {
			continue
		}
		mergedInstances = append(mergedInstances, inst)
	}
	for _, id := range order {
		mergedInstances = append(mergedInstances, liveInstancesByNode[id]...)
	}
	return mergedNodes, mergedInstances, nil
}

type poolAdmissionLeaseResponse struct {
	Found bool         `json:"found"`
	Lease domain.Lease `json:"lease,omitempty"`
}

func annotatePoolSnapshotLeases(ctx context.Context, client *http.Client, peerURL, rpcToken string, snap *domain.NodeSnapshot) error {
	for i := range snap.Instances {
		if snap.Instances[i].ID == "" {
			continue
		}
		lease, found, err := fetchPoolAdmissionLeaseByInstance(ctx, client, peerURL, rpcToken, snap.Instances[i].ID)
		if err != nil {
			return fmt.Errorf("snapshot %s admission lease for instance %q: %w", peerURL, snap.Instances[i].ID, err)
		}
		if !found {
			continue
		}
		snap.Instances[i].LeaseID = lease.ID
		snap.Instances[i].ReservationID = lease.ReservationID
	}
	return nil
}

func fetchPoolAdmissionLeaseByInstance(ctx context.Context, client *http.Client, peerURL, rpcToken, instanceID string) (domain.Lease, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peerURL, "/")+"/admission/lease-by-instance?instance_id="+url.QueryEscape(instanceID), nil)
	if err != nil {
		return domain.Lease{}, false, err
	}
	if rpcToken != "" {
		req.Header.Set("Authorization", "Bearer "+rpcToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return domain.Lease{}, false, err
	}
	defer resp.Body.Close()
	body, err := readControlHTTPBody(resp.Body, "admission lease response body")
	if err != nil {
		return domain.Lease{}, false, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return domain.Lease{}, false, fmt.Errorf("returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out poolAdmissionLeaseResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return domain.Lease{}, false, fmt.Errorf("decode admission lease response: %w", err)
	}
	return out.Lease, out.Found, nil
}

func resolvePoolPresets(ctx context.Context, store *storesqlite.Store, model string, configured []domain.Preset) ([]domain.Preset, error) {
	stored, err := store.ListPresets(ctx)
	if err != nil {
		return nil, err
	}
	all := mergePoolPresets(stored, configured)
	var matched []domain.Preset
	for _, preset := range all {
		if poolPresetMatches(preset, model) {
			matched = append(matched, preset)
		}
	}
	if len(matched) == 0 {
		return nil, fmt.Errorf("no presets match model %q; register the preset in the store or pass --config with the live route catalog", model)
	}
	return matched, nil
}

func mergePoolPresets(stored, configured []domain.Preset) []domain.Preset {
	byID := map[string]int{}
	out := make([]domain.Preset, 0, len(stored)+len(configured))
	for _, preset := range stored {
		if preset.ID == "" {
			continue
		}
		byID[preset.ID] = len(out)
		out = append(out, preset)
	}
	for _, preset := range configured {
		if preset.ID == "" {
			continue
		}
		if idx, ok := byID[preset.ID]; ok {
			out[idx] = preset
			continue
		}
		byID[preset.ID] = len(out)
		out = append(out, preset)
	}
	return out
}

func poolPresetMatches(preset domain.Preset, model string) bool {
	if preset.ID == model || preset.ModelRef == model {
		return true
	}
	for _, alias := range preset.Aliases {
		if alias == model {
			return true
		}
	}
	return false
}

// poolState is the durable record of a launched pool, written by `pool up`
// and consumed by `pool status` / `pool down`.
type poolState struct {
	Name      string       `json:"name"`
	Model     string       `json:"model"`
	Project   string       `json:"project,omitempty"`
	CreatedAt time.Time    `json:"created_at"`
	Workers   []poolWorker `json:"workers"`
}

type poolWorker struct {
	WorkloadID string `json:"workload_id"`
	NodeID     string `json:"node_id"`
	PresetID   string `json:"preset_id,omitempty"`
	InstanceID string `json:"instance_id,omitempty"`
	LeaseID    string `json:"lease_id,omitempty"`
	Status     string `json:"status"`
	Queued     bool   `json:"queued,omitempty"`
	Error      string `json:"error,omitempty"`
}

func runPoolUp(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("pool up", flag.ContinueOnError)
	planPath := fs.String("plan", "", "pool plan JSON from `myce pool plan --out`")
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	name := fs.String("name", "pool", "pool name used in worker job ids and the default project label")
	project := fs.String("project", "", "project label for pool workers; must match the consumer's project-scoped gateway token; defaults to pool-<name>")
	runID := fs.String("run-id", "", "explicit run id for worker job ids; defaults to a UTC timestamp")
	statePath := fs.String("state", "", "write pool worker state JSON to this file")
	admitTimeout := fs.Duration("admit-timeout", 15*time.Minute, "per-worker admission timeout; cold loads legitimately take minutes; 0 disables")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *planPath == "" || *peerURL == "" || *statePath == "" {
		return fmt.Errorf("usage: myce pool up --plan plan.json --url <peer-url> --state pool.json [--rpc-token token] [--name pool] [--admit-timeout 15m] [--json]")
	}
	plan, err := loadPoolPlan(*planPath)
	if err != nil {
		return err
	}
	run := strings.TrimSpace(*runID)
	if run == "" {
		run = time.Now().UTC().Format("20060102t150405z")
	}
	workerProject := strings.TrimSpace(*project)
	if workerProject == "" {
		workerProject = "pool-" + *name
	}
	state := poolState{Name: *name, Model: plan.Model, Project: workerProject, CreatedAt: time.Now().UTC()}
	var jobs []domain.Job
	for _, node := range plan.Nodes {
		for i := 0; i < node.Additional; i++ {
			job := domain.Job{
				ID:                  fmt.Sprintf("%s-%s-%s-%d", *name, run, node.NodeID, i+1),
				TaskType:            string(domain.CapabilityChat),
				Model:               plan.Model,
				PresetID:            node.PresetID,
				Project:             workerProject,
				Priority:            domain.PriorityBackground,
				SpeedPref:           domain.SpeedThroughput,
				ContextRequest:      node.ContextLength,
				ExpectedConcurrency: plan.Concurrency,
				Preemption:          domain.PreemptSoft,
				NodeSelector:        map[string]string{domain.LabelNodeID: node.NodeID},
				Status:              domain.JobQueued,
			}
			jobs = append(jobs, job)
			state.Workers = append(state.Workers, poolWorker{
				WorkloadID: job.ID,
				NodeID:     node.NodeID,
				PresetID:   node.PresetID,
				Status:     "pending",
			})
		}
	}
	// Persist every planned worker BEFORE the first admission so a killed or
	// timed-out run always leaves cleanup-capable state for `pool down`.
	if err := writePrettyJSONFile(*statePath, state); err != nil {
		return err
	}
	for i := range jobs {
		worker := &state.Workers[i]
		fmt.Printf("pool up\tlaunching %s on %s (a cold load can take minutes; admit timeout %s)\n",
			worker.WorkloadID, worker.NodeID, admitTimeoutLabel(*admitTimeout))
		admitCtx := ctx
		cancel := func() {}
		if *admitTimeout > 0 {
			admitCtx, cancel = context.WithTimeout(ctx, *admitTimeout)
		}
		admitted, err := admitPrewarmWorkload(admitCtx, client, *peerURL, *rpcToken, jobs[i])
		if err != nil {
			worker.Status = "admit_error"
			worker.Error = err.Error()
		} else {
			applyPoolAdmission(worker, admitted)
			if !poolWorkerHeld(*worker) && !poolWorkerTerminal(worker.Status) {
				if waitErr := waitPoolWorkerHeld(admitCtx, client, *peerURL, *rpcToken, worker); waitErr != nil {
					worker.Error = waitErr.Error()
				}
			}
		}
		cancel()
		// Flush after every worker so state on disk always matches reality.
		if err := writePrettyJSONFile(*statePath, state); err != nil {
			return err
		}
	}
	if *jsonOut {
		if err := writePrettyJSON(os.Stdout, state); err != nil {
			return err
		}
	} else {
		printPoolState("pool up", state)
	}
	held, failed := countPoolWorkers(state)
	if len(state.Workers) > 0 && held != len(state.Workers) {
		if held == 0 {
			return fmt.Errorf("pool up held no workers (%d attempted); inspect and release with `myce pool down --state %s`", len(state.Workers), *statePath)
		}
		return fmt.Errorf("pool up held %d/%d workers (%d failed or not held); inspect and release with `myce pool down --state %s`", held, len(state.Workers), failed, *statePath)
	}
	return nil
}

func admitTimeoutLabel(timeout time.Duration) string {
	if timeout <= 0 {
		return "disabled"
	}
	return timeout.String()
}

func runPoolStatus(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("pool status", flag.ContinueOnError)
	statePath := fs.String("state", "", "pool state JSON from `myce pool up --state`")
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *statePath == "" || *peerURL == "" {
		return fmt.Errorf("usage: myce pool status --state pool.json --url <peer-url> [--rpc-token token] [--json]")
	}
	state, err := loadPoolState(*statePath)
	if err != nil {
		return err
	}
	for i := range state.Workers {
		worker := &state.Workers[i]
		if worker.WorkloadID == "" {
			continue
		}
		status, err := fetchPoolWorkload(ctx, client, *peerURL, *rpcToken, worker.WorkloadID)
		if errors.Is(err, errPoolWorkloadNotFound) {
			worker.Status = "not_admitted"
			worker.Error = ""
			continue
		}
		if err != nil {
			worker.Error = err.Error()
			continue
		}
		applyPoolStatus(worker, status)
	}
	if err := writePrettyJSONFile(*statePath, state); err != nil {
		return err
	}
	if *jsonOut {
		return writePrettyJSON(os.Stdout, state)
	}
	printPoolState("pool status", state)
	return nil
}

func runPoolDown(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("pool down", flag.ContinueOnError)
	statePath := fs.String("state", "", "pool state JSON from `myce pool up --state`")
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	reason := fs.String("reason", "pool down", "cancel reason recorded for queued workers")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *statePath == "" || *peerURL == "" {
		return fmt.Errorf("usage: myce pool down --state pool.json --url <peer-url> [--rpc-token token] [--json]")
	}
	state, err := loadPoolState(*statePath)
	if err != nil {
		return err
	}
	var downErrs []error
	for i := range state.Workers {
		worker := &state.Workers[i]
		if worker.WorkloadID == "" || poolWorkerTerminal(worker.Status) {
			continue
		}
		if shouldRefreshPoolWorkerBeforeDown(*worker) {
			status, err := fetchPoolWorkload(ctx, client, *peerURL, *rpcToken, worker.WorkloadID)
			if errors.Is(err, errPoolWorkloadNotFound) {
				worker.Status = "not_admitted"
				worker.Error = ""
				continue
			}
			if err != nil {
				worker.Error = err.Error()
				downErrs = append(downErrs, fmt.Errorf("worker %s: %w", worker.WorkloadID, err))
				continue
			}
			applyPoolStatus(worker, status)
			if poolWorkerTerminal(worker.Status) {
				continue
			}
		}
		var release error
		if worker.LeaseID != "" && !worker.Queued {
			finished, err := finishPrewarmWorkload(ctx, client, *peerURL, *rpcToken, worker.WorkloadID)
			if err == nil {
				worker.Status = string(finished.Job.Status)
				if finished.Result != nil {
					applyPoolResult(worker, finished.Result)
				}
			}
			release = err
		} else {
			canceled, err := cancelPoolWorkload(ctx, client, *peerURL, *rpcToken, worker.WorkloadID, *reason)
			if errors.Is(err, errPoolWorkloadNotFound) {
				// The admit never reached the daemon (killed or timed-out
				// run); there is nothing to release for this worker.
				worker.Status = "not_admitted"
				worker.Error = ""
				continue
			}
			if err == nil {
				worker.Status = string(canceled.Job.Status)
				if canceled.Result != nil {
					applyPoolResult(worker, canceled.Result)
				}
				err = verifyPoolWorkerReleasedAfterCancel(ctx, client, *peerURL, *rpcToken, worker)
			}
			release = err
		}
		if release != nil {
			worker.Error = release.Error()
			downErrs = append(downErrs, fmt.Errorf("worker %s: %w", worker.WorkloadID, release))
			continue
		}
		worker.Error = ""
	}
	if err := writePrettyJSONFile(*statePath, state); err != nil {
		return err
	}
	if *jsonOut {
		if err := writePrettyJSON(os.Stdout, state); err != nil {
			return err
		}
	} else {
		printPoolState("pool down", state)
	}
	return errors.Join(downErrs...)
}

func verifyPoolWorkerReleasedAfterCancel(ctx context.Context, client *http.Client, peerURL, rpcToken string, worker *poolWorker) error {
	if worker.WorkloadID == "" {
		return nil
	}
	verifyCtx, cancel := context.WithTimeout(ctx, poolDownVerifyTimeout)
	defer cancel()
	ticker := time.NewTicker(poolStatusPollInterval)
	defer ticker.Stop()
	stableTerminal := false
	for {
		select {
		case <-verifyCtx.Done():
			return nil
		case <-ticker.C:
			status, err := fetchPoolWorkload(verifyCtx, client, peerURL, rpcToken, worker.WorkloadID)
			if errors.Is(err, errPoolWorkloadNotFound) {
				worker.Status = "not_admitted"
				worker.Error = ""
				return nil
			}
			if err != nil {
				return fmt.Errorf("verify canceled worker release: %w", err)
			}
			applyPoolStatus(worker, status)
			if poolWorkerHeld(*worker) {
				finished, err := finishPrewarmWorkload(verifyCtx, client, peerURL, rpcToken, worker.WorkloadID)
				if err != nil {
					return fmt.Errorf("release raced held worker: %w", err)
				}
				worker.Status = string(finished.Job.Status)
				worker.Queued = false
				if finished.Result != nil {
					applyPoolResult(worker, finished.Result)
				}
				worker.Error = finished.Job.Error
				return nil
			}
			if poolWorkerTerminal(worker.Status) {
				if stableTerminal {
					return nil
				}
				stableTerminal = true
				continue
			}
			stableTerminal = false
		}
	}
}

func shouldRefreshPoolWorkerBeforeDown(worker poolWorker) bool {
	if worker.Queued || poolWorkerHeld(worker) || poolWorkerTerminal(worker.Status) {
		return false
	}
	return worker.WorkloadID != "" && (worker.LeaseID == "" || worker.InstanceID == "" || worker.Status == "admit_error")
}

func loadPoolPlan(path string) (pool.Plan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return pool.Plan{}, fmt.Errorf("read pool plan: %w", err)
	}
	var plan pool.Plan
	if err := json.Unmarshal(data, &plan); err != nil {
		return pool.Plan{}, fmt.Errorf("decode pool plan: %w", err)
	}
	if plan.Model == "" {
		return pool.Plan{}, fmt.Errorf("pool plan %q has no model", path)
	}
	return plan, nil
}

func loadPoolState(path string) (poolState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return poolState{}, fmt.Errorf("read pool state: %w", err)
	}
	var state poolState
	if err := json.Unmarshal(data, &state); err != nil {
		return poolState{}, fmt.Errorf("decode pool state: %w", err)
	}
	return state, nil
}

func fetchPoolWorkload(ctx context.Context, client *http.Client, peerURL, rpcToken, id string) (workload.StatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peerURL, "/")+"/workloads/"+url.PathEscape(id), nil)
	if err != nil {
		return workload.StatusResponse{}, err
	}
	if rpcToken != "" {
		req.Header.Set("Authorization", "Bearer "+rpcToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return workload.StatusResponse{}, err
	}
	defer resp.Body.Close()
	body, err := readControlHTTPBody(resp.Body, "pool workload status body")
	if err != nil {
		return workload.StatusResponse{}, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return workload.StatusResponse{}, fmt.Errorf("%w: %s", errPoolWorkloadNotFound, strings.TrimSpace(string(body)))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return workload.StatusResponse{}, fmt.Errorf("pool workload status returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out workload.StatusResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return workload.StatusResponse{}, err
	}
	return out, nil
}

func cancelPoolWorkload(ctx context.Context, client *http.Client, peerURL, rpcToken, id, reason string) (workload.FinishResponse, error) {
	body, err := json.Marshal(workload.CancelRequest{Reason: reason})
	if err != nil {
		return workload.FinishResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(peerURL, "/")+"/workloads/"+url.PathEscape(id)+"/cancel", strings.NewReader(string(body)))
	if err != nil {
		return workload.FinishResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if rpcToken != "" {
		req.Header.Set("Authorization", "Bearer "+rpcToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return workload.FinishResponse{}, err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "pool workload cancel body")
	if err != nil {
		return workload.FinishResponse{}, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return workload.FinishResponse{}, fmt.Errorf("%w: %s", errPoolWorkloadNotFound, strings.TrimSpace(string(respBody)))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return workload.FinishResponse{}, fmt.Errorf("pool workload cancel returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.FinishResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return workload.FinishResponse{}, err
	}
	return out, nil
}

var errPoolWorkloadNotFound = errors.New("workload not found on peer")

func applyPoolAdmission(worker *poolWorker, admitted workload.AdmissionResponse) {
	worker.Status = string(admitted.Job.Status)
	worker.Queued = admitted.Queued
	worker.InstanceID = admitted.Instance.ID
	worker.LeaseID = admitted.Lease.ID
	if admitted.Result != nil {
		applyPoolResult(worker, admitted.Result)
	}
	worker.Error = admitted.Job.Error
}

func applyPoolStatus(worker *poolWorker, status workload.StatusResponse) {
	worker.Status = string(status.Job.Status)
	worker.Error = status.Job.Error
	worker.Queued = status.Job.Status == domain.JobQueued
	if status.Result != nil {
		applyPoolResult(worker, status.Result)
	}
}

func applyPoolResult(worker *poolWorker, result *domain.JobResult) {
	if result.Instance.ID != "" {
		worker.InstanceID = result.Instance.ID
	}
	if result.LeaseID != "" {
		worker.LeaseID = result.LeaseID
	}
	if result.NodeID != "" {
		worker.NodeID = result.NodeID
	}
}

func waitPoolWorkerHeld(ctx context.Context, client *http.Client, peerURL, rpcToken string, worker *poolWorker) error {
	if poolWorkerHeld(*worker) {
		return nil
	}
	ticker := time.NewTicker(poolStatusPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("worker %s did not become held before admit timeout: %w", worker.WorkloadID, ctx.Err())
		case <-ticker.C:
			status, err := fetchPoolWorkload(ctx, client, peerURL, rpcToken, worker.WorkloadID)
			if err != nil {
				return fmt.Errorf("refresh worker %s while waiting for held lease: %w", worker.WorkloadID, err)
			}
			applyPoolStatus(worker, status)
			if poolWorkerHeld(*worker) {
				return nil
			}
			if poolWorkerTerminal(worker.Status) {
				if worker.Error != "" {
					return fmt.Errorf("worker %s became %s before held lease: %s", worker.WorkloadID, worker.Status, worker.Error)
				}
				return fmt.Errorf("worker %s became %s before held lease", worker.WorkloadID, worker.Status)
			}
		}
	}
}

func poolWorkerTerminal(status string) bool {
	return status == string(domain.JobDone) || status == string(domain.JobFailed) || status == "not_admitted"
}

func poolWorkerHeld(worker poolWorker) bool {
	return worker.Status == string(domain.JobRunning) && worker.LeaseID != "" && worker.InstanceID != ""
}

func countPoolWorkers(state poolState) (held, failed int) {
	for _, worker := range state.Workers {
		switch {
		case worker.Error != "":
			failed++
		case poolWorkerHeld(worker):
			held++
		}
	}
	return held, failed
}

func printPoolPlan(plan pool.Plan) {
	fmt.Printf("pool plan\tmodel=%s\tconcurrency=%d\texisting=%d\tadditional=%d\n",
		plan.Model, plan.Concurrency, plan.TotalExisting, plan.TotalAdditional)
	for _, node := range plan.Nodes {
		if node.Refusal != "" {
			fmt.Printf("node\t%s\texisting=%d\tadditional=0\trefusal=%s\tgap=%s\n",
				node.NodeID, node.Existing, node.Refusal, node.SetupGap)
			continue
		}
		serial := ""
		if node.SerialColdLoads {
			serial = "\tserial-cold-loads"
		}
		fmt.Printf("node\t%s\tpreset=%s\texisting=%d\tadditional=%d\tclaim=%d+%dMB\tcontext=%d%s\n",
			node.NodeID, node.PresetID, node.Existing, node.Additional,
			node.ClaimPerReplica.WeightsMB, node.ClaimPerReplica.KVReservedMB, node.ContextLength, serial)
	}
}

func printPoolState(label string, state poolState) {
	held, failed := countPoolWorkers(state)
	project := ""
	if state.Project != "" {
		project = "\tproject=" + state.Project
	}
	fmt.Printf("%s\tname=%s\tmodel=%s%s\tworkers=%d\theld=%d\tfailed=%d\n",
		label, state.Name, state.Model, project, len(state.Workers), held, failed)
	for _, worker := range state.Workers {
		line := fmt.Sprintf("worker\t%s\tnode=%s\tstatus=%s", worker.WorkloadID, worker.NodeID, worker.Status)
		if worker.LeaseID != "" {
			line += "\tlease=" + worker.LeaseID
		}
		if worker.InstanceID != "" {
			line += "\tinstance=" + worker.InstanceID
		}
		if worker.Queued {
			line += "\tqueued"
		}
		if worker.Error != "" {
			line += "\terror=" + worker.Error
		}
		fmt.Println(line)
	}
}
