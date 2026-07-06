package controlcli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/gateway"
	"mycelium/internal/pool"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/internal/workload"
	"mycelium/test/fixtures"
)

func seedPoolStore(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SaveNode(ctx, fixtures.Make4090Node()); err != nil {
		t.Fatalf("save node: %v", err)
	}
	if err := store.SaveNode(ctx, fixtures.MakeSparkNode()); err != nil {
		t.Fatalf("save spark: %v", err)
	}
	preset := fixtures.MakePreset(func(p *domain.Preset) {
		p.Aliases = []string{"chandra"}
	})
	if err := store.SavePreset(ctx, preset); err != nil {
		t.Fatalf("save preset: %v", err)
	}
	return dbPath
}

func TestPoolPlanWritesPlanFromControlStore(t *testing.T) {
	dbPath := seedPoolStore(t)
	planPath := filepath.Join(t.TempDir(), "plan.json")
	err := RunWithClient(context.Background(), []string{
		"pool", "plan", "--model", "chandra", "--db", dbPath,
		"--max-per-node", "2", "--out", planPath,
	}, http.DefaultClient)
	if err != nil {
		t.Fatalf("pool plan: %v", err)
	}
	data, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	var plan pool.Plan
	if err := json.Unmarshal(data, &plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	if plan.Model != "chandra" || plan.TotalAdditional != 4 || len(plan.Nodes) != 2 {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestOverlayPoolSnapshotsReplacesStaleStoreInstances(t *testing.T) {
	stale := domain.ModelInstance{
		ID:       "stale-b70",
		NodeID:   "b70",
		PresetID: "old-qwen",
		State:    domain.InstReady,
		Claim:    domain.Claim{WeightsMB: 26000},
	}
	other := domain.ModelInstance{
		ID:       "warm-spark",
		NodeID:   "spark",
		PresetID: "spark-chandra",
		State:    domain.InstReady,
		Claim:    domain.Claim{WeightsMB: 56000},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/admission/lease-by-instance" {
			_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{})
			return
		}
		if r.URL.Path != "/snapshot" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer rpc-secret" {
			t.Fatalf("auth = %q", got)
		}
		_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{
			Node:      domain.Node{ID: "b70", Status: domain.NodeReady, MaxUtil: 0.86, Accelerators: []domain.Accelerator{{Index: 0, VRAMTotalMB: 31023}}},
			Instances: nil,
		})
	}))
	defer server.Close()

	nodes, instances, err := overlayPoolSnapshots(context.Background(), server.Client(),
		[]domain.Node{{ID: "b70", Status: domain.NodeReady}, {ID: "spark", Status: domain.NodeReady}},
		[]domain.ModelInstance{stale, other},
		[]string{server.URL}, "rpc-secret")
	if err != nil {
		t.Fatalf("overlay snapshots: %v", err)
	}
	if len(nodes) != 2 || nodes[0].ID != "b70" || nodes[0].MaxUtil != 0.86 {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(instances) != 1 || instances[0].ID != other.ID {
		t.Fatalf("instances = %+v", instances)
	}
}

func TestOverlayPoolSnapshotsAnnotatesAdmissionLeases(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer rpc-secret" {
			t.Fatalf("auth = %q", got)
		}
		switch r.URL.Path {
		case "/snapshot":
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{
				Node: domain.Node{ID: "spark", Status: domain.NodeReady},
				Instances: []domain.ModelInstance{{
					ID:       "inst_2",
					NodeID:   "spark",
					PresetID: "spark-chandra",
					State:    domain.InstReady,
				}},
			})
		case "/admission/lease-by-instance":
			if got := r.URL.Query().Get("instance_id"); got != "inst_2" {
				t.Fatalf("instance_id = %q", got)
			}
			_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{
				Found: true,
				Lease: domain.Lease{ID: "lease-spark-437", InstanceID: "inst_2", ReservationID: "reservation-a"},
			})
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	_, instances, err := overlayPoolSnapshots(context.Background(), server.Client(),
		nil, nil, []string{server.URL}, "rpc-secret")
	if err != nil {
		t.Fatalf("overlay snapshots: %v", err)
	}
	if len(instances) != 1 || instances[0].LeaseID != "lease-spark-437" || instances[0].ReservationID != "reservation-a" {
		t.Fatalf("instances = %+v", instances)
	}
}

func TestPoolConfigHelpers(t *testing.T) {
	urls := poolSnapshotURLsFromConfig(operatorPeerConfig{
		Compute:   true,
		Listen:    "127.0.0.1:9000",
		SeedPeers: []string{"peer-a:9001", "http://peer-a:9001", "https://peer-b:9002"},
	})
	if strings.Join(urls, ",") != "http://peer-a:9001,https://peer-b:9002,http://127.0.0.1:9000" {
		t.Fatalf("urls = %v", urls)
	}

	merged := mergePoolPresets(
		[]domain.Preset{{ID: "same", ContextLength: 1024}, {ID: "store-only", ContextLength: 2048}, {ContextLength: 999}},
		[]domain.Preset{{ID: "same", ContextLength: 49152}, {ID: "config-only", ContextLength: 8192}},
	)
	byID := map[string]domain.Preset{}
	for _, preset := range merged {
		byID[preset.ID] = preset
	}
	if len(merged) != 3 || byID["same"].ContextLength != 49152 || byID["store-only"].ContextLength != 2048 || byID["config-only"].ContextLength != 8192 {
		t.Fatalf("merged = %+v", merged)
	}
}

func TestPoolPlanUsesConfigPresetsAndLiveSnapshots(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "control.db")
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	store.Close()

	snapshotServer := func(node domain.Node) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/admission/lease-by-instance" {
				_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{})
				return
			}
			if r.URL.Path != "/snapshot" {
				t.Fatalf("path = %s", r.URL.Path)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer rpc-secret" {
				t.Fatalf("auth = %q", got)
			}
			_ = json.NewEncoder(w).Encode(domain.NodeSnapshot{Node: node})
		}))
	}
	b70 := snapshotServer(domain.Node{
		ID:          "b70",
		Name:        "b70",
		Status:      domain.NodeReady,
		MaxUtil:     0.86,
		OOMSeverity: domain.OOMSoft,
		Labels:      map[string]string{domain.LabelPeerBackends: string(domain.BackendVLLM)},
		Accelerators: []domain.Accelerator{{
			Index:       0,
			VRAMTotalMB: 32000,
		}},
	})
	defer b70.Close()
	spark := snapshotServer(domain.Node{
		ID:          "spark",
		Name:        "spark",
		Status:      domain.NodeReady,
		MaxUtil:     0.85,
		OOMSeverity: domain.OOMSoft,
		Labels:      map[string]string{domain.LabelPeerBackends: string(domain.BackendVLLM)},
		Accelerators: []domain.Accelerator{{
			Index:       0,
			VRAMTotalMB: 120000,
		}},
	})
	defer spark.Close()

	configPath := filepath.Join(t.TempDir(), "peer.json")
	cfg := operatorPeerConfig{
		ID:        "gateway",
		StorePath: dbPath,
		RPCToken:  "rpc-secret",
		SeedPeers: []string{b70.URL, spark.URL},
		Presets: []domain.Preset{
			{
				ID:            "b70-chandra-ocr2-vllm",
				ModelRef:      "chandra",
				Aliases:       []string{"chandra"},
				Backend:       domain.BackendVLLM,
				ContextLength: 49152,
				Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityVision},
				NodeID:        "b70",
				LaunchArgs:    []string{"--gpu-memory-utilization", "0.50"},
			},
			{
				ID:            "spark-chandra-ocr2-vllm",
				ModelRef:      "chandra",
				Aliases:       []string{"chandra"},
				Backend:       domain.BackendVLLM,
				ContextLength: 49152,
				Capabilities:  []domain.Capability{domain.CapabilityChat, domain.CapabilityVision},
				NodeID:        "spark",
				LaunchArgs:    []string{"--gpu-memory-utilization", "0.50"},
			},
		},
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	planPath := filepath.Join(t.TempDir(), "plan.json")
	err = RunWithClient(context.Background(), []string{
		"pool", "plan",
		"--config", configPath,
		"--live",
		"--model", "chandra",
		"--context", "49152",
		"--max-total", "2",
		"--max-per-node", "1",
		"--out", planPath,
	}, http.DefaultClient)
	if err != nil {
		t.Fatalf("pool plan: %v", err)
	}
	planData, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}
	var plan pool.Plan
	if err := json.Unmarshal(planData, &plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	byNode := map[string]pool.NodePlan{}
	for _, node := range plan.Nodes {
		byNode[node.NodeID] = node
	}
	if byNode["b70"].PresetID != "b70-chandra-ocr2-vllm" || byNode["b70"].Additional != 1 || byNode["b70"].ContextLength != 49152 {
		t.Fatalf("b70 plan = %+v", byNode["b70"])
	}
	if byNode["spark"].PresetID != "spark-chandra-ocr2-vllm" || byNode["spark"].Additional != 1 || byNode["spark"].ContextLength != 49152 {
		t.Fatalf("spark plan = %+v", byNode["spark"])
	}
}

func TestPoolPlanFailsWithoutMatchingPreset(t *testing.T) {
	dbPath := seedPoolStore(t)
	err := RunWithClient(context.Background(), []string{
		"pool", "plan", "--model", "unknown-model", "--db", dbPath,
	}, http.DefaultClient)
	if err == nil || !strings.Contains(err.Error(), "no presets match") {
		t.Fatalf("expected preset resolution error, got %v", err)
	}
}

type poolPeerServer struct {
	mu          sync.Mutex
	admits      []workload.AdmissionRequest
	finishes    []string
	cancels     []string
	statuses    []string
	authSeen    []string
	queueAll    bool
	stayQueued  bool
	rejectAfter int
}

func (s *poolPeerServer) handler(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(gateway.HeaderProject, "pool-ocr")
		_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
	})
	mux.HandleFunc("/admission/lease-by-instance", func(w http.ResponseWriter, r *http.Request) {
		instanceID := r.URL.Query().Get("instance_id")
		if strings.TrimSpace(instanceID) == "" {
			http.Error(w, "missing instance_id", http.StatusBadRequest)
			return
		}
		jobID := strings.TrimPrefix(instanceID, "inst-")
		if jobID == instanceID {
			_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{})
			return
		}
		_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{
			Found: true,
			Lease: domain.Lease{ID: "lease-" + jobID, JobID: jobID, InstanceID: instanceID},
		})
	})
	mux.HandleFunc("/workloads", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.authSeen = append(s.authSeen, r.Header.Get("Authorization"))
		var req workload.AdmissionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode admit: %v", err)
		}
		s.admits = append(s.admits, req)
		job := req.Job
		if s.rejectAfter > 0 && len(s.admits) > s.rejectAfter {
			http.Error(w, `{"error":"warm instance concurrency envelope is full"}`, http.StatusConflict)
			return
		}
		resp := workload.AdmissionResponse{Job: job}
		if s.queueAll {
			job.Status = domain.JobQueued
			resp.Job = job
			resp.Queued = true
			w.WriteHeader(http.StatusAccepted)
		} else {
			job.Status = domain.JobRunning
			resp.Job = job
			resp.Lease = domain.Lease{ID: "lease-" + job.ID}
			resp.Instance = domain.ModelInstance{ID: "inst-" + job.ID, NodeID: job.NodeSelector[domain.LabelNodeID]}
			w.WriteHeader(http.StatusCreated)
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("encode admit response: %v", err)
		}
	})
	mux.HandleFunc("/workloads/", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		id := strings.TrimPrefix(r.URL.Path, "/workloads/")
		switch {
		case strings.HasSuffix(id, "/finish"):
			id = strings.TrimSuffix(id, "/finish")
			s.finishes = append(s.finishes, id)
			job := domain.Job{ID: id, Status: domain.JobDone}
			_ = json.NewEncoder(w).Encode(workload.FinishResponse{Job: job, Result: &domain.JobResult{Terminal: true}})
		case strings.HasSuffix(id, "/cancel"):
			id = strings.TrimSuffix(id, "/cancel")
			s.cancels = append(s.cancels, id)
			job := domain.Job{ID: id, Status: domain.JobFailed}
			_ = json.NewEncoder(w).Encode(workload.FinishResponse{Job: job, Result: &domain.JobResult{Terminal: true}})
		default:
			s.statuses = append(s.statuses, id)
			if s.stayQueued {
				job := domain.Job{ID: id, Status: domain.JobQueued}
				_ = json.NewEncoder(w).Encode(workload.StatusResponse{Job: job})
				return
			}
			job := domain.Job{ID: id, Status: domain.JobRunning}
			result := &domain.JobResult{LeaseID: "lease-" + id, Instance: domain.ModelInstance{ID: "inst-" + id}, NodeID: "node_4090a"}
			_ = json.NewEncoder(w).Encode(workload.StatusResponse{Job: job, Result: result})
		}
	})
	return mux
}

func TestPoolStatusDoesNotReportRunningForInactiveLease(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/workloads/w-stale", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(workload.StatusResponse{
			Job: domain.Job{ID: "w-stale", Status: domain.JobRunning},
			Result: &domain.JobResult{
				LeaseID:  "lease-stale",
				Instance: domain.ModelInstance{ID: "inst-stale"},
				NodeID:   "node_spark",
			},
		})
	})
	mux.HandleFunc("/admission/lease-by-instance", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(poolAdmissionLeaseResponse{})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pool.json")
	if err := writePrettyJSONFile(statePath, poolState{Name: "ocr", Model: "chandra", Workers: []poolWorker{
		{WorkloadID: "w-stale", NodeID: "node_spark", Status: string(domain.JobRunning), InstanceID: "inst-stale", LeaseID: "lease-stale"},
	}}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if err := RunWithClient(context.Background(), []string{
		"pool", "status", "--state", statePath, "--url", ts.URL,
	}, ts.Client()); err != nil {
		t.Fatalf("pool status: %v", err)
	}
	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.Workers[0].Status != "lease_inactive" || !strings.Contains(state.Workers[0].Error, "no active admission lease") {
		t.Fatalf("stale worker should be lease_inactive, got %+v", state.Workers[0])
	}
	if poolWorkerHeld(state.Workers[0]) {
		t.Fatalf("stale worker must not count as held")
	}
}

func TestPoolServeOwnsLifecycleAndReleasesOnStop(t *testing.T) {
	server := &poolPeerServer{}
	ts := httptest.NewServer(server.handler(t))
	defer ts.Close()
	dir := t.TempDir()
	planPath := writePoolPlanFile(t, dir, pool.Plan{
		Model:       "chandra",
		Concurrency: 1,
		Nodes: []pool.NodePlan{{
			NodeID:          "node_4090a",
			PresetID:        "preset_test",
			ContextLength:   8000,
			Additional:      1,
			ClaimPerReplica: domain.Claim{WeightsMB: 5600},
		}},
		TotalAdditional: 1,
	})
	statePath := filepath.Join(dir, "serve-state.json")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunWithClient(ctx, []string{
			"pool", "serve",
			"--plan", planPath,
			"--url", ts.URL,
			"--rpc-token", "rpc-secret",
			"--gateway-token", "gateway-secret",
			"--state", statePath,
			"--name", "ocr",
			"--listen", "127.0.0.1:0",
		}, ts.Client())
	}()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("pool serve exited before cancellation: %v", err)
		case <-deadline:
			t.Fatalf("timed out waiting for pool serve to hold a worker")
		default:
		}
		state, err := loadPoolState(statePath)
		if err == nil && len(state.Workers) == 1 && poolWorkerHeld(state.Workers[0]) {
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("pool serve returned %v", err)
			}
			if len(server.finishes) != 1 || server.finishes[0] != state.Workers[0].WorkloadID {
				t.Fatalf("pool serve did not release held worker: finishes=%v worker=%+v", server.finishes, state.Workers[0])
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writePoolPlanFile(t *testing.T, dir string, plan pool.Plan) string {
	t.Helper()
	path := filepath.Join(dir, "plan.json")
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}
	return path
}

func smallPoolPlan() pool.Plan {
	return pool.Plan{
		Model:           "chandra",
		Concurrency:     2,
		TotalAdditional: 3,
		Nodes: []pool.NodePlan{
			{NodeID: "node_4090a", PresetID: "preset_test", ContextLength: 8000, Additional: 2},
			{NodeID: "node_spark", PresetID: "preset_test", ContextLength: 8000, Additional: 1, SerialColdLoads: true},
			{NodeID: "node_b70", Refusal: "no compatible backend runtime"},
		},
	}
}

func TestPoolUpHoldsWorkersAndWritesState(t *testing.T) {
	server := &poolPeerServer{}
	ts := httptest.NewServer(server.handler(t))
	defer ts.Close()
	dir := t.TempDir()
	planPath := writePoolPlanFile(t, dir, smallPoolPlan())
	statePath := filepath.Join(dir, "pool.json")

	err := RunWithClient(context.Background(), []string{
		"pool", "up", "--plan", planPath, "--url", ts.URL, "--state", statePath,
		"--rpc-token", "secret", "--name", "ocr", "--run-id", "runx",
	}, ts.Client())
	if err != nil {
		t.Fatalf("pool up: %v", err)
	}

	if len(server.admits) != 3 {
		t.Fatalf("admits = %d, want 3", len(server.admits))
	}
	if len(server.finishes) != 0 {
		t.Fatalf("pool up must hold leases, saw finishes %v", server.finishes)
	}
	for _, auth := range server.authSeen {
		if auth != "Bearer secret" {
			t.Fatalf("auth header = %q", auth)
		}
	}
	first := server.admits[0].Job
	if first.NodeSelector[domain.LabelNodeID] != "node_4090a" {
		t.Fatalf("first admit selector = %v", first.NodeSelector)
	}
	if first.Priority != domain.PriorityBackground || first.ExpectedConcurrency != 2 || first.Project != "pool-ocr" {
		t.Fatalf("first admit job = %+v", first)
	}
	if first.ID != "ocr-runx-node_4090a-1" {
		t.Fatalf("first worker id = %q", first.ID)
	}

	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.Workers) != 3 {
		t.Fatalf("state workers = %d", len(state.Workers))
	}
	for _, worker := range state.Workers {
		if worker.LeaseID == "" || worker.Status != string(domain.JobRunning) {
			t.Fatalf("worker not held: %+v", worker)
		}
	}
}

func TestPoolUpWaitsForQueuedColdLoadToBecomeHeld(t *testing.T) {
	oldPoll := poolStatusPollInterval
	poolStatusPollInterval = time.Millisecond
	defer func() { poolStatusPollInterval = oldPoll }()
	server := &poolPeerServer{queueAll: true}
	ts := httptest.NewServer(server.handler(t))
	defer ts.Close()
	dir := t.TempDir()
	planPath := writePoolPlanFile(t, dir, pool.Plan{
		Model:           "chandra",
		Concurrency:     1,
		TotalAdditional: 1,
		Nodes:           []pool.NodePlan{{NodeID: "node_spark", PresetID: "preset_test", ContextLength: 49152, Additional: 1}},
	})
	statePath := filepath.Join(dir, "pool.json")

	err := RunWithClient(context.Background(), []string{
		"pool", "up", "--plan", planPath, "--url", ts.URL, "--state", statePath,
		"--run-id", "runcold", "--admit-timeout", "1s",
	}, ts.Client())
	if err != nil {
		t.Fatalf("pool up should wait for held lease: %v", err)
	}
	if len(server.statuses) == 0 {
		t.Fatalf("pool up did not refresh queued worker status")
	}
	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(state.Workers) != 1 || !poolWorkerHeld(state.Workers[0]) || state.Workers[0].Queued {
		t.Fatalf("queued worker was not refreshed to held lease: %+v", state.Workers)
	}
}

func TestPoolUpReportsWhenNothingHeld(t *testing.T) {
	oldPoll := poolStatusPollInterval
	poolStatusPollInterval = time.Millisecond
	defer func() { poolStatusPollInterval = oldPoll }()
	server := &poolPeerServer{queueAll: true, stayQueued: true}
	ts := httptest.NewServer(server.handler(t))
	defer ts.Close()
	dir := t.TempDir()
	planPath := writePoolPlanFile(t, dir, smallPoolPlan())
	statePath := filepath.Join(dir, "pool.json")

	err := RunWithClient(context.Background(), []string{
		"pool", "up", "--plan", planPath, "--url", ts.URL, "--state", statePath, "--run-id", "runq", "--admit-timeout", "10ms",
	}, ts.Client())
	if err == nil || !strings.Contains(err.Error(), "held no workers") {
		t.Fatalf("expected held-no-workers error, got %v", err)
	}
	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("state must still be written: %v", err)
	}
	for _, worker := range state.Workers {
		if !worker.Queued || worker.Error == "" {
			t.Fatalf("worker should be queued: %+v", worker)
		}
	}
}

func TestPoolUpReportsPartialHeldPool(t *testing.T) {
	server := &poolPeerServer{rejectAfter: 1}
	ts := httptest.NewServer(server.handler(t))
	defer ts.Close()
	dir := t.TempDir()
	planPath := writePoolPlanFile(t, dir, pool.Plan{
		Model:           "chandra",
		Concurrency:     1,
		TotalAdditional: 2,
		Nodes:           []pool.NodePlan{{NodeID: "node_spark", PresetID: "preset_test", ContextLength: 49152, Additional: 2}},
	})
	statePath := filepath.Join(dir, "pool.json")

	err := RunWithClient(context.Background(), []string{
		"pool", "up", "--plan", planPath, "--url", ts.URL, "--state", statePath,
		"--run-id", "runpartial", "--admit-timeout", "1s",
	}, ts.Client())
	if err == nil || !strings.Contains(err.Error(), "held 1/2 workers") {
		t.Fatalf("expected partial-held error, got %v", err)
	}
	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("state must still be written: %v", err)
	}
	if len(state.Workers) != 2 || !poolWorkerHeld(state.Workers[0]) || state.Workers[1].Status != "admit_error" || !strings.Contains(state.Workers[1].Error, "warm instance concurrency envelope is full") {
		t.Fatalf("partial state = %+v", state.Workers)
	}
}

func TestPoolStatusAndDownLifecycle(t *testing.T) {
	server := &poolPeerServer{}
	ts := httptest.NewServer(server.handler(t))
	defer ts.Close()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pool.json")
	initial := poolState{Name: "ocr", Model: "chandra", Workers: []poolWorker{
		{WorkloadID: "w1", NodeID: "node_4090a", LeaseID: "lease-w1", Status: string(domain.JobRunning)},
		{WorkloadID: "w2", NodeID: "node_spark", Status: string(domain.JobQueued), Queued: true},
		{WorkloadID: "w3", NodeID: "node_spark", Status: string(domain.JobDone)},
	}}
	if err := writePrettyJSONFile(statePath, initial); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	if err := RunWithClient(context.Background(), []string{
		"pool", "status", "--state", statePath, "--url", ts.URL,
	}, ts.Client()); err != nil {
		t.Fatalf("pool status: %v", err)
	}
	if len(server.statuses) != 3 {
		t.Fatalf("status fetches = %v", server.statuses)
	}
	refreshed, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load refreshed state: %v", err)
	}
	if refreshed.Workers[1].Status != string(domain.JobRunning) || refreshed.Workers[1].LeaseID != "lease-w2" {
		t.Fatalf("status refresh did not update worker: %+v", refreshed.Workers[1])
	}

	// Reset to the seeded mix so down exercises finish, cancel, and skip.
	if err := writePrettyJSONFile(statePath, initial); err != nil {
		t.Fatalf("reseed state: %v", err)
	}
	if err := RunWithClient(context.Background(), []string{
		"pool", "down", "--state", statePath, "--url", ts.URL,
	}, ts.Client()); err != nil {
		t.Fatalf("pool down: %v", err)
	}
	if len(server.finishes) != 2 || server.finishes[0] != "w1" || server.finishes[1] != "w2" {
		t.Fatalf("finishes = %v", server.finishes)
	}
	if len(server.cancels) != 1 || server.cancels[0] != "w2" {
		t.Fatalf("cancels = %v", server.cancels)
	}
	final, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	if final.Workers[0].Status != string(domain.JobDone) {
		t.Fatalf("finished worker status = %q", final.Workers[0].Status)
	}
	if final.Workers[1].Status != string(domain.JobDone) {
		t.Fatalf("raced worker status = %q", final.Workers[1].Status)
	}
}

func TestPoolDownRefreshesAmbiguousWorkerBeforeRelease(t *testing.T) {
	server := &poolPeerServer{}
	ts := httptest.NewServer(server.handler(t))
	defer ts.Close()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pool.json")
	initial := poolState{Name: "ocr", Model: "chandra", Workers: []poolWorker{
		{WorkloadID: "w-stale", NodeID: "node_spark", Status: "admit_error", Error: "warm instance concurrency envelope is full"},
	}}
	if err := writePrettyJSONFile(statePath, initial); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	if err := RunWithClient(context.Background(), []string{
		"pool", "down", "--state", statePath, "--url", ts.URL,
	}, ts.Client()); err != nil {
		t.Fatalf("pool down: %v", err)
	}
	if len(server.statuses) != 1 || server.statuses[0] != "w-stale" {
		t.Fatalf("statuses = %v", server.statuses)
	}
	if len(server.finishes) != 1 || server.finishes[0] != "w-stale" {
		t.Fatalf("finishes = %v", server.finishes)
	}
	if len(server.cancels) != 0 {
		t.Fatalf("cancels = %v", server.cancels)
	}
	final, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	if final.Workers[0].Status != string(domain.JobDone) || final.Workers[0].Error != "" {
		t.Fatalf("final worker = %+v", final.Workers[0])
	}
	if final.Workers[0].LeaseID != "lease-w-stale" || final.Workers[0].InstanceID != "inst-w-stale" {
		t.Fatalf("release evidence was not preserved: %+v", final.Workers[0])
	}
}

func TestPoolDownReleasesWorkerThatBecomesHeldAfterCancel(t *testing.T) {
	oldPoll := poolStatusPollInterval
	oldVerify := poolDownVerifyTimeout
	poolStatusPollInterval = time.Millisecond
	poolDownVerifyTimeout = 50 * time.Millisecond
	defer func() {
		poolStatusPollInterval = oldPoll
		poolDownVerifyTimeout = oldVerify
	}()
	var mu sync.Mutex
	var cancels, finishes, statuses int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/workloads/w-race/cancel":
			cancels++
			_ = json.NewEncoder(w).Encode(workload.FinishResponse{Job: domain.Job{ID: "w-race", Status: domain.JobFailed}})
		case r.URL.Path == "/workloads/w-race/finish":
			finishes++
			_ = json.NewEncoder(w).Encode(workload.FinishResponse{
				Job:    domain.Job{ID: "w-race", Status: domain.JobDone},
				Result: &domain.JobResult{LeaseID: "lease-race", Instance: domain.ModelInstance{ID: "inst-race"}, NodeID: "node_spark", Terminal: true},
			})
		case r.URL.Path == "/workloads/w-race":
			statuses++
			if statuses == 1 {
				_ = json.NewEncoder(w).Encode(workload.StatusResponse{Job: domain.Job{ID: "w-race", Status: domain.JobFailed}})
				return
			}
			_ = json.NewEncoder(w).Encode(workload.StatusResponse{
				Job:    domain.Job{ID: "w-race", Status: domain.JobRunning},
				Result: &domain.JobResult{LeaseID: "lease-race", Instance: domain.ModelInstance{ID: "inst-race"}, NodeID: "node_spark"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pool.json")
	initial := poolState{Name: "ocr", Model: "chandra", Workers: []poolWorker{
		{WorkloadID: "w-race", NodeID: "node_spark", Status: string(domain.JobQueued), Queued: true},
	}}
	if err := writePrettyJSONFile(statePath, initial); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	if err := RunWithClient(context.Background(), []string{
		"pool", "down", "--state", statePath, "--url", ts.URL,
	}, ts.Client()); err != nil {
		t.Fatalf("pool down: %v", err)
	}
	if cancels != 1 || finishes != 1 || statuses < 2 {
		t.Fatalf("cancels=%d finishes=%d statuses=%d", cancels, finishes, statuses)
	}
	final, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	if final.Workers[0].Status != string(domain.JobDone) || final.Workers[0].LeaseID != "lease-race" || final.Workers[0].InstanceID != "inst-race" {
		t.Fatalf("final worker = %+v", final.Workers[0])
	}
}

func TestPoolUsageErrors(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.json")
	cases := [][]string{
		{"pool"},
		{"pool", "bogus"},
		{"pool", "plan"},
		{"pool", "up"},
		{"pool", "up", "--plan", missing, "--url", "http://x", "--state", filepath.Join(dir, "s.json")},
		{"pool", "status"},
		{"pool", "status", "--state", missing, "--url", "http://x"},
		{"pool", "down"},
		{"pool", "down", "--state", missing, "--url", "http://x"},
	}
	for _, args := range cases {
		if err := RunWithClient(context.Background(), args, http.DefaultClient); err == nil {
			t.Fatalf("expected error for %v", args)
		}
	}
}

func TestPoolJSONOutputsAndErrorBranches(t *testing.T) {
	dbPath := seedPoolStore(t)
	dir := t.TempDir()

	// plan --json prints and matches by preset id and model ref too.
	if err := RunWithClient(context.Background(), []string{
		"pool", "plan", "--model", "preset_test", "--db", dbPath, "--json",
	}, http.DefaultClient); err != nil {
		t.Fatalf("pool plan --json: %v", err)
	}
	if err := RunWithClient(context.Background(), []string{
		"pool", "plan", "--model", "qwen2.5-9b-instruct", "--db", dbPath,
	}, http.DefaultClient); err != nil {
		t.Fatalf("pool plan by model ref: %v", err)
	}
	// plan with an unreadable store path fails loudly.
	if err := RunWithClient(context.Background(), []string{
		"pool", "plan", "--model", "chandra", "--db", filepath.Join(dir, "missing-dir", "x.db"),
	}, http.DefaultClient); err == nil {
		t.Fatalf("expected store open error")
	}

	// Bad plan/state payloads fail with decode errors.
	badJSON := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badJSON, []byte("{"), 0o644); err != nil {
		t.Fatalf("write bad json: %v", err)
	}
	if _, err := loadPoolPlan(badJSON); err == nil || !strings.Contains(err.Error(), "decode pool plan") {
		t.Fatalf("bad plan error = %v", err)
	}
	if _, err := loadPoolState(badJSON); err == nil || !strings.Contains(err.Error(), "decode pool state") {
		t.Fatalf("bad state error = %v", err)
	}
	empty := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(empty, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write empty plan: %v", err)
	}
	if _, err := loadPoolPlan(empty); err == nil || !strings.Contains(err.Error(), "no model") {
		t.Fatalf("empty plan error = %v", err)
	}
}

func TestPoolUpJSONAndAdmitFailures(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer failing.Close()
	dir := t.TempDir()
	planPath := writePoolPlanFile(t, dir, smallPoolPlan())
	statePath := filepath.Join(dir, "pool.json")
	err := RunWithClient(context.Background(), []string{
		"pool", "up", "--plan", planPath, "--url", failing.URL, "--state", statePath, "--run-id", "runf", "--json",
	}, failing.Client())
	if err == nil || !strings.Contains(err.Error(), "held no workers") {
		t.Fatalf("expected admit failures, got %v", err)
	}
	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	for _, worker := range state.Workers {
		if worker.Status != "admit_error" || worker.Error == "" {
			t.Fatalf("worker should record admit error: %+v", worker)
		}
	}
}

func TestPoolStatusAndDownJSONAndServerErrors(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer failing.Close()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pool.json")
	seed := poolState{Name: "ocr", Model: "chandra", Workers: []poolWorker{
		{WorkloadID: "w1", NodeID: "node_4090a", LeaseID: "lease-w1", Status: string(domain.JobRunning)},
		{WorkloadID: "w2", NodeID: "node_spark", Status: string(domain.JobQueued), Queued: true},
	}}
	if err := writePrettyJSONFile(statePath, seed); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	// status --json against a failing peer records per-worker errors but succeeds.
	if err := RunWithClient(context.Background(), []string{
		"pool", "status", "--state", statePath, "--url", failing.URL, "--json",
	}, failing.Client()); err != nil {
		t.Fatalf("pool status with failing peer: %v", err)
	}
	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	for _, worker := range state.Workers {
		if worker.Error == "" {
			t.Fatalf("status should record fetch error: %+v", worker)
		}
	}

	// down --json against a failing peer returns joined release errors.
	if err := writePrettyJSONFile(statePath, seed); err != nil {
		t.Fatalf("reseed state: %v", err)
	}
	err = RunWithClient(context.Background(), []string{
		"pool", "down", "--state", statePath, "--url", failing.URL, "--json",
	}, failing.Client())
	if err == nil || !strings.Contains(err.Error(), "w1") || !strings.Contains(err.Error(), "w2") {
		t.Fatalf("down should join release errors, got %v", err)
	}
}

func TestPoolUpTimeoutLeavesCleanupCapableState(t *testing.T) {
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a long cold load: block until the test releases handlers.
		// (A handler that has not read the request body cannot observe the
		// client disconnect, so waiting on r.Context() would deadlock Close.)
		<-release
	}))
	defer slow.Close()
	defer close(release) // runs before slow.Close, unblocking handlers first
	dir := t.TempDir()
	planPath := writePoolPlanFile(t, dir, smallPoolPlan())
	statePath := filepath.Join(dir, "pool.json")

	err := RunWithClient(context.Background(), []string{
		"pool", "up", "--plan", planPath, "--url", slow.URL, "--state", statePath,
		"--run-id", "runt", "--admit-timeout", "50ms",
	}, slow.Client())
	if err == nil || !strings.Contains(err.Error(), "held no workers") {
		t.Fatalf("expected held-no-workers error, got %v", err)
	}
	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("state must exist after timeouts: %v", err)
	}
	if len(state.Workers) != 3 {
		t.Fatalf("state workers = %d, want all 3 pre-registered", len(state.Workers))
	}
	for _, worker := range state.Workers {
		if worker.Status != "admit_error" || !strings.Contains(worker.Error, "context deadline exceeded") {
			t.Fatalf("timed-out worker = %+v", worker)
		}
		if worker.WorkloadID == "" {
			t.Fatalf("worker must keep its pre-assigned workload id for cleanup")
		}
	}
}

func TestPoolStatusAndDownTolerateNeverAdmittedWorkers(t *testing.T) {
	notFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "job not found", http.StatusNotFound)
	}))
	defer notFound.Close()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "pool.json")
	seed := poolState{Name: "ocr", Model: "chandra", Workers: []poolWorker{
		{WorkloadID: "w1", NodeID: "node_spark", Status: "pending"},
	}}
	if err := writePrettyJSONFile(statePath, seed); err != nil {
		t.Fatalf("seed state: %v", err)
	}

	if err := RunWithClient(context.Background(), []string{
		"pool", "status", "--state", statePath, "--url", notFound.URL,
	}, notFound.Client()); err != nil {
		t.Fatalf("pool status: %v", err)
	}
	state, err := loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if state.Workers[0].Status != "not_admitted" || state.Workers[0].Error != "" {
		t.Fatalf("status should classify missing workload: %+v", state.Workers[0])
	}

	if err := writePrettyJSONFile(statePath, seed); err != nil {
		t.Fatalf("reseed state: %v", err)
	}
	if err := RunWithClient(context.Background(), []string{
		"pool", "down", "--state", statePath, "--url", notFound.URL,
	}, notFound.Client()); err != nil {
		t.Fatalf("pool down must tolerate never-admitted workers: %v", err)
	}
	state, err = loadPoolState(statePath)
	if err != nil {
		t.Fatalf("load state after down: %v", err)
	}
	if state.Workers[0].Status != "not_admitted" || state.Workers[0].Error != "" {
		t.Fatalf("down should classify missing workload: %+v", state.Workers[0])
	}
}

func TestPoolUpProjectOverride(t *testing.T) {
	server := &poolPeerServer{}
	ts := httptest.NewServer(server.handler(t))
	defer ts.Close()
	dir := t.TempDir()
	planPath := writePoolPlanFile(t, dir, smallPoolPlan())
	statePath := filepath.Join(dir, "pool.json")
	if err := RunWithClient(context.Background(), []string{
		"pool", "up", "--plan", planPath, "--url", ts.URL, "--state", statePath,
		"--name", "ocr", "--project", "atlas", "--run-id", "runp",
	}, ts.Client()); err != nil {
		t.Fatalf("pool up with project override: %v", err)
	}
	for _, admit := range server.admits {
		if admit.Job.Project != "atlas" {
			t.Fatalf("worker project = %q, want atlas", admit.Job.Project)
		}
	}
}
