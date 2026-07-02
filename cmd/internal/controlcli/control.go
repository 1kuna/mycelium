package controlcli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/bench"
	"mycelium/internal/catalog"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/estimate"
	"mycelium/internal/lease"
	"mycelium/internal/locality"
	"mycelium/internal/modelruntime"
	"mycelium/internal/optimizer"
	"mycelium/internal/ports"
	projectvalidation "mycelium/internal/project"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/internal/workload"
	"mycelium/pkg/api"
)

const maxControlHTTPBodyBytes int64 = 16 << 20

func Run(ctx context.Context, args []string) error {
	return RunWithClient(ctx, args, http.DefaultClient)
}

func RunWithClient(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce <add-model|models|nodes|projects|jobs|telemetry|recommendations|benchmark|calibrate|gateway|route|doctor|debug|status>")
	}
	switch args[0] {
	case "add-model":
		return runAddModel(ctx, args[1:])
	case "models":
		return runModels(ctx, args[1:], client)
	case "nodes":
		return runNodes(ctx, args[1:], client)
	case "projects":
		return runProjects(ctx, args[1:])
	case "jobs":
		return runJobs(ctx, args[1:], client)
	case "telemetry":
		return runTelemetry(ctx, args[1:])
	case "recommendations":
		return runRecommendations(ctx, args[1:])
	case "benchmark":
		return runBenchmark(ctx, args[1:], client)
	case "calibrate":
		return runCalibrate(ctx, args[1:], client)
	case "gateway":
		return runGateway(ctx, args[1:], client)
	case "route":
		return runRoute(ctx, args[1:], client)
	case "doctor":
		return runDoctor(ctx, args[1:], client)
	case "debug":
		return runDebug(ctx, args[1:], client)
	case "status":
		return runStatus(ctx, args[1:], client)
	default:
		return fmt.Errorf("unknown myce command %q", args[0])
	}
}

func runAddModel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("add-model", flag.ContinueOnError)
	store := fs.String("store", defaultCatalogStore(), "catalog store directory")
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	id := fs.String("id", "", "preset id")
	model := fs.String("model", "", "logical model name")
	contextLen := fs.Int("context", 2048, "preset context length")
	quant := fs.String("quant", "unknown", "preset quantization label")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: myce add-model [flags] <source>")
	}
	req := catalog.InstallRequest{
		Source:        fs.Arg(0),
		ID:            *id,
		Model:         *model,
		ContextLength: *contextLen,
		Quant:         *quant,
		Backend:       domain.BackendLlamaCpp,
	}
	control, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer control.Close()
	jobID, err := catalog.InstallJobID(req)
	if err != nil {
		return err
	}
	job := domain.Job{
		ID:       jobID,
		TaskType: "catalog_install",
		Model:    req.Source,
		PresetID: req.ID,
		Status:   domain.JobQueued,
	}
	if err := control.SaveJob(ctx, job); err != nil {
		return err
	}
	fmt.Printf("job\t%s\tstarted\n", job.ID)
	result, err := catalog.NewInstaller(*store).InstallWithProgress(ctx, req, func(event catalog.ProgressEvent, state catalog.InstallState) error {
		job.Status = installStageStatus(event.Stage)
		job.PresetID = state.PresetID
		job.Progress = append(job.Progress, domain.JobProgress{Stage: event.Stage, Message: event.Message, At: event.At})
		if err := control.SaveJob(ctx, job); err != nil {
			return err
		}
		fmt.Printf("job\t%s\t%s\t%s\n", job.ID, event.Stage, event.Message)
		return nil
	})
	if err != nil {
		job.Status = domain.JobFailed
		job.Error = err.Error()
		_ = control.SaveJob(ctx, job)
		return err
	}
	if err := control.SavePreset(ctx, result.Preset); err != nil {
		job.Status = domain.JobFailed
		job.Error = err.Error()
		_ = control.SaveJob(ctx, job)
		return err
	}
	job.Status = domain.JobDone
	job.PresetID = result.Preset.ID
	if err := control.SaveJob(ctx, job); err != nil {
		return err
	}
	fmt.Printf("preset\t%s\t%s\n", result.Preset.ID, result.Preset.ModelRef)
	return nil
}

func runModels(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce models <list|stage|locality|prewarm|scale>")
	}
	switch args[0] {
	case "list":
		store, err := openCLIStore(args[1:])
		if err != nil {
			return err
		}
		defer store.Close()
		presets, err := store.ListPresets(ctx)
		if err != nil {
			return err
		}
		for _, preset := range presets {
			fmt.Printf("%s\t%s\t%s\t%d\n", preset.ID, preset.ModelRef, preset.Backend, preset.ContextLength)
		}
		return nil
	case "stage":
		return runModelsStage(ctx, args[1:], client)
	case "locality":
		return runModelsLocality(ctx, args[1:])
	case "prewarm", "scale":
		return runModelsPrewarm(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce models <list|stage|locality|prewarm|scale>")
	}
}

type modelPrewarmResult struct {
	JobID    string                 `json:"job_id"`
	Model    string                 `json:"model,omitempty"`
	PresetID string                 `json:"preset_id,omitempty"`
	Status   domain.JobStatus       `json:"status"`
	Action   domain.PlacementAction `json:"action,omitempty"`
	NodeID   string                 `json:"node_id,omitempty"`
	Instance string                 `json:"instance_id,omitempty"`
	LeaseID  string                 `json:"lease_id,omitempty"`
	Finished bool                   `json:"finished"`
	Terminal bool                   `json:"terminal"`
	Error    string                 `json:"error,omitempty"`
}

type modelPrewarmFit struct {
	NodeID         string         `json:"node_id"`
	Address        string         `json:"address,omitempty"`
	Backend        domain.Backend `json:"backend,omitempty"`
	AcceleratorSet []int          `json:"accelerator_set,omitempty"`
	Claim          domain.Claim   `json:"claim"`
	Status         string         `json:"status"`
	Reason         string         `json:"reason,omitempty"`
}

type modelPrewarmReport struct {
	Model             string               `json:"model,omitempty"`
	PresetID          string               `json:"preset_id,omitempty"`
	RequestedReplicas int                  `json:"requested_replicas"`
	DesiredReplicas   int                  `json:"desired_replicas"`
	Warmed            int                  `json:"warmed"`
	Queued            int                  `json:"queued"`
	UniqueInstances   int                  `json:"unique_instances"`
	FitCandidates     []modelPrewarmFit    `json:"fit_candidates,omitempty"`
	Results           []modelPrewarmResult `json:"results"`
}

func runModelsPrewarm(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("models prewarm", flag.ContinueOnError)
	configPath := fs.String("config", defaultPeerConfigPath(), "peer config JSON for preset and live peer discovery")
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	model := fs.String("model", "", "model or alias to prewarm")
	presetID := fs.String("preset", "", "preset id to prewarm")
	project := fs.String("project", "operator-prewarm", "project label for prewarm evidence")
	contextCap := fs.Int("context", 0, "context request for admission evidence")
	concurrency := fs.Int("concurrency", 1, "expected concurrency envelope")
	var replicas optionalReplicasFlag
	fs.Var(&replicas, "replicas", "number of distinct owner replicas to prewarm; defaults to one, or every fitting owner with --all-fitting")
	allFitting := fs.Bool("all-fitting", false, "target every live owner that can fit the preset")
	priority := fs.String("priority", string(domain.PriorityBackground), "priority: interactive, normal, or background")
	jsonOut := fs.Bool("json", false, "print JSON report")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerURL == "" {
		return fmt.Errorf("--url is required")
	}
	if *model == "" && *presetID == "" {
		return fmt.Errorf("--model or --preset is required")
	}
	if replicas.set && replicas.value <= 0 {
		return fmt.Errorf("--replicas must be positive")
	}
	if *concurrency <= 0 {
		return fmt.Errorf("--concurrency must be positive")
	}
	parsedPriority, err := parsePriorityFlag(*priority)
	if err != nil {
		return err
	}
	if client == nil {
		client = http.DefaultClient
	}
	loaded, err := loadOperatorConfig(*configPath)
	if err != nil && *allFitting {
		return err
	}
	resolvedRPCToken := strings.TrimSpace(*rpcToken)
	if resolvedRPCToken == "" && err == nil {
		resolvedRPCToken = strings.TrimSpace(loaded.Config.RPCToken)
	}
	requestedReplicas := 1
	if replicas.set {
		requestedReplicas = replicas.value
	}
	report := modelPrewarmReport{
		Model:             strings.TrimSpace(*model),
		PresetID:          strings.TrimSpace(*presetID),
		RequestedReplicas: requestedReplicas,
		Results:           make([]modelPrewarmResult, 0, requestedReplicas),
	}
	targets := []modelPrewarmFit{{Status: "untargeted_scheduler_admission"}}
	if *allFitting {
		preset, err := resolveOperatorPreset(loaded.Config.Presets, report.Model, report.PresetID)
		if err != nil {
			return err
		}
		if report.Model == "" {
			report.Model = preferredPresetModel(preset)
		}
		if report.PresetID == "" {
			report.PresetID = preset.ID
		}
		if *contextCap == 0 {
			*contextCap = modelruntime.EffectiveContextLength(preset)
		}
		fleet, err := liveFleetSnapshotForPrewarm(ctx, loaded.Config, resolvedRPCToken, client)
		if err != nil {
			return err
		}
		candidates := prewarmFitCandidates(ctx, preset, *contextCap, *concurrency, fleet)
		report.FitCandidates = candidates
		targets = targets[:0]
		limit := len(candidates)
		if replicas.set {
			limit = replicas.value
		}
		for _, candidate := range candidates {
			if candidate.Status != "fits" {
				continue
			}
			targets = append(targets, candidate)
			if len(targets) == limit {
				break
			}
		}
		report.RequestedReplicas = len(targets)
		report.DesiredReplicas = len(targets)
		if len(targets) == 0 {
			if *jsonOut {
				return json.NewEncoder(os.Stdout).Encode(report)
			}
			printModelPrewarmReport(report)
			return nil
		}
	} else {
		targets = make([]modelPrewarmFit, 0, requestedReplicas)
		for i := 0; i < requestedReplicas; i++ {
			targets = append(targets, modelPrewarmFit{Status: "untargeted_scheduler_admission"})
		}
		report.DesiredReplicas = requestedReplicas
	}
	seenInstances := map[string]struct{}{}
	for i, target := range targets {
		job := domain.Job{
			ID:                  prewarmJobID(report.Model, report.PresetID, i),
			TaskType:            string(domain.CapabilityChat),
			Model:               report.Model,
			PresetID:            report.PresetID,
			Project:             strings.TrimSpace(*project),
			Priority:            parsedPriority,
			SpeedPref:           domain.SpeedThroughput,
			ContextRequest:      *contextCap,
			ExpectedConcurrency: *concurrency,
			Preemption:          domain.PreemptSoft,
			Status:              domain.JobQueued,
		}
		if target.NodeID != "" {
			job.NodeSelector = map[string]string{domain.LabelNodeID: target.NodeID}
		}
		admitted, err := admitPrewarmWorkload(ctx, client, *peerURL, resolvedRPCToken, job)
		if err != nil {
			return err
		}
		result := prewarmResultFromAdmission(admitted)
		if result.Instance != "" {
			seenInstances[result.Instance] = struct{}{}
		}
		if admitted.Job.Status == domain.JobRunning {
			finished, err := finishPrewarmWorkload(ctx, client, *peerURL, resolvedRPCToken, admitted.Job.ID)
			if err != nil {
				return err
			}
			result = prewarmResultFromFinish(finished, result)
		}
		switch result.Status {
		case domain.JobDone:
			report.Warmed++
		case domain.JobQueued:
			report.Queued++
		}
		report.Results = append(report.Results, result)
	}
	report.UniqueInstances = len(seenInstances)
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(report)
	}
	printModelPrewarmReport(report)
	return nil
}

func resolveOperatorPreset(presets []domain.Preset, model, presetID string) (domain.Preset, error) {
	for _, preset := range presets {
		if presetID != "" && preset.ID == presetID {
			return preset, nil
		}
		if model == "" {
			continue
		}
		if preset.ID == model || preset.ModelRef == model {
			return preset, nil
		}
		for _, alias := range preset.Aliases {
			if alias == model {
				return preset, nil
			}
		}
	}
	if presetID != "" {
		return domain.Preset{}, fmt.Errorf("unknown preset %q", presetID)
	}
	return domain.Preset{}, fmt.Errorf("unknown model %q", model)
}

func liveFleetSnapshotForPrewarm(ctx context.Context, cfg operatorPeerConfig, rpcToken string, client *http.Client) (domain.FleetSnapshot, error) {
	urls := append([]string(nil), cfg.SeedPeers...)
	if cfg.Compute && cfg.Listen != "" {
		urls = append(urls, cfg.Listen)
	}
	if len(urls) == 0 {
		return domain.FleetSnapshot{}, fmt.Errorf("no live peer URLs in config; supply a config with seed peers")
	}
	seen := map[string]struct{}{}
	var fleet domain.FleetSnapshot
	var errs []error
	for _, raw := range urls {
		base := strings.TrimRight(catalogPeerBaseURL(raw), "/")
		if _, ok := seen[base]; ok {
			continue
		}
		seen[base] = struct{}{}
		snap, err := fetchPrewarmSnapshot(ctx, base, rpcToken, client)
		if err != nil {
			errs = append(errs, fmt.Errorf("snapshot %s: %w", base, err))
			continue
		}
		fleet.Nodes = append(fleet.Nodes, snap.Node)
		fleet.Instances = append(fleet.Instances, snap.Instances...)
	}
	if len(fleet.Nodes) == 0 {
		return domain.FleetSnapshot{}, errors.Join(errs...)
	}
	return fleet, nil
}

func fetchPrewarmSnapshot(ctx context.Context, baseURL, rpcToken string, client *http.Client) (domain.NodeSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/snapshot", nil)
	if err != nil {
		return domain.NodeSnapshot{}, err
	}
	if rpcToken != "" {
		req.Header.Set("Authorization", "Bearer "+rpcToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return domain.NodeSnapshot{}, err
	}
	defer resp.Body.Close()
	body, err := readControlHTTPBody(resp.Body, "snapshot response body")
	if err != nil {
		return domain.NodeSnapshot{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return domain.NodeSnapshot{}, fmt.Errorf("returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var snap domain.NodeSnapshot
	if err := json.Unmarshal(body, &snap); err != nil {
		return domain.NodeSnapshot{}, err
	}
	if snap.Node.ID == "" {
		return domain.NodeSnapshot{}, fmt.Errorf("snapshot returned empty node id")
	}
	return snap, nil
}

func prewarmFitCandidates(ctx context.Context, preset domain.Preset, contextLen, concurrency int, fleet domain.FleetSnapshot) []modelPrewarmFit {
	if contextLen <= 0 {
		contextLen = modelruntime.EffectiveContextLength(preset)
	}
	alloc := lease.NewAllocator()
	estimator := estimate.NewInMemory()
	out := make([]modelPrewarmFit, 0, len(fleet.Nodes))
	for _, node := range fleet.Nodes {
		fit := modelPrewarmFit{NodeID: node.ID, Address: node.Address, Backend: preset.Backend, Status: "not_fit"}
		if node.Status != domain.NodeReady {
			fit.Reason = string(node.Status)
			out = append(out, fit)
			continue
		}
		if reason, ok := presetNodeMismatchForPrewarm(preset, node); ok {
			fit.Reason = reason
			out = append(out, fit)
			continue
		}
		if reason, ok := presetBackendMismatchForPrewarm(preset, node); ok {
			fit.Reason = reason
			out = append(out, fit)
			continue
		}
		if reason, ok := presetDiskMismatchForPrewarm(preset, node, fleet); ok {
			fit.Reason = reason
			out = append(out, fit)
			continue
		}
		sets := prewarmAcceleratorSets(node)
		if len(sets) == 0 {
			fit.Reason = "accelerators"
			out = append(out, fit)
			continue
		}
		for _, acc := range sets {
			claim, err := estimator.Estimate(ctx, preset, contextLen, concurrency)
			if err != nil {
				fit.Reason = err.Error()
				break
			}
			if !alloc.CanStackLoad(node, acc, fleet.Instances) {
				fit.Reason = "load_in_flight"
				continue
			}
			if alloc.Fits(node, acc, fleet.Instances, claim) {
				fit.Status = "fits"
				fit.Reason = ""
				fit.AcceleratorSet = append([]int(nil), acc...)
				fit.Claim = claim
				break
			}
			fit.Reason = "fit"
		}
		out = append(out, fit)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func prewarmAcceleratorSets(node domain.Node) [][]int {
	accs := make([]int, 0, len(node.Accelerators))
	for _, acc := range node.Accelerators {
		accs = append(accs, acc.Index)
	}
	sort.Ints(accs)
	sets := make([][]int, 0)
	for mask := 1; mask < 1<<len(accs); mask++ {
		set := make([]int, 0, len(accs))
		for i, acc := range accs {
			if mask&(1<<i) != 0 {
				set = append(set, acc)
			}
		}
		sets = append(sets, set)
	}
	sort.Slice(sets, func(i, j int) bool {
		if len(sets[i]) != len(sets[j]) {
			return len(sets[i]) < len(sets[j])
		}
		for k := range sets[i] {
			if sets[i][k] != sets[j][k] {
				return sets[i][k] < sets[j][k]
			}
		}
		return false
	})
	return sets
}

func presetNodeMismatchForPrewarm(preset domain.Preset, node domain.Node) (string, bool) {
	if preset.NodeID != "" && preset.NodeID != node.ID {
		return "preset.node_id", true
	}
	return "", false
}

func presetBackendMismatchForPrewarm(preset domain.Preset, node domain.Node) (string, bool) {
	if preset.Backend == "" {
		return "", false
	}
	if backends := node.Labels[domain.LabelPeerBackends]; backends != "" {
		for _, backend := range strings.Split(backends, ",") {
			if strings.TrimSpace(backend) == string(preset.Backend) {
				return "", false
			}
		}
		return "label." + domain.LabelPeerBackends, true
	}
	backend := node.Labels[domain.LabelPeerBackend]
	if backend == "" || backend == string(preset.Backend) {
		return "", false
	}
	return "label." + domain.LabelPeerBackend, true
}

func presetDiskMismatchForPrewarm(preset domain.Preset, node domain.Node, fleet domain.FleetSnapshot) (string, bool) {
	if node.DiskTotalMB <= 0 {
		return "disk.unknown", true
	}
	minFreeRatio := node.DiskMinFreeRatio
	if minFreeRatio == 0 {
		minFreeRatio = domain.DefaultDiskMinFreeRatio
	}
	if minFreeRatio <= 0 || minFreeRatio >= 1 {
		return "disk.limit", true
	}
	floorMB := int(math.Ceil(float64(node.DiskTotalMB) * minFreeRatio))
	if node.DiskFreeMB <= floorMB {
		return "disk.free", true
	}
	requiredMB := preset.ArtifactSizeMB
	if modelAlreadyPresentForPrewarm(preset, node, fleet) {
		requiredMB = 0
	}
	if requiredMB == 0 {
		requiredMB = preset.EstWeightsMB
	}
	if requiredMB < 0 {
		return "disk.required", true
	}
	if node.DiskFreeMB-requiredMB <= floorMB {
		return "disk.free_after_model", true
	}
	return "", false
}

func modelAlreadyPresentForPrewarm(preset domain.Preset, node domain.Node, fleet domain.FleetSnapshot) bool {
	if preset.NodeID != "" && preset.NodeID == node.ID {
		return true
	}
	for _, inst := range fleet.Instances {
		if inst.NodeID == node.ID && inst.PresetID == preset.ID {
			return true
		}
	}
	return false
}

func parsePriorityFlag(raw string) (domain.Priority, error) {
	switch domain.Priority(strings.TrimSpace(raw)) {
	case domain.PriorityInteractive, domain.PriorityNormal, domain.PriorityBackground:
		return domain.Priority(strings.TrimSpace(raw)), nil
	default:
		return "", fmt.Errorf("unknown priority %q", raw)
	}
}

type optionalReplicasFlag struct {
	set   bool
	value int
}

func (f *optionalReplicasFlag) Set(raw string) error {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return err
	}
	f.set = true
	f.value = value
	return nil
}

func (f *optionalReplicasFlag) String() string {
	return strconv.Itoa(f.value)
}

func prewarmJobID(model, preset string, i int) string {
	base := model
	if base == "" {
		base = preset
	}
	base = strings.Trim(strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		case r >= '0' && r <= '9':
			return r
		default:
			return '-'
		}
	}, base), "-")
	if base == "" {
		base = "model"
	}
	return fmt.Sprintf("prewarm-%s-%d-%d", base, time.Now().UTC().UnixNano(), i+1)
}

func admitPrewarmWorkload(ctx context.Context, client *http.Client, peerURL, rpcToken string, job domain.Job) (workload.AdmissionResponse, error) {
	payload, err := prewarmWorkloadPayload(job)
	if err != nil {
		return workload.AdmissionResponse{}, err
	}
	body, err := json.Marshal(workload.AdmissionRequest{Job: job, Payload: payload})
	if err != nil {
		return workload.AdmissionResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(peerURL, "/")+"/workloads", bytes.NewReader(body))
	if err != nil {
		return workload.AdmissionResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if rpcToken != "" {
		req.Header.Set("Authorization", "Bearer "+rpcToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return workload.AdmissionResponse{}, err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "workload prewarm response body")
	if err != nil {
		return workload.AdmissionResponse{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return workload.AdmissionResponse{}, fmt.Errorf("models prewarm admit returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.AdmissionResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return workload.AdmissionResponse{}, err
	}
	return out, nil
}

func prewarmWorkloadPayload(job domain.Job) (json.RawMessage, error) {
	payload := map[string]any{
		"kind":                 "model_prewarm",
		"model":                job.Model,
		"preset_id":            job.PresetID,
		"project":              job.Project,
		"context_request":      job.ContextRequest,
		"expected_concurrency": job.ExpectedConcurrency,
		"node_selector":        job.NodeSelector,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func finishPrewarmWorkload(ctx context.Context, client *http.Client, peerURL, rpcToken, jobID string) (workload.FinishResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(peerURL, "/")+"/workloads/"+url.PathEscape(jobID)+"/finish", nil)
	if err != nil {
		return workload.FinishResponse{}, err
	}
	if rpcToken != "" {
		req.Header.Set("Authorization", "Bearer "+rpcToken)
	}
	resp, err := client.Do(req)
	if err != nil {
		return workload.FinishResponse{}, err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "workload prewarm finish response body")
	if err != nil {
		return workload.FinishResponse{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return workload.FinishResponse{}, fmt.Errorf("models prewarm finish returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.FinishResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return workload.FinishResponse{}, err
	}
	return out, nil
}

func prewarmResultFromAdmission(out workload.AdmissionResponse) modelPrewarmResult {
	result := modelPrewarmResult{
		JobID:    out.Job.ID,
		Model:    out.Job.Model,
		PresetID: out.Job.PresetID,
		Status:   out.Job.Status,
		Action:   out.Decision.Action,
		NodeID:   out.Decision.NodeID,
		Instance: out.Instance.ID,
		LeaseID:  out.Lease.ID,
	}
	if out.Result != nil {
		result.PresetID = firstNonEmpty(result.PresetID, out.Result.PresetID)
		result.NodeID = firstNonEmpty(result.NodeID, out.Result.NodeID)
		result.Action = out.Result.Action
		result.LeaseID = firstNonEmpty(result.LeaseID, out.Result.LeaseID)
		if out.Result.Instance.ID != "" {
			result.Instance = out.Result.Instance.ID
		}
		result.Terminal = out.Result.Terminal
	}
	return result
}

func prewarmResultFromFinish(out workload.FinishResponse, prior modelPrewarmResult) modelPrewarmResult {
	result := prior
	result.Status = out.Job.Status
	result.Finished = true
	if out.Result != nil {
		result.PresetID = firstNonEmpty(result.PresetID, out.Result.PresetID)
		result.NodeID = firstNonEmpty(result.NodeID, out.Result.NodeID)
		result.Action = out.Result.Action
		result.LeaseID = firstNonEmpty(result.LeaseID, out.Result.LeaseID)
		if out.Result.Instance.ID != "" {
			result.Instance = out.Result.Instance.ID
		}
		result.Terminal = out.Result.Terminal
	}
	return result
}

func printModelPrewarmReport(report modelPrewarmReport) {
	fmt.Printf("models-prewarm\tmodel=%s\tpreset=%s\trequested=%d\tdesired=%d\twarmed=%d\tqueued=%d\tunique_instances=%d\n",
		emptyDash(report.Model), emptyDash(report.PresetID), report.RequestedReplicas, report.DesiredReplicas, report.Warmed, report.Queued, report.UniqueInstances)
	for _, fit := range report.FitCandidates {
		fmt.Printf("fit\tnode=%s\tstatus=%s\tbackend=%s\tacc=%s\tclaim_weights_mb=%d\tclaim_kv_mb=%d\treason=%s\n",
			emptyDash(fit.NodeID), emptyDash(fit.Status), emptyDash(string(fit.Backend)), intSliceString(fit.AcceleratorSet), fit.Claim.WeightsMB, fit.Claim.KVReservedMB, emptyDash(fit.Reason))
	}
	for _, result := range report.Results {
		fmt.Printf("prewarm\t%s\tstatus=%s\taction=%s\tnode=%s\tpreset=%s\tinstance=%s\tlease=%s\tfinished=%t\tterminal=%t\n",
			result.JobID,
			result.Status,
			emptyDash(string(result.Action)),
			emptyDash(result.NodeID),
			emptyDash(result.PresetID),
			emptyDash(result.Instance),
			emptyDash(result.LeaseID),
			result.Finished,
			result.Terminal,
		)
	}
}

func intSliceString(values []int) string {
	if len(values) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}

func runModelsStage(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("models stage", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	presetID := fs.String("preset", "", "preset id to stage")
	nodeID := fs.String("node", "", "target node id")
	url := fs.String("url", "", "target peer URL override")
	rpcToken := fs.String("rpc-token", "", "target peer RPC bearer token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *presetID == "" {
		return fmt.Errorf("--preset is required")
	}
	if *nodeID == "" {
		return fmt.Errorf("--node is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	preset, err := store.Preset(ctx, *presetID)
	if err != nil {
		return err
	}
	node, err := store.Node(ctx, *nodeID)
	if err != nil {
		return err
	}
	address := *url
	if address == "" {
		address = node.Address
	}
	if address == "" {
		return fmt.Errorf("node %q has no reachable address", node.ID)
	}
	peer := domain.Peer{ID: node.ID, Addresses: []string{address}, Compute: true}
	locality, err := catalogStageHTTPClient{AuthToken: *rpcToken, Client: client}.StageModel(ctx, peer, preset)
	if err != nil {
		return err
	}
	if err := store.SaveModelLocality(ctx, locality); err != nil {
		return err
	}
	fmt.Printf("stage\t%s\t%s\t%s\t%s\n", locality.PresetID, locality.NodeID, locality.State, locality.ModelRef)
	return nil
}

func runModelsLocality(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce models locality <report|plan|apply>")
	}
	switch args[0] {
	case "report":
		return runModelsLocalityReport(ctx, args[1:])
	case "plan":
		return runModelsLocalityPlan(ctx, args[1:])
	case "apply":
		return runModelsLocalityApply(ctx, args[1:], catalogStageHTTPClient{})
	default:
		return fmt.Errorf("usage: myce models locality <report|plan|apply>")
	}
}

func runModelsLocalityReport(ctx context.Context, args []string) error {
	store, err := openCLIStore(args)
	if err != nil {
		return err
	}
	defer store.Close()
	report, err := (locality.Planner{Store: store}).Report(ctx)
	if err != nil {
		return err
	}
	for _, locality := range report.Localities {
		fmt.Printf("%s\t%s\t%s\t%s\t%t\t%d\t%s\n", locality.PresetID, locality.NodeID, locality.State, locality.ModelRef, locality.Managed, locality.ArtifactSizeMB, locality.Reason)
	}
	return nil
}

func runModelsLocalityPlan(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("models locality plan", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	id := fs.String("id", "", "locality plan id")
	project := fs.String("project", "", "project id for telemetry demand")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token for live snapshot refresh")
	var peerURLs repeatedString
	fs.Var(&peerURLs, "peer-url", "compute peer URL to snapshot before planning; may be repeated")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := refreshLocalityPeerSnapshots(ctx, store, peerURLs, *rpcToken, nil); err != nil {
		return err
	}
	plan, err := (locality.Planner{Store: store, Clock: clock.System{}}).Plan(ctx, locality.PlanRequest{ID: *id, Project: *project})
	if err != nil {
		return err
	}
	printLocalityPlan(plan)
	return nil
}

func runModelsLocalityApply(ctx context.Context, args []string, client catalogStageHTTPClient) error {
	fs := flag.NewFlagSet("models locality apply", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	id := fs.String("id", "", "locality plan id")
	rpcToken := fs.String("rpc-token", "", "target peer RPC bearer token")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	plan, err := store.LocalityPlan(ctx, *id)
	if err != nil {
		return err
	}
	client.AuthToken = *rpcToken
	for i := range plan.Actions {
		action := &plan.Actions[i]
		switch action.Kind {
		case domain.LocalityActionKeep:
			continue
		case domain.LocalityActionStage:
			preset, err := store.Preset(ctx, action.PresetID)
			if err != nil {
				return recordLocalityActionError(ctx, store, &plan, action, err)
			}
			peer, err := localityActionPeer(ctx, store, action.NodeID)
			if err != nil {
				return recordLocalityActionError(ctx, store, &plan, action, err)
			}
			localityRecord, err := client.StageModel(ctx, peer, preset)
			if err != nil {
				return recordLocalityActionError(ctx, store, &plan, action, err)
			}
			if err := store.SaveModelLocality(ctx, localityRecord); err != nil {
				return recordLocalityActionError(ctx, store, &plan, action, err)
			}
			action.State = domain.ModelLocalityReady
			action.Error = ""
			fmt.Printf("locality-apply\tstage\t%s\t%s\tready\n", action.PresetID, action.NodeID)
		case domain.LocalityActionEvict:
			peer, err := localityActionPeer(ctx, store, action.NodeID)
			if err != nil {
				return recordLocalityActionError(ctx, store, &plan, action, err)
			}
			localityRecord, err := client.EvictModel(ctx, peer, *action)
			if err != nil {
				return recordLocalityActionError(ctx, store, &plan, action, err)
			}
			if err := store.DeleteModelLocality(ctx, modelLocalityID(action.NodeID, action.PresetID)); err != nil {
				return recordLocalityActionError(ctx, store, &plan, action, err)
			}
			action.State = localityRecord.State
			action.Error = ""
			fmt.Printf("locality-apply\tevict\t%s\t%s\t%s\n", action.PresetID, action.NodeID, localityRecord.State)
		default:
			return recordLocalityActionError(ctx, store, &plan, action, fmt.Errorf("unknown locality action kind %q", action.Kind))
		}
		if err := store.SaveLocalityPlan(ctx, plan); err != nil {
			return err
		}
	}
	return store.SaveLocalityPlan(ctx, plan)
}

func localityActionPeer(ctx context.Context, store *storesqlite.Store, nodeID string) (domain.Peer, error) {
	node, err := store.Node(ctx, nodeID)
	if err != nil {
		return domain.Peer{}, err
	}
	if node.Address == "" {
		return domain.Peer{}, fmt.Errorf("node %q has no reachable address", node.ID)
	}
	return domain.Peer{ID: node.ID, Addresses: []string{node.Address}, Compute: true}, nil
}

func recordLocalityActionError(ctx context.Context, store *storesqlite.Store, plan *domain.LocalityPlan, action *domain.LocalityAction, err error) error {
	action.State = domain.ModelLocalityFailed
	action.Error = err.Error()
	_ = store.SaveLocalityPlan(ctx, *plan)
	return err
}

func printLocalityPlan(plan domain.LocalityPlan) {
	fmt.Printf("locality-plan\t%s\t%d\twarnings=%d\n", plan.ID, len(plan.Actions), len(plan.Warnings))
	for _, action := range plan.Actions {
		fmt.Printf("locality-action\t%s\t%s\t%s\t%s\t%s\n", action.ID, action.Kind, action.PresetID, action.NodeID, action.Reason)
	}
	for _, warning := range plan.Warnings {
		fmt.Printf("locality-warning\t%s\n", warning)
	}
}

func refreshLocalityPeerSnapshots(ctx context.Context, store *storesqlite.Store, peerURLs []string, rpcToken string, client *http.Client) error {
	if len(peerURLs) == 0 {
		return nil
	}
	if client == nil {
		client = http.DefaultClient
	}
	for _, peerURL := range peerURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(catalogPeerBaseURL(peerURL), "/")+"/snapshot", nil)
		if err != nil {
			return err
		}
		if rpcToken != "" {
			req.Header.Set("Authorization", "Bearer "+rpcToken)
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		data, readErr := readControlHTTPBody(resp.Body, "snapshot response body")
		_ = resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("snapshot %s: %s", peerURL, strings.TrimSpace(string(data)))
		}
		var snap domain.NodeSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return err
		}
		if snap.Node.ID == "" {
			return fmt.Errorf("snapshot %s returned empty node id", peerURL)
		}
		if err := store.SaveNode(ctx, snap.Node); err != nil {
			return err
		}
		for _, inst := range snap.Instances {
			if err := store.SaveInstance(ctx, inst); err != nil {
				return err
			}
		}
	}
	return nil
}

type catalogStageHTTPClient struct {
	AuthToken string
	Client    *http.Client
}

func (c catalogStageHTTPClient) StageModel(ctx context.Context, peer domain.Peer, preset domain.Preset) (domain.ModelLocality, error) {
	if len(peer.Addresses) == 0 {
		return domain.ModelLocality{}, fmt.Errorf("peer %q has no reachable address", peer.ID)
	}
	body, err := json.Marshal(map[string]any{"preset": preset})
	if err != nil {
		return domain.ModelLocality{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(catalogPeerBaseURL(peer.Addresses[0]), "/")+"/catalog/stage", bytes.NewReader(body))
	if err != nil {
		return domain.ModelLocality{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return domain.ModelLocality{}, err
	}
	defer resp.Body.Close()
	data, err := readControlHTTPBody(resp.Body, "catalog stage response body")
	if err != nil {
		return domain.ModelLocality{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return domain.ModelLocality{}, fmt.Errorf("catalog stage %s: %s", peer.ID, strings.TrimSpace(string(data)))
	}
	var out struct {
		Locality domain.ModelLocality `json:"locality"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return domain.ModelLocality{}, err
	}
	if out.Locality.ID == "" {
		return domain.ModelLocality{}, fmt.Errorf("catalog stage %s returned no locality", peer.ID)
	}
	return out.Locality, nil
}

func (c catalogStageHTTPClient) EvictModel(ctx context.Context, peer domain.Peer, action domain.LocalityAction) (domain.ModelLocality, error) {
	if len(peer.Addresses) == 0 {
		return domain.ModelLocality{}, fmt.Errorf("peer %q has no reachable address", peer.ID)
	}
	body, err := json.Marshal(map[string]string{"preset_id": action.PresetID, "node_id": action.NodeID})
	if err != nil {
		return domain.ModelLocality{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(catalogPeerBaseURL(peer.Addresses[0]), "/")+"/catalog/evict", bytes.NewReader(body))
	if err != nil {
		return domain.ModelLocality{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return domain.ModelLocality{}, err
	}
	defer resp.Body.Close()
	data, err := readControlHTTPBody(resp.Body, "catalog evict response body")
	if err != nil {
		return domain.ModelLocality{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return domain.ModelLocality{}, fmt.Errorf("catalog evict %s: %s", peer.ID, strings.TrimSpace(string(data)))
	}
	var out struct {
		Locality domain.ModelLocality `json:"locality"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return domain.ModelLocality{}, err
	}
	if out.Locality.State != domain.ModelLocalityEvicted {
		return domain.ModelLocality{}, fmt.Errorf("catalog evict %s returned state %q", peer.ID, out.Locality.State)
	}
	return out.Locality, nil
}

func catalogPeerBaseURL(address string) string {
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return address
	}
	return "http://" + address
}

var _ ports.PeerCatalogStager = catalogStageHTTPClient{}

func modelLocalityID(nodeID, presetID string) string {
	return nodeID + ":" + presetID
}

func runNodes(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce nodes <list|invite|tokens>")
	}
	switch args[0] {
	case "list":
		store, err := openCLIStore(args[1:])
		if err != nil {
			return err
		}
		defer store.Close()
		nodes, err := store.ListNodes(ctx)
		if err != nil {
			return err
		}
		for _, node := range nodes {
			fmt.Printf("%s\t%s\t%s\t%s\n", node.ID, node.Name, node.Address, node.Status)
		}
		return nil
	case "invite":
		admin, err := nodeAdminFromArgs(args[1:], client)
		if err != nil {
			return err
		}
		join, err := admin.Invite(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", join)
		return nil
	case "tokens":
		return runNodeTokens(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce nodes <list|invite|tokens>")
	}
}

func runNodeTokens(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce nodes tokens <list|rotate|revoke>")
	}
	switch args[0] {
	case "list":
		admin, err := nodeAdminFromArgs(args[1:], client)
		if err != nil {
			return err
		}
		tokens, err := admin.Tokens(ctx)
		if err != nil {
			return err
		}
		for _, token := range tokens {
			fmt.Printf("%s\t%t\t%t\n", token.Hash, token.Active, token.Current)
		}
		return nil
	case "rotate":
		admin, token, err := nodeAdminWithTokenFromArgs(args[1:], client, false)
		if err != nil {
			return err
		}
		join, err := admin.Rotate(ctx, token)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", join)
		return nil
	case "revoke":
		admin, token, err := nodeAdminWithTokenFromArgs(args[1:], client, true)
		if err != nil {
			return err
		}
		if err := admin.Revoke(ctx, token); err != nil {
			return err
		}
		fmt.Printf("revoked\n")
		return nil
	default:
		return fmt.Errorf("usage: myce nodes tokens <list|rotate|revoke>")
	}
}

func nodeAdminFromArgs(args []string, client *http.Client) (nodeAdminClient, error) {
	fs := flag.NewFlagSet("nodes admin", flag.ContinueOnError)
	url := fs.String("url", "", "peer URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	if err := fs.Parse(args); err != nil {
		return nodeAdminClient{}, err
	}
	defaults, err := loadNodeAdminDefaults()
	if err != nil {
		return nodeAdminClient{}, err
	}
	return nodeAdminClient{BaseURL: firstNonEmpty(*url, defaults.BaseURL, "http://127.0.0.1:51846"), AuthToken: firstNonEmpty(*rpcToken, defaults.RPCToken), Client: client}, nil
}

func nodeAdminWithTokenFromArgs(args []string, client *http.Client, requireToken bool) (nodeAdminClient, string, error) {
	fs := flag.NewFlagSet("nodes tokens", flag.ContinueOnError)
	url := fs.String("url", "", "peer URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	token := fs.String("token", "", "join token")
	if err := fs.Parse(args); err != nil {
		return nodeAdminClient{}, "", err
	}
	if requireToken && *token == "" {
		return nodeAdminClient{}, "", fmt.Errorf("--token is required")
	}
	defaults, err := loadNodeAdminDefaults()
	if err != nil {
		return nodeAdminClient{}, "", err
	}
	return nodeAdminClient{BaseURL: firstNonEmpty(*url, defaults.BaseURL, "http://127.0.0.1:51846"), AuthToken: firstNonEmpty(*rpcToken, defaults.RPCToken), Client: client}, *token, nil
}

type nodeAdminDefaults struct {
	BaseURL  string
	RPCToken string
}

func loadNodeAdminDefaults() (nodeAdminDefaults, error) {
	path := filepath.Join(defaultMyceliumHome(), "peer.json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nodeAdminDefaults{}, nil
	}
	if err != nil {
		return nodeAdminDefaults{}, err
	}
	var cfg struct {
		Listen   string `json:"listen"`
		RPCToken string `json:"rpc_token"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nodeAdminDefaults{}, fmt.Errorf("parse peer config %s: %w", path, err)
	}
	return nodeAdminDefaults{BaseURL: adminBaseURL(cfg.Listen), RPCToken: cfg.RPCToken}, nil
}

func adminBaseURL(listen string) string {
	if listen == "" {
		return ""
	}
	if strings.HasPrefix(listen, "http://") || strings.HasPrefix(listen, "https://") {
		return listen
	}
	return "http://" + listen
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

type nodeAdminClient struct {
	BaseURL   string
	AuthToken string
	Client    *http.Client
}

type inviteResponse struct {
	Join string `json:"join"`
}

func (c nodeAdminClient) Invite(ctx context.Context) (string, error) {
	var out inviteResponse
	if err := c.do(ctx, http.MethodPost, "/admin/invite", nil, &out); err != nil {
		return "", err
	}
	if out.Join == "" {
		return "", fmt.Errorf("peer returned an empty join uri")
	}
	return out.Join, nil
}

func (c nodeAdminClient) Tokens(ctx context.Context) ([]domain.JoinTokenRecord, error) {
	var out []domain.JoinTokenRecord
	err := c.do(ctx, http.MethodGet, "/admin/tokens", nil, &out)
	return out, err
}

func (c nodeAdminClient) Rotate(ctx context.Context, token string) (string, error) {
	var in any
	if token != "" {
		in = map[string]string{"token": token}
	}
	var out inviteResponse
	if err := c.do(ctx, http.MethodPost, "/admin/tokens/rotate", in, &out); err != nil {
		return "", err
	}
	if out.Join == "" {
		return "", fmt.Errorf("peer returned an empty join uri")
	}
	return out.Join, nil
}

func (c nodeAdminClient) Revoke(ctx context.Context, token string) error {
	return c.do(ctx, http.MethodPost, "/admin/tokens/revoke", map[string]string{"token": token}, nil)
}

func (c nodeAdminClient) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := readControlHTTPBody(resp.Body, "peer admin response body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("peer admin %s %s: %s", method, path, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func runProjects(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "set" {
		return fmt.Errorf("usage: myce projects set --id id [--db path]")
	}
	fs := flag.NewFlagSet("projects set", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	id := fs.String("id", "", "project id")
	defaultModel := fs.String("default-model", "", "default model or preset id")
	priority := fs.String("priority", string(domain.PriorityInteractive), "priority")
	speed := fs.String("speed-pref", string(domain.SpeedThroughput), "speed preference")
	contextCap := fs.Int("context-cap", 0, "context cap")
	expectedConcurrency := fs.Int("expected-concurrency", 1, "expected concurrent requests for resource estimates")
	latencyTarget := fs.Int("latency-target-ms", 0, "latency target in milliseconds")
	preemption := fs.String("preemption", string(domain.PreemptSoft), "preemption mode")
	autoApply := fs.Bool("auto-apply", false, "enable optimizer auto-apply")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	project := domain.Project{
		ID:                  *id,
		DefaultModel:        *defaultModel,
		Priority:            domain.Priority(*priority),
		SpeedPref:           domain.SpeedPref(*speed),
		ContextCap:          *contextCap,
		ExpectedConcurrency: *expectedConcurrency,
		LatencyTargetMS:     *latencyTarget,
		Preemption:          domain.Preemption(*preemption),
		AutoApply:           *autoApply,
	}
	if err := projectvalidation.Validate(project); err != nil {
		return err
	}
	if err := store.SaveProject(ctx, project); err != nil {
		return err
	}
	fmt.Printf("project\t%s\n", project.ID)
	return nil
}

func runJobs(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce jobs <list|admit|status|drain|execute|finish|cancel|result>")
	}
	switch args[0] {
	case "list":
		return runJobsList(ctx, args[1:])
	case "admit":
		return runJobsAdmit(ctx, args[1:], client)
	case "status":
		return runJobsStatus(ctx, args[1:], client)
	case "drain":
		return runJobsDrain(ctx, args[1:], client)
	case "execute":
		return runJobsExecute(ctx, args[1:], client)
	case "finish":
		return runJobsFinish(ctx, args[1:], client)
	case "cancel":
		return runJobsCancel(ctx, args[1:], client)
	case "result":
		return runJobsResult(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce jobs <list|admit|status|drain|execute|finish|cancel|result>")
	}
}

func runJobsList(ctx context.Context, args []string) error {
	store, err := openCLIStore(args)
	if err != nil {
		return err
	}
	defer store.Close()
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\n", job.ID, job.TaskType, job.Project, job.Model, job.Status, jobProgressSummary(job))
	}
	return nil
}

func runJobsAdmit(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("jobs admit", flag.ContinueOnError)
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	file := fs.String("file", "", "workload admission JSON file")
	payloadKind := fs.String("payload-kind", "", "executable payload kind to wrap from --payload-file")
	payloadFile := fs.String("payload-file", "", "private request body JSON file to wrap as executable payload")
	queueOnly := fs.Bool("queue-only", false, "admit the workload to the explicit queue without immediate placement")
	jsonOut := fs.Bool("json", false, "print JSON response")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerURL == "" {
		return fmt.Errorf("--url is required")
	}
	if *file == "" {
		return fmt.Errorf("--file is required")
	}
	data, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var req workload.AdmissionRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	if *payloadKind != "" || *payloadFile != "" {
		if *payloadKind == "" || *payloadFile == "" {
			return fmt.Errorf("--payload-kind and --payload-file must be supplied together")
		}
		if len(req.Payload) > 0 {
			return fmt.Errorf("--payload-file cannot be combined with payload in --file")
		}
		payloadBody, err := os.ReadFile(*payloadFile)
		if err != nil {
			return err
		}
		executable := workload.ExecutablePayload{Kind: *payloadKind, Body: json.RawMessage(payloadBody)}
		payload, err := json.Marshal(executable)
		if err != nil {
			return err
		}
		if _, err := workload.DecodeExecutablePayload(payload); err != nil {
			return err
		}
		req.Payload = payload
	}
	if *queueOnly {
		req.QueueOnly = true
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(*peerURL, "/")+"/workloads", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if *rpcToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+*rpcToken)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "workload admission response body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("workload admit returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.AdmissionResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return err
	}
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	printWorkloadAdmission(out)
	return nil
}

func runJobsStatus(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("jobs status", flag.ContinueOnError)
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	id := fs.String("id", "", "workload job id")
	jsonOut := fs.Bool("json", false, "print JSON response")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerURL == "" {
		return fmt.Errorf("--url is required")
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(*peerURL, "/")+"/workloads/"+url.PathEscape(*id), nil)
	if err != nil {
		return err
	}
	if *rpcToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+*rpcToken)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "workload status response body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("workload status returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.StatusResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return err
	}
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	printWorkloadStatus(out)
	return nil
}

func runJobsDrain(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("jobs drain", flag.ContinueOnError)
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	limit := fs.Int("limit", 1, "maximum queued workloads to drain")
	execute := fs.Bool("execute", false, "execute drained executable payloads through the provider path")
	jsonOut := fs.Bool("json", false, "print JSON response")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerURL == "" {
		return fmt.Errorf("--url is required")
	}
	if *limit <= 0 {
		return fmt.Errorf("--limit must be positive")
	}
	endpoint := strings.TrimRight(*peerURL, "/") + "/workloads/drain?limit=" + url.QueryEscape(strconv.Itoa(*limit))
	if *execute {
		endpoint += "&execute=true"
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	if *rpcToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+*rpcToken)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "workload drain response body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("workload drain returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.DrainResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return err
	}
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	fmt.Printf("drained\t%d\n", len(out.Results))
	for _, result := range out.Results {
		printWorkloadAdmission(result)
	}
	return nil
}

func runJobsExecute(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("jobs execute", flag.ContinueOnError)
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	id := fs.String("id", "", "running workload job id")
	jsonOut := fs.Bool("json", false, "print JSON response")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerURL == "" {
		return fmt.Errorf("--url is required")
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	endpoint := strings.TrimRight(*peerURL, "/") + "/workloads/" + url.PathEscape(*id) + "/execute"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	if *rpcToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+*rpcToken)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "workload execute response body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("workload execute returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.FinishResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return err
	}
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	printWorkloadStatus(workload.StatusResponse{Job: out.Job, Result: out.Result, Execution: out.Execution})
	return nil
}

func runJobsFinish(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("jobs finish", flag.ContinueOnError)
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	id := fs.String("id", "", "workload job id")
	errorText := fs.String("error", "", "mark workload failed with this redacted error string")
	resultArtifact := fs.String("result-artifact", "", "redacted result artifact reference to attach to the terminal workload")
	resultMediaType := fs.String("result-media-type", "", "media type of the redacted result artifact")
	resultBytes := fs.Int("result-bytes", 0, "byte size of the redacted result artifact")
	resultSHA256 := fs.String("result-sha256", "", "sha256 digest of the redacted result artifact")
	resultFile := fs.String("result-file", "", "read and store replayable result bytes from this file")
	jsonOut := fs.Bool("json", false, "print JSON response")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerURL == "" {
		return fmt.Errorf("--url is required")
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	var resultBody []byte
	if *resultFile != "" {
		var err error
		resultBody, err = os.ReadFile(*resultFile)
		if err != nil {
			return err
		}
		if *resultBytes != 0 && *resultBytes != len(resultBody) {
			return fmt.Errorf("--result-bytes does not match --result-file")
		}
		*resultBytes = len(resultBody)
		sum := sha256.Sum256(resultBody)
		computed := hex.EncodeToString(sum[:])
		if *resultSHA256 != "" && strings.ToLower(strings.TrimSpace(*resultSHA256)) != computed {
			return fmt.Errorf("--result-sha256 does not match --result-file")
		}
		*resultSHA256 = computed
		if *resultArtifact == "" {
			*resultArtifact = "stored://workload/" + *id
		}
	}
	body, err := json.Marshal(workload.FinishRequest{
		Error:           *errorText,
		ResultArtifact:  *resultArtifact,
		ResultMediaType: *resultMediaType,
		ResultBytes:     *resultBytes,
		ResultSHA256:    *resultSHA256,
		ResultBody:      resultBody,
	})
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(*peerURL, "/") + "/workloads/" + url.PathEscape(*id) + "/finish"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if *rpcToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+*rpcToken)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "workload finish response body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("workload finish returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.FinishResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return err
	}
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	printWorkloadStatus(workload.StatusResponse{Job: out.Job, Result: out.Result, Execution: out.Execution})
	return nil
}

func runJobsCancel(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("jobs cancel", flag.ContinueOnError)
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	id := fs.String("id", "", "workload job id")
	reason := fs.String("reason", "canceled by operator", "redacted cancellation reason")
	jsonOut := fs.Bool("json", false, "print JSON response")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerURL == "" {
		return fmt.Errorf("--url is required")
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	body, err := json.Marshal(workload.CancelRequest{Reason: *reason})
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(*peerURL, "/")+"/workloads/"+url.PathEscape(*id)+"/cancel", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if *rpcToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+*rpcToken)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := readControlHTTPBody(resp.Body, "workload cancel response body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("workload cancel returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var out workload.FinishResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return err
	}
	if *jsonOut {
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	printWorkloadStatus(workload.StatusResponse{Job: out.Job, Result: out.Result, Execution: out.Execution})
	return nil
}

func runJobsResult(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("jobs result", flag.ContinueOnError)
	peerURL := fs.String("url", "", "peer base URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	id := fs.String("id", "", "workload job id")
	outPath := fs.String("out", "", "output file for replayable result bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *peerURL == "" {
		return fmt.Errorf("--url is required")
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	if *outPath == "" {
		return fmt.Errorf("--out is required")
	}
	endpoint := strings.TrimRight(*peerURL, "/") + "/workloads/" + url.PathEscape(*id) + "/result"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	if *rpcToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+*rpcToken)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := readControlHTTPBody(resp.Body, "workload result body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("workload result returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := os.WriteFile(*outPath, body, 0600); err != nil {
		return err
	}
	fmt.Printf("workload-result\t%s\tbytes=%d\tsha256=%s\tmedia_type=%s\n",
		*outPath,
		len(body),
		emptyDash(resp.Header.Get("X-Myc-Result-SHA256")),
		emptyDash(resp.Header.Get("Content-Type")),
	)
	return nil
}

func printWorkloadAdmission(out workload.AdmissionResponse) {
	node := emptyDash(out.Decision.NodeID)
	preset := emptyDash(out.Decision.Preset.ID)
	if out.Result != nil {
		node = emptyDash(out.Result.NodeID)
		preset = emptyDash(out.Result.PresetID)
	}
	fmt.Printf("workload\t%s\tstatus=%s\tqueued=%t\taction=%s\tnode=%s\tpreset=%s\tpayload_stored=%t\tpayload_bytes=%d\tpayload_state=%s\tpayload_replayable=%t\tpayload_contract=%s\tpayload_kind=%s\tpayload_executable=%t\texecution_state=%s\tprovider_output_capture=%t\tresult_replayable=%t\tterminal=%t\n",
		out.Job.ID, out.Job.Status, out.Queued, out.Decision.Action, node, preset, out.PayloadStored, out.PayloadBytes, emptyDash(out.Payload.State), out.Payload.Replayable, emptyDash(out.Execution.PayloadContract), emptyDash(out.Execution.PayloadKind), out.Execution.PayloadExecutable, emptyDash(out.Execution.State), out.Execution.AutomaticProviderOutputCapture, out.Execution.ResultReplayable, out.Result != nil && out.Result.Terminal)
}

func printWorkloadStatus(out workload.StatusResponse) {
	action := domain.PlacementAction("")
	node := ""
	preset := out.Job.PresetID
	lease := ""
	payloadStored := false
	payloadBytes := 0
	terminal := false
	resultArtifact := ""
	resultBytes := 0
	payloadState := out.Payload.State
	payloadReplayable := out.Payload.Replayable
	payloadContract := out.Execution.PayloadContract
	payloadKind := out.Execution.PayloadKind
	payloadExecutable := out.Execution.PayloadExecutable
	executionState := out.Execution.State
	providerOutputCapture := out.Execution.AutomaticProviderOutputCapture
	resultReplayable := out.Execution.ResultReplayable
	if out.Result != nil {
		action = out.Result.Action
		node = out.Result.NodeID
		if out.Result.PresetID != "" {
			preset = out.Result.PresetID
		}
		lease = out.Result.LeaseID
		payloadStored = out.Result.PayloadStored
		payloadBytes = out.Result.PayloadBytes
		terminal = out.Result.Terminal
		resultArtifact = out.Result.ResultArtifact
		resultBytes = out.Result.ResultBytes
	}
	fmt.Printf("workload\t%s\tstatus=%s\tproject=%s\tmodel=%s\tpreset=%s\tpriority=%s\tcontext=%d\tconcurrency=%d\tpreemption=%s\taction=%s\tnode=%s\tlease=%s\tpayload_stored=%t\tpayload_bytes=%d\tpayload_state=%s\tpayload_replayable=%t\tpayload_contract=%s\tpayload_kind=%s\tpayload_executable=%t\texecution_state=%s\tprovider_output_capture=%t\tresult_replayable=%t\tterminal=%t\tresult_artifact=%s\tresult_bytes=%d\n",
		out.Job.ID, out.Job.Status, out.Job.Project, out.Job.Model, preset, out.Job.Priority, out.Job.ContextRequest, out.Job.ExpectedConcurrency, out.Job.Preemption, action, emptyDash(node), emptyDash(lease), payloadStored, payloadBytes, emptyDash(payloadState), payloadReplayable, emptyDash(payloadContract), emptyDash(payloadKind), payloadExecutable, emptyDash(executionState), providerOutputCapture, resultReplayable, terminal, emptyDash(resultArtifact), resultBytes)
}

func runTelemetry(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce telemetry <samples>")
	}
	switch args[0] {
	case "samples":
		return runTelemetrySamples(ctx, args[1:])
	default:
		return fmt.Errorf("usage: myce telemetry <samples>")
	}
}

func runTelemetrySamples(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("telemetry samples", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	sessionID := fs.String("session", "", "session id")
	project := fs.String("project", "", "project id")
	nodeID := fs.String("node", "", "node id")
	since := fs.String("since", "", "RFC3339 timestamp lower bound")
	until := fs.String("until", "", "RFC3339 timestamp upper bound")
	limit := fs.Int("limit", 0, "maximum samples to print")
	if err := fs.Parse(args); err != nil {
		return err
	}
	query := domain.SessionMetricQuery{
		SessionID: *sessionID,
		Project:   *project,
		NodeID:    *nodeID,
		Limit:     *limit,
	}
	if *since != "" {
		at, err := time.Parse(time.RFC3339Nano, *since)
		if err != nil {
			return fmt.Errorf("invalid --since: %w", err)
		}
		query.Since = at
	}
	if *until != "" {
		at, err := time.Parse(time.RFC3339Nano, *until)
		if err != nil {
			return fmt.Errorf("invalid --until: %w", err)
		}
		query.Until = at
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	samples, err := store.Samples(ctx, query)
	if err != nil {
		return err
	}
	for _, sample := range samples {
		fmt.Printf("sample\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%.2f\t%d\t%d\t%s\n",
			sample.SessionID,
			sample.Sequence,
			sample.Phase,
			sample.JobID,
			sample.NodeID,
			sample.Project,
			sample.PresetID,
			sample.ContextUsed,
			sample.BytesIn,
			sample.BytesOut,
			sample.TokensPerSec,
			sample.TTFTms,
			sample.ElapsedMS,
			sample.At.UTC().Format(time.RFC3339Nano),
		)
	}
	return nil
}

func runBenchmark(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce benchmark <run|fleet>")
	}
	switch args[0] {
	case "run":
		return runBenchmarkFanout(ctx, args[1:], client)
	case "fleet":
		return runBenchmarkFleet(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce benchmark <run|fleet>")
	}
}

func runGateway(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce gateway <preflight|routes>")
	}
	switch args[0] {
	case "preflight":
		return runGatewayPreflight(ctx, args[1:], client)
	case "routes":
		return runGatewayRoutes(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce gateway <preflight|routes>")
	}
}

func runGatewayRoutes(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("gateway routes", flag.ContinueOnError)
	configPath := fs.String("config", defaultPeerConfigPath(), "peer config JSON")
	live := fs.Bool("live", false, "fetch the live gateway /v1/models catalog and compare configured routes")
	url := fs.String("url", "", "live gateway URL override for --live")
	gatewayTokenEnv := fs.String("gateway-token-env", "", "environment variable containing the Mycelium gateway bearer token for --live")
	gatewayTokenFile := fs.String("gateway-token-file", "", "file containing the Mycelium gateway bearer token for --live")
	minContextCap := fs.Int("min-context-cap", 0, "mark routes below this context cap as undersized")
	requireTools := fs.Bool("require-tools", false, "mark routes without vLLM tool-parser configuration as not ready")
	requireAutoTools := fs.Bool("require-auto-tools", false, "mark routes without vLLM auto-tool-choice configuration as not ready")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := buildGatewayRoutesReport(ctx, *configPath, gatewayRoutesLiveOptions{
		Enabled:   *live,
		URL:       *url,
		TokenEnv:  *gatewayTokenEnv,
		TokenFile: *gatewayTokenFile,
		Client:    client,
	})
	if err != nil {
		return err
	}
	annotateGatewayRoutesReadiness(&report, gatewayRouteReadinessOptions{
		MinContextCap:    *minContextCap,
		RequireTools:     *requireTools,
		RequireAutoTools: *requireAutoTools,
	})
	if *jsonOut {
		return writePrettyJSON(os.Stdout, report)
	}
	printGatewayRoutesReport(os.Stdout, report)
	return nil
}

type gatewayRoutesReport struct {
	ConfigPath string                 `json:"config_path"`
	PeerID     string                 `json:"peer_id"`
	Gateway    string                 `json:"gateway"`
	BaseURL    string                 `json:"base_url"`
	Live       gatewayRoutesLive      `json:"live,omitempty"`
	Routes     []gatewayRouteContract `json:"routes"`
}

type gatewayRoutesLive struct {
	Checked    bool     `json:"checked"`
	Gateway    string   `json:"gateway,omitempty"`
	AuthSource string   `json:"auth_source,omitempty"`
	Status     string   `json:"status,omitempty"`
	Error      string   `json:"error,omitempty"`
	ModelCount int      `json:"model_count,omitempty"`
	ModelIDs   []string `json:"model_ids,omitempty"`
}

type gatewayRouteContract struct {
	Project                      string              `json:"project"`
	Model                        string              `json:"model"`
	PresetID                     string              `json:"preset_id,omitempty"`
	NodeID                       string              `json:"node_id,omitempty"`
	Backend                      domain.Backend      `json:"backend,omitempty"`
	ProviderProfile              string              `json:"provider_profile,omitempty"`
	ContextLength                int                 `json:"context_length,omitempty"`
	ProjectContextCap            int                 `json:"project_context_cap,omitempty"`
	Capabilities                 []domain.Capability `json:"capabilities,omitempty"`
	ProjectTokenConfigured       bool                `json:"project_token_configured"`
	GlobalGatewayTokenConfigured bool                `json:"global_gateway_token_configured"`
	AuthHint                     string              `json:"auth_hint"`
	StructuredOutputLikely       bool                `json:"structured_output_likely"`
	StreamingLikely              bool                `json:"streaming_likely"`
	ToolParserConfigured         bool                `json:"tool_parser_configured"`
	AutoToolChoiceConfigured     bool                `json:"auto_tool_choice_configured"`
	RecommendedToolChoice        string              `json:"recommended_tool_choice,omitempty"`
	ToolReadiness                string              `json:"tool_readiness,omitempty"`
	RouteReadiness               string              `json:"route_readiness,omitempty"`
	MinContextCap                int                 `json:"min_context_cap,omitempty"`
	EffectiveContextCap          int                 `json:"effective_context_cap,omitempty"`
	ContextAdequate              bool                `json:"context_adequate,omitempty"`
	ContextIssue                 string              `json:"context_issue,omitempty"`
	DiscoveryStatus              string              `json:"discovery_status"`
	LiveModelAvailable           bool                `json:"live_model_available,omitempty"`
	LiveModelMatch               string              `json:"live_model_match,omitempty"`
}

type gatewayRouteReadinessOptions struct {
	MinContextCap    int
	RequireTools     bool
	RequireAutoTools bool
}

type gatewayRoutesLiveOptions struct {
	Enabled   bool
	URL       string
	TokenEnv  string
	TokenFile string
	Client    *http.Client
}

func buildGatewayRoutesReport(ctx context.Context, configPath string, live gatewayRoutesLiveOptions) (gatewayRoutesReport, error) {
	loaded, err := loadOperatorConfig(configPath)
	if err != nil {
		return gatewayRoutesReport{}, err
	}
	cfg := loaded.Config
	gateway := adminBaseURL(cfg.Listen)
	report := gatewayRoutesReport{
		ConfigPath: loaded.Path,
		PeerID:     cfg.ID,
		Gateway:    gateway,
		BaseURL:    strings.TrimRight(gateway, "/") + "/v1",
	}
	if gateway == "" {
		report.BaseURL = ""
	}
	var liveModels map[string]struct{}
	if live.Enabled {
		liveReport, models := fetchGatewayRoutesLiveCatalog(ctx, loaded.Path, gateway, live)
		report.Live = liveReport
		liveModels = models
	}
	presets := cfg.Presets
	sort.Slice(presets, func(i, j int) bool { return presets[i].ID < presets[j].ID })
	projects := cfg.Projects
	sort.Slice(projects, func(i, j int) bool { return projects[i].ID < projects[j].ID })
	if len(projects) == 0 {
		for _, preset := range presets {
			route := routeContractForPreset(cfg, domain.Project{}, preset)
			annotateGatewayRouteLive(&route, preset, liveModels)
			report.Routes = append(report.Routes, route)
		}
		return report, nil
	}
	for _, project := range projects {
		route := gatewayRouteContract{
			Project:                      project.ID,
			Model:                        project.DefaultModel,
			ProjectContextCap:            project.ContextCap,
			ProjectTokenConfigured:       projectTokenConfigured(cfg, project.ID),
			GlobalGatewayTokenConfigured: strings.TrimSpace(cfg.GatewayToken) != "",
			AuthHint:                     gatewayRoutesAuthHint(cfg, project.ID),
			DiscoveryStatus:              "preset_missing",
		}
		if project.DefaultModel == "" {
			route.DiscoveryStatus = "project_default_model_missing"
			report.Routes = append(report.Routes, route)
			continue
		}
		if preset, ok := findPresetForModel(presets, project.DefaultModel); ok {
			route = routeContractForPreset(cfg, project, preset)
			annotateGatewayRouteLive(&route, preset, liveModels)
		}
		report.Routes = append(report.Routes, route)
	}
	return report, nil
}

func fetchGatewayRoutesLiveCatalog(ctx context.Context, configPath, configGateway string, opts gatewayRoutesLiveOptions) (gatewayRoutesLive, map[string]struct{}) {
	live := gatewayRoutesLive{Checked: true}
	gatewayURL := firstNonEmpty(opts.URL, configGateway)
	live.Gateway = gatewayURL
	if gatewayURL == "" {
		live.Status = "unavailable"
		live.Error = "no gateway URL configured"
		return live, nil
	}
	token, source, err := resolveGatewayPreflightToken(gatewayURL, "", opts.TokenEnv, opts.TokenFile, configPath)
	if err != nil {
		live.Status = "auth_error"
		live.Error = err.Error()
		return live, nil
	}
	live.AuthSource = source
	models, err := fetchGatewayLiveModels(ctx, gatewayURL, token, opts.Client)
	if err != nil {
		live.Status = "error"
		live.Error = err.Error()
		return live, nil
	}
	sort.Strings(models)
	live.Status = "ok"
	live.ModelCount = len(models)
	live.ModelIDs = models
	modelSet := map[string]struct{}{}
	for _, model := range models {
		modelSet[model] = struct{}{}
	}
	return live, modelSet
}

func fetchGatewayLiveModels(ctx context.Context, gatewayURL, token string, client *http.Client) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(gatewayURL, "/")+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := clientOrDefault(client).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := readControlHTTPBody(resp.Body, "gateway models response body")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("gateway models returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var parsed struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("decode gateway models response: %w", err)
	}
	models := make([]string, 0, len(parsed.Data))
	seen := map[string]struct{}{}
	for _, item := range parsed.Data {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		models = append(models, id)
	}
	return models, nil
}

func checkGatewayPreflightLiveModel(ctx context.Context, gatewayURL, model, token, authSource string, client *http.Client) gatewayPreflightLiveModelEvidence {
	live := gatewayPreflightLiveModelEvidence{
		Checked:    true,
		Gateway:    strings.TrimRight(gatewayURL, "/"),
		Model:      model,
		AuthSource: authSource,
	}
	models, err := fetchGatewayLiveModels(ctx, gatewayURL, token, client)
	if err != nil {
		live.Status = "error"
		live.Error = err.Error()
		return live
	}
	live.ModelCount = len(models)
	for _, id := range models {
		if id == model {
			live.Status = "ok"
			live.ModelAvailable = true
			live.ModelMatch = id
			return live
		}
	}
	live.Status = "missing"
	return live
}

func printGatewayPreflightLiveModel(w io.Writer, live gatewayPreflightLiveModelEvidence) {
	fmt.Fprintf(w, "live_model\tstatus=%s\tavailable=%t\tmatch=%s\tmodels=%d\terror=%s\n",
		emptyDash(live.Status),
		live.ModelAvailable,
		emptyDash(live.ModelMatch),
		live.ModelCount,
		emptyDash(live.Error),
	)
}

func annotateGatewayRouteLive(route *gatewayRouteContract, preset domain.Preset, liveModels map[string]struct{}) {
	if liveModels == nil {
		return
	}
	for _, id := range append([]string{route.Model, preset.ID}, preset.Aliases...) {
		if _, ok := liveModels[id]; ok {
			route.LiveModelAvailable = true
			route.LiveModelMatch = id
			return
		}
	}
	route.LiveModelAvailable = false
}

func routeContractForPreset(cfg operatorPeerConfig, project domain.Project, preset domain.Preset) gatewayRouteContract {
	model := project.DefaultModel
	if model == "" {
		model = preferredPresetModel(preset)
	}
	contract := gatewayRouteContract{
		Project:                      project.ID,
		Model:                        model,
		PresetID:                     preset.ID,
		NodeID:                       preset.NodeID,
		Backend:                      preset.Backend,
		ProviderProfile:              preset.ProviderProfile,
		ContextLength:                modelruntime.EffectiveContextLength(preset),
		ProjectContextCap:            project.ContextCap,
		Capabilities:                 append([]domain.Capability(nil), preset.Capabilities...),
		ProjectTokenConfigured:       project.ID != "" && projectTokenConfigured(cfg, project.ID),
		GlobalGatewayTokenConfigured: strings.TrimSpace(cfg.GatewayToken) != "",
		AuthHint:                     gatewayRoutesAuthHint(cfg, project.ID),
		StructuredOutputLikely:       preset.Backend == domain.BackendVLLM,
		StreamingLikely:              preset.Backend == domain.BackendVLLM,
		ToolParserConfigured:         launchArgConfigured(preset.LaunchArgs, "--tool-call-parser"),
		AutoToolChoiceConfigured:     launchArgConfigured(preset.LaunchArgs, "--enable-auto-tool-choice"),
		DiscoveryStatus:              "ready_from_config",
	}
	contract.ToolReadiness = gatewayRouteToolReadiness(contract)
	if contract.ToolParserConfigured {
		if contract.AutoToolChoiceConfigured {
			contract.RecommendedToolChoice = "forced-function-first; auto requires model/template proof"
		} else {
			contract.RecommendedToolChoice = "forced-function-only"
		}
	}
	return contract
}

func printGatewayRoutesReport(w io.Writer, report gatewayRoutesReport) {
	fmt.Fprintf(w, "gateway\t%s\tbase_url=%s\tpeer=%s\tconfig=%s\n", emptyDash(report.Gateway), emptyDash(report.BaseURL), emptyDash(report.PeerID), report.ConfigPath)
	if report.Live.Checked {
		fmt.Fprintf(w, "live\tgateway=%s\tstatus=%s\tmodels=%d\tauth=%s\terror=%s\n", emptyDash(report.Live.Gateway), emptyDash(report.Live.Status), report.Live.ModelCount, emptyDash(report.Live.AuthSource), emptyDash(report.Live.Error))
	}
	for _, route := range report.Routes {
		fmt.Fprintf(w, "route\tproject=%s\tmodel=%s\tpreset=%s\tnode=%s\tbackend=%s\tcontext=%d\tproject_cap=%d\teffective_context=%d\tmin_context=%d\tcontext_issue=%s\treadiness=%s\ttool_readiness=%s\tstatus=%s\tauth=%s\tproject_token=%t\tglobal_token=%t\tstructured=%t\tstreaming=%t\ttool_parser=%t\tauto_tool=%t\ttool_choice=%s\tlive_model=%t\tlive_match=%s\n",
			emptyDash(route.Project),
			emptyDash(route.Model),
			emptyDash(route.PresetID),
			emptyDash(route.NodeID),
			emptyDash(string(route.Backend)),
			route.ContextLength,
			route.ProjectContextCap,
			route.EffectiveContextCap,
			route.MinContextCap,
			emptyDash(route.ContextIssue),
			emptyDash(route.RouteReadiness),
			emptyDash(route.ToolReadiness),
			route.DiscoveryStatus,
			route.AuthHint,
			route.ProjectTokenConfigured,
			route.GlobalGatewayTokenConfigured,
			route.StructuredOutputLikely,
			route.StreamingLikely,
			route.ToolParserConfigured,
			route.AutoToolChoiceConfigured,
			emptyDash(route.RecommendedToolChoice),
			route.LiveModelAvailable,
			emptyDash(route.LiveModelMatch),
		)
	}
}

func annotateGatewayRoutesReadiness(report *gatewayRoutesReport, opts gatewayRouteReadinessOptions) {
	for i := range report.Routes {
		effective := effectiveRouteContextCap(report.Routes[i])
		report.Routes[i].EffectiveContextCap = effective
		report.Routes[i].ToolReadiness = gatewayRouteToolReadiness(report.Routes[i])
		if opts.MinContextCap > 0 {
			report.Routes[i].MinContextCap = opts.MinContextCap
			if effective >= opts.MinContextCap {
				report.Routes[i].ContextAdequate = true
			} else {
				report.Routes[i].ContextIssue = "undersized_context"
			}
		}
		report.Routes[i].RouteReadiness = gatewayRouteReadiness(report.Routes[i], report.Live, opts)
	}
}

func gatewayRouteToolReadiness(route gatewayRouteContract) string {
	switch {
	case route.ToolParserConfigured && route.AutoToolChoiceConfigured:
		return "auto_tool_configured"
	case route.ToolParserConfigured:
		return "forced_function_configured"
	default:
		return "not_tool_configured"
	}
}

func gatewayRouteReadiness(route gatewayRouteContract, live gatewayRoutesLive, opts gatewayRouteReadinessOptions) string {
	if route.DiscoveryStatus != "ready_from_config" {
		return route.DiscoveryStatus
	}
	if live.Checked && (live.Status != "ok" || !route.LiveModelAvailable) {
		return "live_catalog_missing"
	}
	if route.ContextIssue != "" {
		return route.ContextIssue
	}
	if (opts.RequireTools || opts.RequireAutoTools) && !route.ToolParserConfigured {
		return "tool_flags_missing"
	}
	if opts.RequireAutoTools && !route.AutoToolChoiceConfigured {
		return "auto_tool_flags_missing"
	}
	if live.Checked && route.LiveModelAvailable {
		return "live_ready"
	}
	return "config_ready"
}

func effectiveRouteContextCap(route gatewayRouteContract) int {
	effective := route.ContextLength
	if route.ProjectContextCap > 0 && (effective == 0 || route.ProjectContextCap < effective) {
		effective = route.ProjectContextCap
	}
	return effective
}

func findPresetForModel(presets []domain.Preset, model string) (domain.Preset, bool) {
	for _, preset := range presets {
		if preset.ID == model || preset.ModelRef == model {
			return preset, true
		}
		for _, alias := range preset.Aliases {
			if alias == model {
				return preset, true
			}
		}
	}
	return domain.Preset{}, false
}

func preferredPresetModel(preset domain.Preset) string {
	if len(preset.Aliases) > 0 {
		return preset.Aliases[0]
	}
	if preset.ModelRef != "" {
		return preset.ModelRef
	}
	return preset.ID
}

func projectTokenConfigured(cfg operatorPeerConfig, projectID string) bool {
	for _, token := range cfg.GatewayProjectTokens {
		if token.Project == projectID && strings.TrimSpace(token.Token) != "" {
			return true
		}
	}
	return false
}

func gatewayRoutesAuthHint(cfg operatorPeerConfig, projectID string) string {
	if projectID != "" && projectTokenConfigured(cfg, projectID) {
		return "project_token"
	}
	if strings.TrimSpace(cfg.GatewayToken) != "" {
		return "global_gateway_token"
	}
	return "missing"
}

func launchArgConfigured(args []string, flagName string) bool {
	for _, arg := range args {
		if arg == flagName || strings.HasPrefix(arg, flagName+"=") {
			return true
		}
	}
	return false
}

func runGatewayPreflight(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("gateway preflight", flag.ContinueOnError)
	url := fs.String("url", "", "Mycelium gateway URL")
	model := fs.String("model", "", "model or preset alias")
	gatewayToken := fs.String("gateway-token", "", "Mycelium gateway bearer token")
	gatewayTokenEnv := fs.String("gateway-token-env", "", "environment variable containing the Mycelium gateway bearer token")
	gatewayTokenFile := fs.String("gateway-token-file", "", "file containing the Mycelium gateway bearer token")
	configPath := fs.String("config", "", "peer config JSON to read gateway_token from")
	project := fs.String("project", "", "optional project id to send as X-Myc-Project")
	prompt := fs.String("prompt", `Return {"ok":true}.`, "preflight prompt")
	maxTokens := fs.Int("max-tokens", 16, "maximum output tokens for the preflight request")
	contextCap := fs.Int("context-cap", 0, "known selected route context cap to preserve in evidence")
	toolName := fs.String("tool-name", "", "optional echo-style function name for an OpenAI tool-call canary")
	toolChoice := fs.String("tool-choice", "", "tool choice for --tool-name: auto, required, none, or function")
	expectNode := fs.String("expect-node", "", "fail unless X-Myc-Node matches this node")
	requireJSON := fs.Bool("require-json", false, "fail unless the final content is valid JSON")
	requireToolCall := fs.Bool("require-tool-call", false, "fail unless the response contains at least one structured OpenAI tool call")
	rejectTextualToolCall := fs.Bool("reject-textual-tool-call", false, "fail if final content contains a textual <tool_call> block")
	requireLiveModel := fs.Bool("require-live-model", false, "fail unless authenticated /v1/models exposes the requested model before running the preflight")
	minPromptTokens := fs.Int("min-prompt-tokens", 0, "fail unless usage.prompt_tokens is at least this value")
	minContextCap := fs.Int("min-context-cap", 0, "fail unless --context-cap is at least this value")
	expectFinishReason := fs.String("expect-finish-reason", "", "fail unless finish_reason matches this value")
	minContentLength := fs.Int("min-content-length", 0, "fail unless final content has at least this many bytes")
	maxReasoningLength := fs.Int("max-reasoning-length", -1, "fail unless reasoning content has at most this many bytes")
	stream := fs.Bool("stream", false, "send a streaming request and record stream completion/tool/usage evidence")
	outDir := fs.String("out", "", "optional directory for a redacted preflight evidence packet")
	consumerRun := fs.String("consumer-run", "", "consumer batch/run id to preserve in preflight evidence")
	consumerItem := fs.String("consumer-item", "", "consumer item/row/document id to preserve in preflight evidence")
	consumerStatus := fs.String("consumer-status", "", "consumer outcome: ok, timeout, error, canceled, or partial")
	consumerElapsedMS := fs.Int("consumer-elapsed-ms", 0, "consumer-observed elapsed milliseconds")
	consumerError := fs.String("consumer-error", "", "sanitized consumer error class or short message")
	consumerResult := fs.String("consumer-result", "", "consumer validation result: pass, fail, or unknown")
	consumerQualityGate := fs.String("consumer-quality-gate", "", "sanitized quality or validation gate name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *url == "" {
		return fmt.Errorf("--url is required")
	}
	if *model == "" {
		return fmt.Errorf("--model is required")
	}
	consumer, err := consumerEvidenceFromFlags(*consumerRun, *consumerItem, *consumerStatus, *consumerElapsedMS, *consumerError, *consumerResult, *consumerQualityGate)
	if err != nil {
		return err
	}
	token, tokenSource, err := resolveGatewayPreflightToken(*url, *gatewayToken, *gatewayTokenEnv, *gatewayTokenFile, *configPath)
	if err != nil {
		return err
	}
	cfg := gatewayPreflightConfig{
		URL:        *url,
		Model:      *model,
		Prompt:     *prompt,
		MaxTokens:  *maxTokens,
		ContextCap: *contextCap,
		Project:    *project,
		ToolName:   *toolName,
		ToolChoice: *toolChoice,
		Token:      token,
		Client:     client,
		Stream:     *stream,
	}
	expectations := gatewayPreflightExpectations{
		ExpectNode:            *expectNode,
		RequireJSON:           *requireJSON,
		RequireToolCall:       *requireToolCall,
		RejectTextualToolCall: *rejectTextualToolCall,
		RequireLiveModel:      *requireLiveModel,
		MinPromptTokens:       *minPromptTokens,
		MinContextCap:         *minContextCap,
		ExpectFinishReason:    *expectFinishReason,
		MinContentLength:      *minContentLength,
		MaxReasoningLength:    *maxReasoningLength,
	}
	var liveModel *gatewayPreflightLiveModelEvidence
	if *requireLiveModel {
		live := checkGatewayPreflightLiveModel(ctx, cfg.URL, cfg.Model, cfg.Token, tokenSource, cfg.Client)
		liveModel = &live
		printGatewayPreflightLiveModel(os.Stdout, live)
		if live.Status != "ok" || !live.ModelAvailable {
			result := gatewayPreflightResult{
				Headers:    http.Header{},
				ContextCap: cfg.ContextCap,
				LiveModel:  liveModel,
			}
			var evidencePath string
			if *outDir != "" {
				evidenceCfg := sanitizedGatewayPreflightEvidenceConfig(cfg, consumer)
				path, err := writeGatewayPreflightEvidence(*outDir, tokenSource, evidenceCfg, result, expectations)
				if err != nil {
					return err
				}
				evidencePath = path
				fmt.Printf("evidence\t%s\n", path)
			}
			err := fmt.Errorf("gateway preflight live model check failed: status=%s available=%t match=%s error=%s", live.Status, live.ModelAvailable, emptyDash(live.ModelMatch), emptyDash(live.Error))
			if evidencePath != "" {
				return fmt.Errorf("%w; evidence=%s", err, evidencePath)
			}
			return err
		}
	}
	result, err := gatewayPreflightComplete(ctx, cfg)
	if err != nil {
		result.LiveModel = liveModel
		var evidencePath string
		if *outDir != "" {
			evidenceCfg := sanitizedGatewayPreflightEvidenceConfig(cfg, consumer)
			path, writeErr := writeGatewayPreflightEvidence(*outDir, tokenSource, evidenceCfg, result, expectations)
			if writeErr != nil {
				return writeErr
			}
			evidencePath = path
			fmt.Printf("evidence\t%s\n", path)
		}
		if evidencePath != "" {
			return fmt.Errorf("%w; evidence=%s", err, evidencePath)
		}
		return err
	}
	result.LiveModel = liveModel
	fmt.Printf("gateway\t%s\tok\n", strings.TrimRight(*url, "/"))
	fmt.Printf("auth\t%s\tconfigured\n", tokenSource)
	fmt.Printf("status\t%d\n", result.StatusCode)
	for _, name := range gatewayPreflightHeaders() {
		fmt.Printf("header\t%s\t%s\n", name, emptyDash(result.Headers.Get(name)))
	}
	fmt.Printf("finish_reason\t%s\n", emptyDash(result.FinishReason))
	fmt.Printf("content_length\t%d\n", len(result.Content))
	fmt.Printf("reasoning_length\t%d\n", len(result.Reasoning))
	fmt.Printf("tool_calls\tcount=%d\tnames=%s\n", result.ToolCallCount, emptyDash(strings.Join(result.ToolCallNames, ",")))
	fmt.Printf("textual_tool_call\t%t\n", result.TextualToolCall)
	fmt.Printf("json\t%t\n", result.JSONContent)
	if result.Streaming {
		fmt.Printf("stream\tdone=%t\tusage_available=%t\n", result.StreamDone, result.UsageAvailable)
	}
	if result.UsageAvailable {
		fmt.Printf("usage\tprompt=%d\tcompletion=%d\ttotal=%d\n", result.Usage.PromptTokens, result.Usage.CompletionTokens, result.Usage.TotalTokens)
	} else if result.Streaming {
		fmt.Printf("usage\tunavailable\n")
	}
	if consumer != nil {
		printConsumerEvidenceSummary(os.Stdout, *consumer)
	}
	var evidencePath string
	if *outDir != "" {
		evidenceCfg := sanitizedGatewayPreflightEvidenceConfig(cfg, consumer)
		path, err := writeGatewayPreflightEvidence(*outDir, tokenSource, evidenceCfg, result, expectations)
		if err != nil {
			return err
		}
		evidencePath = path
		fmt.Printf("evidence\t%s\n", path)
	}
	if err := validateGatewayPreflightExpectations(result, expectations); err != nil {
		if evidencePath != "" {
			return fmt.Errorf("%w; evidence=%s", err, evidencePath)
		}
		return err
	}
	return nil
}

type gatewayPreflightExpectations struct {
	ExpectNode            string
	RequireJSON           bool
	RequireToolCall       bool
	RejectTextualToolCall bool
	RequireLiveModel      bool
	MinPromptTokens       int
	MinContextCap         int
	ExpectFinishReason    string
	MinContentLength      int
	MaxReasoningLength    int
}

func validateGatewayPreflightExpectations(result gatewayPreflightResult, expectations gatewayPreflightExpectations) error {
	var failures []string
	if expectations.ExpectNode != "" {
		if got := result.Headers.Get("X-Myc-Node"); got != expectations.ExpectNode {
			failures = append(failures, fmt.Sprintf("expected X-Myc-Node=%s got %s", expectations.ExpectNode, emptyDash(got)))
		}
	}
	if result.Streaming && !result.StreamDone {
		failures = append(failures, "expected terminal streaming [DONE] event")
	}
	if expectations.ExpectFinishReason != "" && result.FinishReason != expectations.ExpectFinishReason {
		failures = append(failures, fmt.Sprintf("expected finish_reason=%s got %s", expectations.ExpectFinishReason, emptyDash(result.FinishReason)))
	}
	if expectations.RequireJSON && !result.JSONContent {
		failures = append(failures, "expected valid JSON final content")
	}
	if expectations.MinContentLength > 0 && len(result.Content) < expectations.MinContentLength {
		failures = append(failures, fmt.Sprintf("expected content length >= %d got %d", expectations.MinContentLength, len(result.Content)))
	}
	if expectations.MaxReasoningLength >= 0 && len(result.Reasoning) > expectations.MaxReasoningLength {
		failures = append(failures, fmt.Sprintf("expected reasoning length <= %d got %d", expectations.MaxReasoningLength, len(result.Reasoning)))
	}
	if expectations.RequireToolCall && result.ToolCallCount == 0 {
		failures = append(failures, "expected at least one structured tool call")
	}
	if expectations.RejectTextualToolCall && result.TextualToolCall {
		failures = append(failures, "expected no textual <tool_call> block in final content")
	}
	if expectations.RequireLiveModel {
		switch {
		case result.LiveModel == nil:
			failures = append(failures, "expected authenticated /v1/models live model check")
		case result.LiveModel.Status != "ok":
			failures = append(failures, fmt.Sprintf("expected live model catalog status=ok got %s", emptyDash(result.LiveModel.Status)))
		case !result.LiveModel.ModelAvailable:
			failures = append(failures, fmt.Sprintf("expected live model catalog to expose %s", result.LiveModel.Model))
		}
	}
	if expectations.MinPromptTokens > 0 && !result.UsageAvailable {
		failures = append(failures, fmt.Sprintf("expected prompt_tokens >= %d but usage was unavailable", expectations.MinPromptTokens))
	} else if expectations.MinPromptTokens > 0 && result.Usage.PromptTokens < expectations.MinPromptTokens {
		failures = append(failures, fmt.Sprintf("expected prompt_tokens >= %d got %d", expectations.MinPromptTokens, result.Usage.PromptTokens))
	}
	if expectations.MinContextCap > 0 && result.ContextCap == 0 {
		failures = append(failures, fmt.Sprintf("expected context cap >= %d but --context-cap was not set", expectations.MinContextCap))
	} else if expectations.MinContextCap > 0 && result.ContextCap < expectations.MinContextCap {
		failures = append(failures, fmt.Sprintf("expected context cap >= %d got %d", expectations.MinContextCap, result.ContextCap))
	}
	if len(failures) > 0 {
		return fmt.Errorf("gateway preflight expectation failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

type gatewayPreflightConfig struct {
	URL        string
	Model      string
	Prompt     string
	MaxTokens  int
	ContextCap int
	Project    string
	ToolName   string
	ToolChoice string
	Token      string
	Client     *http.Client
	Stream     bool
	Consumer   *consumerEvidence
}

type gatewayPreflightResult struct {
	StatusCode      int
	Error           string
	Headers         http.Header
	Streaming       bool
	StreamDone      bool
	FinishReason    string
	Content         string
	Reasoning       string
	ToolCallCount   int
	ToolCallNames   []string
	TextualToolCall bool
	JSONContent     bool
	UsageAvailable  bool
	Usage           api.OpenAIUsage
	ContextCap      int
	LiveModel       *gatewayPreflightLiveModelEvidence
}

type gatewayPreflightLiveModelEvidence struct {
	Checked        bool   `json:"checked"`
	Gateway        string `json:"gateway,omitempty"`
	Model          string `json:"model,omitempty"`
	Status         string `json:"status,omitempty"`
	ModelAvailable bool   `json:"model_available,omitempty"`
	ModelMatch     string `json:"model_match,omitempty"`
	ModelCount     int    `json:"model_count,omitempty"`
	AuthSource     string `json:"auth_source,omitempty"`
	Error          string `json:"error,omitempty"`
}

type gatewayPreflightEvidence struct {
	Gateway         string                               `json:"gateway"`
	Model           string                               `json:"model"`
	Project         string                               `json:"project,omitempty"`
	AuthSource      string                               `json:"auth_source"`
	Request         gatewayPreflightRequestEvidence      `json:"request"`
	Expectations    *gatewayPreflightExpectationEvidence `json:"expectations,omitempty"`
	StatusCode      int                                  `json:"status_code"`
	Error           string                               `json:"error,omitempty"`
	Headers         map[string]string                    `json:"headers"`
	Streaming       bool                                 `json:"streaming"`
	StreamDone      bool                                 `json:"stream_done,omitempty"`
	FinishReason    string                               `json:"finish_reason"`
	ContentLength   int                                  `json:"content_length"`
	ReasoningLength int                                  `json:"reasoning_length"`
	ToolCallCount   int                                  `json:"tool_call_count"`
	ToolCallNames   []string                             `json:"tool_call_names,omitempty"`
	TextualToolCall bool                                 `json:"textual_tool_call"`
	JSONContent     bool                                 `json:"json_content"`
	UsageAvailable  bool                                 `json:"usage_available"`
	Usage           api.OpenAIUsage                      `json:"usage"`
	LiveModel       *gatewayPreflightLiveModelEvidence   `json:"live_model,omitempty"`
	Consumer        *consumerEvidence                    `json:"consumer,omitempty"`
}

type gatewayPreflightRequestEvidence struct {
	MaxTokens        int    `json:"max_tokens"`
	ContextCap       int    `json:"context_cap,omitempty"`
	Streaming        bool   `json:"streaming"`
	StructuredOutput string `json:"structured_output"`
	ToolRequested    bool   `json:"tool_requested"`
	ToolName         string `json:"tool_name,omitempty"`
	ToolChoice       string `json:"tool_choice,omitempty"`
}

type gatewayPreflightExpectationEvidence struct {
	ExpectNode            string `json:"expect_node,omitempty"`
	RequireJSON           bool   `json:"require_json,omitempty"`
	RequireToolCall       bool   `json:"require_tool_call,omitempty"`
	RejectTextualToolCall bool   `json:"reject_textual_tool_call,omitempty"`
	RequireLiveModel      bool   `json:"require_live_model,omitempty"`
	MinPromptTokens       int    `json:"min_prompt_tokens,omitempty"`
	MinContextCap         int    `json:"min_context_cap,omitempty"`
	ExpectFinishReason    string `json:"expect_finish_reason,omitempty"`
	MinContentLength      int    `json:"min_content_length,omitempty"`
	MaxReasoningLength    *int   `json:"max_reasoning_length,omitempty"`
}

func gatewayPreflightRequestEvidenceFromConfig(cfg gatewayPreflightConfig) gatewayPreflightRequestEvidence {
	toolName := strings.TrimSpace(cfg.ToolName)
	toolChoice := strings.TrimSpace(cfg.ToolChoice)
	if toolName != "" && toolChoice == "" {
		toolChoice = "auto"
	}
	structuredOutput := "json_schema"
	if toolName != "" {
		structuredOutput = "none"
	}
	return gatewayPreflightRequestEvidence{
		MaxTokens:        cfg.MaxTokens,
		ContextCap:       cfg.ContextCap,
		Streaming:        cfg.Stream,
		StructuredOutput: structuredOutput,
		ToolRequested:    toolName != "",
		ToolName:         toolName,
		ToolChoice:       toolChoice,
	}
}

func gatewayPreflightExpectationEvidenceFromExpectations(expectations gatewayPreflightExpectations) *gatewayPreflightExpectationEvidence {
	if expectations.ExpectNode == "" && !expectations.RequireJSON && !expectations.RequireToolCall && !expectations.RejectTextualToolCall && !expectations.RequireLiveModel && expectations.MinPromptTokens == 0 && expectations.MinContextCap == 0 && expectations.ExpectFinishReason == "" && expectations.MinContentLength == 0 && expectations.MaxReasoningLength < 0 {
		return nil
	}
	out := &gatewayPreflightExpectationEvidence{
		ExpectNode:            expectations.ExpectNode,
		RequireJSON:           expectations.RequireJSON,
		RequireToolCall:       expectations.RequireToolCall,
		RejectTextualToolCall: expectations.RejectTextualToolCall,
		RequireLiveModel:      expectations.RequireLiveModel,
		MinPromptTokens:       expectations.MinPromptTokens,
		MinContextCap:         expectations.MinContextCap,
		ExpectFinishReason:    expectations.ExpectFinishReason,
		MinContentLength:      expectations.MinContentLength,
	}
	if expectations.MaxReasoningLength >= 0 {
		out.MaxReasoningLength = &expectations.MaxReasoningLength
	}
	return out
}

func writeGatewayPreflightEvidence(outDir, authSource string, cfg gatewayPreflightConfig, result gatewayPreflightResult, expectations gatewayPreflightExpectations) (string, error) {
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return "", fmt.Errorf("create gateway preflight evidence dir: %w", err)
	}
	evidence := gatewayPreflightEvidence{
		Gateway:         strings.TrimRight(cfg.URL, "/"),
		Model:           cfg.Model,
		Project:         cfg.Project,
		AuthSource:      authSource,
		Request:         gatewayPreflightRequestEvidenceFromConfig(cfg),
		Expectations:    gatewayPreflightExpectationEvidenceFromExpectations(expectations),
		StatusCode:      result.StatusCode,
		Error:           result.Error,
		Headers:         gatewayPreflightHeaderMap(result.Headers),
		Streaming:       result.Streaming,
		StreamDone:      result.StreamDone,
		FinishReason:    result.FinishReason,
		ContentLength:   len(result.Content),
		ReasoningLength: len(result.Reasoning),
		ToolCallCount:   result.ToolCallCount,
		ToolCallNames:   result.ToolCallNames,
		TextualToolCall: result.TextualToolCall,
		JSONContent:     result.JSONContent,
		UsageAvailable:  result.UsageAvailable,
		Usage:           result.Usage,
		LiveModel:       result.LiveModel,
		Consumer:        cfg.Consumer,
	}
	path := filepath.Join(outDir, "preflight.json")
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("write gateway preflight evidence: %w", err)
	}
	return path, nil
}

func sanitizedGatewayPreflightEvidenceConfig(cfg gatewayPreflightConfig, consumer *consumerEvidence) gatewayPreflightConfig {
	cfg.Token = ""
	cfg.Client = nil
	cfg.Prompt = ""
	cfg.Consumer = consumer
	return cfg
}

func gatewayPreflightHeaderMap(headers http.Header) map[string]string {
	out := map[string]string{}
	for _, name := range gatewayPreflightHeaders() {
		if value := headers.Get(name); value != "" {
			out[name] = value
		}
	}
	return out
}

func resolveGatewayPreflightToken(rawURL, token, tokenEnv, tokenFile, configPath string) (string, string, error) {
	if token != "" {
		return token, "--gateway-token", nil
	}
	if tokenEnv != "" {
		value := strings.TrimSpace(os.Getenv(tokenEnv))
		if value == "" {
			return "", "", fmt.Errorf("gateway token env %s is not set", tokenEnv)
		}
		return value, tokenEnv, nil
	}
	if tokenFile != "" {
		value, err := readGatewayTokenFile(tokenFile)
		if err != nil {
			return "", "", err
		}
		return value, tokenFile, nil
	}
	if configPath != "" {
		value, err := gatewayTokenFromConfig(configPath)
		if err != nil {
			return "", "", err
		}
		if value == "" {
			return "", "", fmt.Errorf("peer config %s has no gateway_token", configPath)
		}
		return value, configPath + " gateway_token", nil
	}
	token, source, err := bench.ResolveGatewayTokenSourceForURL(rawURL, "", "")
	if err != nil {
		return "", "", fmt.Errorf("%w; checked env %s, --gateway-token-file, --config gateway_token, and ~/.mycelium/peer.json gateway_token; pass --gateway-token-env OPENAI_API_KEY only if that variable intentionally contains a Mycelium gateway token", err, strings.Join(bench.DefaultGatewayTokenEnvNames(), ", "))
	}
	if token != "" {
		return token, source, nil
	}
	return "", "none", nil
}

func gatewayTokenFromConfig(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read peer config %s: %w", path, err)
	}
	var cfg struct {
		GatewayToken string `json:"gateway_token"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", fmt.Errorf("parse peer config %s: %w", path, err)
	}
	return strings.TrimSpace(cfg.GatewayToken), nil
}

func readGatewayTokenFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read gateway token file %s: %w", path, err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("gateway token file %s is empty", path)
	}
	return token, nil
}

func gatewayPreflightComplete(ctx context.Context, cfg gatewayPreflightConfig) (gatewayPreflightResult, error) {
	payload := map[string]any{
		"model": cfg.Model,
		"messages": []map[string]string{{
			"role":    "user",
			"content": cfg.Prompt,
		}},
		"max_tokens":  cfg.MaxTokens,
		"temperature": 0,
	}
	if cfg.Stream {
		payload["stream"] = true
		payload["stream_options"] = map[string]any{"include_usage": true}
	}
	if cfg.ToolName != "" {
		payload["tools"] = []map[string]any{{
			"type":   "function",
			"strict": true,
			"function": map[string]any{
				"name":        cfg.ToolName,
				"description": "Echo a value for Mycelium tool-call preflight.",
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"value": map[string]string{"type": "string"},
					},
					"required":             []string{"value"},
					"additionalProperties": false,
				},
			},
		}}
		switch choice := strings.TrimSpace(cfg.ToolChoice); choice {
		case "", "auto", "required", "none":
			if choice == "" {
				choice = "auto"
			}
			payload["tool_choice"] = choice
		case "function":
			payload["tool_choice"] = map[string]any{
				"type": "function",
				"function": map[string]string{
					"name": cfg.ToolName,
				},
			}
		default:
			return gatewayPreflightResult{}, fmt.Errorf("--tool-choice must be auto, required, none, or function")
		}
	} else {
		payload["response_format"] = map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "mycelium_preflight",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"ok": map[string]string{"type": "boolean"},
					},
					"required":             []string{"ok"},
					"additionalProperties": false,
				},
			},
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return gatewayPreflightResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(cfg.URL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return gatewayPreflightResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}
	if cfg.Project != "" {
		req.Header.Set("X-Myc-Project", cfg.Project)
	}
	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return gatewayPreflightResult{}, err
	}
	defer resp.Body.Close()
	data, err := readControlHTTPBody(resp.Body, "gateway preflight response body")
	if err != nil {
		return gatewayPreflightResult{}, err
	}
	if resp.StatusCode >= 400 {
		result := gatewayPreflightResult{
			StatusCode: resp.StatusCode,
			Error:      sanitizeGatewayPreflightError(resp.StatusCode, data),
			Headers:    resp.Header.Clone(),
			ContextCap: cfg.ContextCap,
		}
		return result, errors.New(result.Error)
	}
	var parsed gatewayPreflightResult
	if cfg.Stream {
		parsed, err = parseGatewayPreflightStream(data)
		if err != nil {
			return gatewayPreflightResult{}, err
		}
	} else {
		parsed, err = parseGatewayPreflightResponse(data)
		if err != nil {
			return gatewayPreflightResult{}, err
		}
	}
	parsed.StatusCode = resp.StatusCode
	parsed.Headers = resp.Header.Clone()
	parsed.ContextCap = cfg.ContextCap
	return parsed, nil
}

func sanitizeGatewayPreflightError(status int, body []byte) string {
	text := strings.Join(strings.Fields(string(body)), " ")
	if len(text) > 500 {
		text = text[:500]
	}
	if text == "" {
		return fmt.Sprintf("gateway preflight returned %d", status)
	}
	return fmt.Sprintf("gateway preflight returned %d: %s", status, text)
}

func parseGatewayPreflightResponse(data []byte) (gatewayPreflightResult, error) {
	var out struct {
		Choices []struct {
			Message struct {
				Content          string               `json:"content"`
				Reasoning        string               `json:"reasoning"`
				ReasoningContent string               `json:"reasoning_content"`
				ToolCalls        []api.OpenAIToolCall `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage api.OpenAIUsage `json:"usage"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return gatewayPreflightResult{}, fmt.Errorf("decode gateway preflight response: %w", err)
	}
	if len(out.Choices) == 0 {
		return gatewayPreflightResult{}, fmt.Errorf("gateway preflight response had no choices")
	}
	choice := out.Choices[0]
	reasoning := choice.Message.Reasoning
	if reasoning == "" {
		reasoning = choice.Message.ReasoningContent
	}
	toolNames := make([]string, 0, len(choice.Message.ToolCalls))
	for _, call := range choice.Message.ToolCalls {
		if call.Function.Name != "" {
			toolNames = append(toolNames, call.Function.Name)
		}
	}
	return gatewayPreflightResult{
		FinishReason:    choice.FinishReason,
		Content:         choice.Message.Content,
		Reasoning:       reasoning,
		ToolCallCount:   len(choice.Message.ToolCalls),
		ToolCallNames:   toolNames,
		TextualToolCall: looksLikeTextualToolCall(choice.Message.Content),
		JSONContent:     json.Valid([]byte(strings.TrimSpace(choice.Message.Content))),
		UsageAvailable:  usageAvailable(out.Usage),
		Usage:           out.Usage,
	}, nil
}

func parseGatewayPreflightStream(data []byte) (gatewayPreflightResult, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 4096), int(maxControlHTTPBodyBytes))
	var content strings.Builder
	var reasoning strings.Builder
	var finishReason string
	var usage api.OpenAIUsage
	var usageSeen bool
	var done bool
	toolNamesByIndex := map[int]string{}
	toolIDsByIndex := map[int]string{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if raw == "" {
			continue
		}
		if raw == "[DONE]" {
			done = true
			continue
		}
		part, err := parseGatewayPreflightStreamChunk(raw)
		if err != nil {
			return gatewayPreflightResult{}, err
		}
		content.WriteString(part.Content)
		reasoning.WriteString(part.Reasoning)
		if part.FinishReason != "" {
			finishReason = part.FinishReason
		}
		if part.Usage != nil {
			usage = *part.Usage
			usageSeen = true
		}
		for _, call := range part.ToolCalls {
			if call.ID != "" {
				toolIDsByIndex[call.Index] = call.ID
			}
			if call.Name != "" {
				toolNamesByIndex[call.Index] = call.Name
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return gatewayPreflightResult{}, err
	}
	toolNames := make([]string, 0, len(toolNamesByIndex))
	for index := 0; index <= maxIntKey(toolNamesByIndex); index++ {
		if name := toolNamesByIndex[index]; name != "" {
			toolNames = append(toolNames, name)
		}
	}
	if len(toolNames) == 0 {
		for index := 0; index <= maxIntKey(toolIDsByIndex); index++ {
			if id := toolIDsByIndex[index]; id != "" {
				toolNames = append(toolNames, id)
			}
		}
	}
	text := content.String()
	return gatewayPreflightResult{
		Streaming:       true,
		StreamDone:      done,
		FinishReason:    finishReason,
		Content:         text,
		Reasoning:       reasoning.String(),
		ToolCallCount:   len(toolNames),
		ToolCallNames:   toolNames,
		TextualToolCall: looksLikeTextualToolCall(text),
		JSONContent:     json.Valid([]byte(strings.TrimSpace(text))),
		UsageAvailable:  usageSeen && usageAvailable(usage),
		Usage:           usage,
	}, nil
}

type gatewayPreflightStreamPart struct {
	Content      string
	Reasoning    string
	FinishReason string
	ToolCalls    []gatewayPreflightStreamToolCall
	Usage        *api.OpenAIUsage
}

type gatewayPreflightStreamToolCall struct {
	Index int
	ID    string
	Name  string
}

func parseGatewayPreflightStreamChunk(raw string) (gatewayPreflightStreamPart, error) {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content          string `json:"content"`
				Reasoning        string `json:"reasoning"`
				ReasoningContent string `json:"reasoning_content"`
				ToolCalls        []struct {
					Index    int    `json:"index"`
					ID       string `json:"id"`
					Function struct {
						Name string `json:"name"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"delta"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage *api.OpenAIUsage `json:"usage"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
		return gatewayPreflightStreamPart{}, fmt.Errorf("decode gateway preflight stream chunk: %w", err)
	}
	if chunk.Error != nil && chunk.Error.Message != "" {
		return gatewayPreflightStreamPart{}, fmt.Errorf("gateway preflight stream error: %s", chunk.Error.Message)
	}
	var out gatewayPreflightStreamPart
	out.Usage = chunk.Usage
	for _, choice := range chunk.Choices {
		out.Content += choice.Delta.Content
		if choice.Delta.Reasoning != "" {
			out.Reasoning += choice.Delta.Reasoning
		} else {
			out.Reasoning += choice.Delta.ReasoningContent
		}
		if choice.FinishReason != "" {
			out.FinishReason = choice.FinishReason
		}
		for _, call := range choice.Delta.ToolCalls {
			out.ToolCalls = append(out.ToolCalls, gatewayPreflightStreamToolCall{
				Index: call.Index,
				ID:    call.ID,
				Name:  call.Function.Name,
			})
		}
	}
	return out, nil
}

func usageAvailable(usage api.OpenAIUsage) bool {
	return usage.TotalTokens != 0 || usage.PromptTokens != 0 || usage.CompletionTokens != 0
}

func maxIntKey(values map[int]string) int {
	max := -1
	for key := range values {
		if key > max {
			max = key
		}
	}
	return max
}

func looksLikeTextualToolCall(content string) bool {
	lower := strings.ToLower(content)
	return strings.Contains(lower, "<tool_call>") || strings.Contains(lower, "</tool_call>")
}

func gatewayPreflightHeaders() []string {
	return []string{
		"X-Myc-Job",
		"X-Myc-Project",
		"X-Myc-Node",
		"X-Myc-Instance",
		"X-Myc-Backend",
		"X-Myc-Decision",
		"X-Myc-Attempts",
		"X-Myc-Cold-Load",
		"X-Myc-Load-Wall-Ms",
		"X-Myc-Queue-Wait-Ms",
		"X-Myc-Upstream-Wall-Ms",
		"X-Myc-Structured-Output",
		"X-Myc-Guided-Decoding-Requested",
		"X-Myc-Tools-Requested",
		"X-Myc-Tool-Choice",
		"X-Myc-VLLM-Tool-Parser-Configured",
		"X-Myc-VLLM-Auto-Tool-Choice-Configured",
	}
}

func runBenchmarkFanout(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce benchmark run --url gateway --prompt prompt --model id [--model id] --out dir [--db path]")
	}
	fs := flag.NewFlagSet("benchmark run", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	url := fs.String("url", "", "Mycelium gateway URL")
	gatewayToken := fs.String("gateway-token", "", "Mycelium gateway bearer token")
	gatewayTokenEnv := fs.String("gateway-token-env", "", "environment variable containing the Mycelium gateway bearer token")
	prompt := fs.String("prompt", "", "benchmark prompt")
	maxTokens := fs.Int("max-tokens", 0, "maximum output tokens for each gateway request")
	out := fs.String("out", "", "output directory")
	id := fs.String("id", "", "parent benchmark job id")
	project := fs.String("project", "", "project id")
	var models repeatedString
	fs.Var(&models, "model", "model or preset id; may be repeated")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *url == "" {
		return fmt.Errorf("--url is required")
	}
	if *prompt == "" {
		return fmt.Errorf("--prompt is required")
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	if len(models) == 0 {
		return fmt.Errorf("--model is required")
	}
	authToken, authSource, err := bench.ResolveGatewayTokenSourceForURL(*url, *gatewayToken, *gatewayTokenEnv)
	if err != nil {
		return err
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	jobID := *id
	if jobID == "" {
		jobID = fmt.Sprintf("benchmark-%d", clock.System{}.Now().UnixNano())
	}
	parent := domain.Job{
		ID:       jobID,
		TaskType: "benchmark",
		Project:  *project,
		Priority: domain.PriorityBackground,
		Status:   domain.JobQueued,
		Benchmark: &domain.BenchmarkSpec{
			Prompt:    *prompt,
			Models:    append([]string(nil), models...),
			OutputDir: *out,
		},
	}
	runner := bench.Runner{
		Client: benchmarkGatewayClient{BaseURL: *url, AuthToken: authToken, MaxTokens: *maxTokens, Client: client},
		Clock:  clock.System{},
		Store:  store,
	}
	fmt.Printf("benchmark-auth\t%s\tconfigured=%t\n", authSource, authToken != "")
	fmt.Printf("benchmark\t%s\tstarted\n", parent.ID)
	results, err := runner.RunJob(ctx, parent)
	for _, result := range results {
		if result.Error != "" {
			fmt.Printf("benchmark-result\t%s\terror\t%s\n", result.Model, result.Error)
			continue
		}
		fmt.Printf("benchmark-result\t%s\t%s\n", result.Model, result.OutputPath)
	}
	if err != nil {
		return err
	}
	fmt.Printf("benchmark\t%s\tdone\t%s\n", parent.ID, *out)
	return nil
}

func runBenchmarkFleet(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("benchmark fleet", flag.ContinueOnError)
	configPath := fs.String("config", "", "fleet benchmark config JSON")
	out := fs.String("out", "", "output directory")
	profile := fs.String("profile", bench.FleetProfileConservative, "benchmark profile: conservative, saturation, or soak")
	simulate := fs.Bool("simulate", false, "run deterministic preflight only")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return fmt.Errorf("--config is required")
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	cfg, err := bench.LoadFleetConfig(*configPath)
	if err != nil {
		return err
	}
	result, err := bench.RunFleet(ctx, cfg, bench.FleetRunOptions{
		Profile:    *profile,
		Simulate:   *simulate,
		OutputRoot: *out,
		Client:     client,
		Clock:      clock.System{},
	})
	if err != nil {
		fmt.Printf("benchmark-fleet\t%s\tfailed\t%s\n", result.RunID, result.OutputDir)
		return err
	}
	fmt.Printf("benchmark-fleet\t%s\tdone\t%s\n", result.RunID, result.OutputDir)
	return nil
}

type repeatedString []string

func (r *repeatedString) String() string {
	return strings.Join(*r, ",")
}

func (r *repeatedString) Set(value string) error {
	if value == "" {
		return fmt.Errorf("model must not be empty")
	}
	*r = append(*r, value)
	return nil
}

type benchmarkGatewayClient struct {
	BaseURL   string
	AuthToken string
	MaxTokens int
	Client    *http.Client
}

func (c benchmarkGatewayClient) Complete(ctx context.Context, model, prompt string) (bench.Completion, error) {
	body, err := json.Marshal(api.OpenAIChatRequest{
		Model: model,
		Messages: []api.OpenAIMessage{{
			Role:    "user",
			Content: prompt,
		}},
		MaxTokens:     c.MaxTokens,
		Stream:        true,
		StreamOptions: &api.OpenAIStreamOptions{IncludeUsage: true},
	})
	if err != nil {
		return bench.Completion{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return bench.Completion{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return bench.Completion{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		data, err := readControlHTTPBody(resp.Body, "gateway benchmark response body")
		if err != nil {
			return bench.Completion{}, err
		}
		return bench.Completion{}, fmt.Errorf("gateway returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return readBenchmarkStream(resp.Body, start, time.Now)
}

func readBenchmarkStream(body io.Reader, start time.Time, now func() time.Time) (bench.Completion, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 4096), int(maxControlHTTPBodyBytes))
	var text strings.Builder
	firstTokenAt := time.Time{}
	end := start
	var usage api.OpenAIUsage
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if raw == "" {
			continue
		}
		end = now()
		if raw == "[DONE]" {
			break
		}
		part, gotUsage, err := parseBenchmarkStreamData(raw)
		if err != nil {
			return bench.Completion{}, err
		}
		if gotUsage.TotalTokens != 0 {
			usage = gotUsage
		}
		if part != "" {
			if firstTokenAt.IsZero() {
				firstTokenAt = end
			}
			text.WriteString(part)
		}
	}
	if err := scanner.Err(); err != nil {
		return bench.Completion{}, err
	}
	if text.Len() == 0 {
		return bench.Completion{}, fmt.Errorf("gateway stream produced no assistant content")
	}
	if firstTokenAt.IsZero() {
		firstTokenAt = end
	}
	completionTokens := usage.CompletionTokens
	if completionTokens == 0 {
		completionTokens = len(strings.Fields(text.String()))
	}
	seconds := end.Sub(firstTokenAt).Seconds()
	if seconds <= 0 {
		seconds = end.Sub(start).Seconds()
	}
	var tokensPerSec float64
	if completionTokens > 0 && seconds > 0 {
		tokensPerSec = float64(completionTokens) / seconds
	}
	return bench.Completion{
		Text:          text.String(),
		TokensPerSec:  tokensPerSec,
		TTFTms:        int(firstTokenAt.Sub(start) / time.Millisecond),
		ContextTokens: usage.TotalTokens,
	}, nil
}

func parseBenchmarkStreamData(raw string) (string, api.OpenAIUsage, error) {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
		Usage *api.OpenAIUsage `json:"usage"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
		return "", api.OpenAIUsage{}, fmt.Errorf("decode gateway stream chunk: %w", err)
	}
	if chunk.Error != nil && chunk.Error.Message != "" {
		return "", api.OpenAIUsage{}, fmt.Errorf("gateway stream error: %s", chunk.Error.Message)
	}
	var text strings.Builder
	for _, choice := range chunk.Choices {
		text.WriteString(choice.Delta.Content)
	}
	var usage api.OpenAIUsage
	if chunk.Usage != nil {
		usage = *chunk.Usage
	}
	return text.String(), usage, nil
}

func readControlHTTPBody(r io.Reader, label string) ([]byte, error) {
	var buf bytes.Buffer
	n, err := buf.ReadFrom(io.LimitReader(r, maxControlHTTPBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if n > maxControlHTTPBodyBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", label, maxControlHTTPBodyBytes)
	}
	return buf.Bytes(), nil
}

func installStageStatus(_ string) domain.JobStatus {
	return domain.JobRunning
}

func jobProgressSummary(job domain.Job) string {
	if job.Error != "" {
		return job.Error
	}
	if len(job.Progress) == 0 {
		return "-"
	}
	last := job.Progress[len(job.Progress)-1]
	if last.Message == "" {
		return last.Stage
	}
	return last.Stage + ":" + last.Message
}

func runRecommendations(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce recommendations <generate|list|apply|calibrate-speed>")
	}
	switch args[0] {
	case "generate":
		return runRecommendationsGenerate(ctx, args[1:])
	case "list":
		return runRecommendationsList(ctx, args[1:])
	case "apply":
		return runRecommendationsApply(ctx, args[1:])
	case "calibrate-speed":
		return runRecommendationsCalibrateSpeed(ctx, args[1:])
	default:
		return fmt.Errorf("unknown recommendations command %q", args[0])
	}
}

func runRecommendationsGenerate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recommendations generate", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	projectID := fs.String("project", "", "project id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *projectID == "" {
		return fmt.Errorf("--project is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	project, err := store.Project(ctx, *projectID)
	if err != nil {
		return err
	}
	service := optimizer.RecommendationService{Store: store, Clock: clock.System{}}
	records, err := service.EvaluateProject(ctx, project)
	if err != nil {
		return err
	}
	for _, rec := range records {
		fmt.Printf("%s\t%s\t%s\t%s\t%t\n", rec.ID, rec.ProjectID, rec.Type, recommendationTarget(rec), rec.Applied)
	}
	return nil
}

func runRecommendationsList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recommendations list", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	project := fs.String("project", "", "project id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	recs, err := store.ListRecommendations(ctx, *project)
	if err != nil {
		return err
	}
	for _, rec := range recs {
		fmt.Printf("%s\t%s\t%s\t%s\t%t\n", rec.ID, rec.ProjectID, rec.Type, recommendationTarget(rec), rec.Applied)
	}
	return nil
}

func runRecommendationsApply(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recommendations apply", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	id := fs.String("id", "", "recommendation id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	rec, err := store.Recommendation(ctx, *id)
	if err != nil {
		return err
	}
	if rec.Rejected {
		return fmt.Errorf("recommendation %q was rejected: %s", rec.ID, rec.RejectReason)
	}
	switch rec.Type {
	case optimizer.RecommendationContextCap:
		project, err := store.Project(ctx, rec.ProjectID)
		if err != nil {
			return err
		}
		if rec.PresetID == "" {
			return fmt.Errorf("recommendation %q has no preset to apply", rec.ID)
		}
		preset, err := store.Preset(ctx, rec.PresetID)
		if err != nil {
			return err
		}
		forced := project
		forced.AutoApply = true
		applied := optimizer.ApplyRecommendation(forced, preset, optimizer.Recommendation{
			Type:           rec.Type,
			ProjectID:      rec.ProjectID,
			CurrentCap:     rec.CurrentValue,
			RecommendedCap: rec.RecommendedValue,
			Rationale:      rec.Rationale,
		})
		if !applied.Applied {
			return fmt.Errorf("recommendation %q was not applied: %s", rec.ID, applied.Log.Result)
		}
		applied.Project.AutoApply = project.AutoApply
		if err := store.SaveProject(ctx, applied.Project); err != nil {
			return err
		}
		if err := store.SavePreset(ctx, applied.Preset); err != nil {
			return err
		}
	case optimizer.RecommendationEngineParameter:
		if rec.RecommendedPresetID == "" {
			return fmt.Errorf("engine recommendation %q has no preset to apply", rec.ID)
		}
		project, err := store.Project(ctx, rec.ProjectID)
		if err != nil {
			return err
		}
		project.DefaultModel = rec.RecommendedPresetID
		if err := store.SaveProject(ctx, project); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown recommendation type %q", rec.Type)
	}
	if err := store.MarkRecommendationApplied(ctx, *id, clock.System{}.Now().UTC()); err != nil {
		return err
	}
	fmt.Printf("recommendation\t%s\tapplied\n", *id)
	return nil
}

func recommendationTarget(rec domain.RecommendationRecord) string {
	if rec.RecommendedPresetID != "" {
		return rec.RecommendedPresetID
	}
	if rec.RecommendedValue != 0 {
		return fmt.Sprint(rec.RecommendedValue)
	}
	return "-"
}

func runRecommendationsCalibrateSpeed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recommendations calibrate-speed", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := storesqlite.Open(*dbPath)
	if err != nil {
		return err
	}
	defer store.Close()
	nodes, err := optimizer.CalibrateSpeedClasses(ctx, store, clock.System{})
	if err != nil {
		return err
	}
	for _, node := range nodes {
		fmt.Printf("%s\t%.2f\t%s\n", node.ID, node.SpeedClass.TokensPerSecRef, node.SpeedClass.Source)
	}
	return nil
}

func openCLIStore(args []string) (*storesqlite.Store, error) {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	return storesqlite.Open(*dbPath)
}

func defaultCatalogStore() string {
	return filepath.Join(defaultMyceliumHome(), "catalog")
}

func defaultControlStorePath() string {
	return filepath.Join(defaultMyceliumHome(), "mycelium.db")
}

func defaultMyceliumHome() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".mycelium"
	}
	return filepath.Join(home, ".mycelium")
}
