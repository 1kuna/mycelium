package controlcli

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"mycelium/internal/bench"
	"mycelium/internal/catalog"
	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/locality"
	"mycelium/internal/optimizer"
	"mycelium/internal/ports"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/pkg/api"
)

func Run(ctx context.Context, args []string) error {
	return RunWithClient(ctx, args, http.DefaultClient)
}

func RunWithClient(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce <add-model|models|nodes|projects|jobs|telemetry|recommendations|benchmark>")
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
		return runJobs(ctx, args[1:])
	case "telemetry":
		return runTelemetry(ctx, args[1:])
	case "recommendations":
		return runRecommendations(ctx, args[1:])
	case "benchmark":
		return runBenchmark(ctx, args[1:], client)
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
	job := domain.Job{
		ID:       catalog.InstallJobID(req),
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
		return fmt.Errorf("usage: myce models <list|stage|locality>")
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
	default:
		return fmt.Errorf("usage: myce models <list|stage|locality>")
	}
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
		data, readErr := io.ReadAll(resp.Body)
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
	data, err := io.ReadAll(resp.Body)
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
	data, err := io.ReadAll(resp.Body)
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
	url := fs.String("url", "http://127.0.0.1:51846", "peer URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	if err := fs.Parse(args); err != nil {
		return nodeAdminClient{}, err
	}
	return nodeAdminClient{BaseURL: *url, AuthToken: *rpcToken, Client: client}, nil
}

func nodeAdminWithTokenFromArgs(args []string, client *http.Client, requireToken bool) (nodeAdminClient, string, error) {
	fs := flag.NewFlagSet("nodes tokens", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:51846", "peer URL")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token")
	token := fs.String("token", "", "join token")
	if err := fs.Parse(args); err != nil {
		return nodeAdminClient{}, "", err
	}
	if requireToken && *token == "" {
		return nodeAdminClient{}, "", fmt.Errorf("--token is required")
	}
	return nodeAdminClient{BaseURL: *url, AuthToken: *rpcToken, Client: client}, *token, nil
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
	data, err := io.ReadAll(resp.Body)
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
	if err := store.SaveProject(ctx, project); err != nil {
		return err
	}
	fmt.Printf("project\t%s\n", project.ID)
	return nil
}

func runJobs(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return fmt.Errorf("usage: myce jobs list [--db path]")
	}
	store, err := openCLIStore(args[1:])
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

func runBenchmarkFanout(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce benchmark run --url gateway --prompt prompt --model id [--model id] --out dir [--db path]")
	}
	fs := flag.NewFlagSet("benchmark run", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	url := fs.String("url", "", "Mycelium gateway URL")
	prompt := fs.String("prompt", "", "benchmark prompt")
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
		Client: benchmarkGatewayClient{BaseURL: *url, Client: client},
		Clock:  clock.System{},
		Store:  store,
	}
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
	BaseURL string
	Client  *http.Client
}

func (c benchmarkGatewayClient) Complete(ctx context.Context, model, prompt string) (bench.Completion, error) {
	body, err := json.Marshal(api.OpenAIChatRequest{
		Model: model,
		Messages: []api.OpenAIMessage{{
			Role:    "user",
			Content: prompt,
		}},
	})
	if err != nil {
		return bench.Completion{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return bench.Completion{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return bench.Completion{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return bench.Completion{}, err
	}
	if resp.StatusCode >= 400 {
		return bench.Completion{}, fmt.Errorf("gateway returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var chat api.OpenAIChatResponse
	if err := json.Unmarshal(data, &chat); err != nil {
		return bench.Completion{}, err
	}
	if len(chat.Choices) == 0 {
		return bench.Completion{}, fmt.Errorf("gateway response had no choices")
	}
	return bench.Completion{
		Text:          chat.Choices[0].Message.Content,
		ContextTokens: chat.Usage.TotalTokens,
	}, nil
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
