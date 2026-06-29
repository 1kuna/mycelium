package controlcli

import (
	"bufio"
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
	projectvalidation "mycelium/internal/project"
	storesqlite "mycelium/internal/store/sqlite"
	"mycelium/pkg/api"
)

const maxControlHTTPBodyBytes int64 = 16 << 20

func Run(ctx context.Context, args []string) error {
	return RunWithClient(ctx, args, http.DefaultClient)
}

func RunWithClient(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce <add-model|models|nodes|projects|jobs|telemetry|recommendations|benchmark|gateway|doctor|debug|status>")
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
	case "gateway":
		return runGateway(ctx, args[1:], client)
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

func runGateway(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce gateway <preflight>")
	}
	switch args[0] {
	case "preflight":
		return runGatewayPreflight(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce gateway <preflight>")
	}
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
	toolName := fs.String("tool-name", "", "optional echo-style function name for an OpenAI tool-call canary")
	toolChoice := fs.String("tool-choice", "", "tool choice for --tool-name: auto, required, none, or function")
	outDir := fs.String("out", "", "optional directory for a redacted preflight evidence packet")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *url == "" {
		return fmt.Errorf("--url is required")
	}
	if *model == "" {
		return fmt.Errorf("--model is required")
	}
	token, tokenSource, err := resolveGatewayPreflightToken(*url, *gatewayToken, *gatewayTokenEnv, *gatewayTokenFile, *configPath)
	if err != nil {
		return err
	}
	result, err := gatewayPreflightComplete(ctx, gatewayPreflightConfig{
		URL:        *url,
		Model:      *model,
		Prompt:     *prompt,
		MaxTokens:  *maxTokens,
		Project:    *project,
		ToolName:   *toolName,
		ToolChoice: *toolChoice,
		Token:      token,
		Client:     client,
	})
	if err != nil {
		return err
	}
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
	if result.Usage.TotalTokens != 0 || result.Usage.PromptTokens != 0 || result.Usage.CompletionTokens != 0 {
		fmt.Printf("usage\tprompt=%d\tcompletion=%d\ttotal=%d\n", result.Usage.PromptTokens, result.Usage.CompletionTokens, result.Usage.TotalTokens)
	}
	if *outDir != "" {
		path, err := writeGatewayPreflightEvidence(*outDir, tokenSource, gatewayPreflightConfig{
			URL:        *url,
			Model:      *model,
			Project:    *project,
			MaxTokens:  *maxTokens,
			ToolName:   *toolName,
			ToolChoice: *toolChoice,
		}, result)
		if err != nil {
			return err
		}
		fmt.Printf("evidence\t%s\n", path)
	}
	return nil
}

type gatewayPreflightConfig struct {
	URL        string
	Model      string
	Prompt     string
	MaxTokens  int
	Project    string
	ToolName   string
	ToolChoice string
	Token      string
	Client     *http.Client
}

type gatewayPreflightResult struct {
	StatusCode      int
	Headers         http.Header
	FinishReason    string
	Content         string
	Reasoning       string
	ToolCallCount   int
	ToolCallNames   []string
	TextualToolCall bool
	JSONContent     bool
	Usage           api.OpenAIUsage
}

type gatewayPreflightEvidence struct {
	Gateway         string            `json:"gateway"`
	Model           string            `json:"model"`
	Project         string            `json:"project,omitempty"`
	AuthSource      string            `json:"auth_source"`
	StatusCode      int               `json:"status_code"`
	Headers         map[string]string `json:"headers"`
	FinishReason    string            `json:"finish_reason"`
	ContentLength   int               `json:"content_length"`
	ReasoningLength int               `json:"reasoning_length"`
	ToolCallCount   int               `json:"tool_call_count"`
	ToolCallNames   []string          `json:"tool_call_names,omitempty"`
	TextualToolCall bool              `json:"textual_tool_call"`
	JSONContent     bool              `json:"json_content"`
	Usage           api.OpenAIUsage   `json:"usage"`
}

func writeGatewayPreflightEvidence(outDir, authSource string, cfg gatewayPreflightConfig, result gatewayPreflightResult) (string, error) {
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return "", fmt.Errorf("create gateway preflight evidence dir: %w", err)
	}
	evidence := gatewayPreflightEvidence{
		Gateway:         strings.TrimRight(cfg.URL, "/"),
		Model:           cfg.Model,
		Project:         cfg.Project,
		AuthSource:      authSource,
		StatusCode:      result.StatusCode,
		Headers:         gatewayPreflightHeaderMap(result.Headers),
		FinishReason:    result.FinishReason,
		ContentLength:   len(result.Content),
		ReasoningLength: len(result.Reasoning),
		ToolCallCount:   result.ToolCallCount,
		ToolCallNames:   result.ToolCallNames,
		TextualToolCall: result.TextualToolCall,
		JSONContent:     result.JSONContent,
		Usage:           result.Usage,
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
	for _, name := range bench.DefaultGatewayTokenEnvNames() {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value, name, nil
		}
	}
	token, err := bench.ResolveGatewayTokenForURL(rawURL, "", "")
	if err != nil {
		return "", "", fmt.Errorf("%w; checked env %s, --gateway-token-file, --config gateway_token, and ~/.mycelium/peer.json gateway_token", err, strings.Join(bench.DefaultGatewayTokenEnvNames(), ", "))
	}
	if token != "" {
		return token, "~/.mycelium/peer.json gateway_token", nil
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
		return gatewayPreflightResult{}, fmt.Errorf("gateway preflight returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	parsed, err := parseGatewayPreflightResponse(data)
	if err != nil {
		return gatewayPreflightResult{}, err
	}
	parsed.StatusCode = resp.StatusCode
	parsed.Headers = resp.Header.Clone()
	return parsed, nil
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
		Usage:           out.Usage,
	}, nil
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
	authToken, err := bench.ResolveGatewayTokenForURL(*url, *gatewayToken, *gatewayTokenEnv)
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
