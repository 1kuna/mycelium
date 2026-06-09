package controlcli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	storesqlite "mycelium/internal/store/sqlite"
)

const operatorRecentLimit = 200

type operatorPeerConfig struct {
	ID                   string                 `json:"id"`
	Listen               string                 `json:"listen"`
	StorePath            string                 `json:"store_path"`
	CatalogDir           string                 `json:"catalog_dir"`
	Compute              bool                   `json:"compute"`
	ComputeConfig        operatorComputeConfig  `json:"compute_config"`
	JoinToken            string                 `json:"join_token"`
	RPCToken             string                 `json:"rpc_token"`
	GatewayToken         string                 `json:"gateway_token"`
	GatewayProjectTokens []operatorProjectToken `json:"gateway_project_tokens"`
	SeedPeers            []string               `json:"seed_peers"`
	Presets              []domain.Preset        `json:"presets"`
	Projects             []domain.Project       `json:"projects"`
}

type operatorComputeConfig struct {
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	Backend          domain.Backend `json:"backend"`
	BackendBinary    string         `json:"backend_binary"`
	LlamaServer      string         `json:"llama_server"`
	DiskPath         string         `json:"disk_path"`
	DiskTotalMB      int            `json:"disk_total_mb"`
	DiskFreeMB       int            `json:"disk_free_mb"`
	DiskMinFreeRatio float64        `json:"disk_min_free_ratio"`
}

type operatorProjectToken struct {
	Project string `json:"project"`
	Token   string `json:"token"`
}

type loadedOperatorConfig struct {
	Path   string
	Config operatorPeerConfig
}

func runDoctor(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	configPath := fs.String("config", defaultPeerConfigPath(), "peer config JSON")
	dbPath := fs.String("db", "", "control-plane SQLite store")
	url := fs.String("url", "", "peer URL override")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token override")
	if err := fs.Parse(args); err != nil {
		return err
	}
	report, err := buildDoctorReport(ctx, doctorRequest{
		ConfigPath: *configPath,
		DBPath:     *dbPath,
		URL:        *url,
		RPCToken:   *rpcToken,
		Client:     client,
	})
	for _, check := range report.Checks {
		next := check.Next
		if next == "" {
			next = "-"
		}
		fmt.Printf("check\t%s\t%s\t%s\t%s\n", check.Name, check.Status, check.Message, next)
	}
	if err != nil {
		return err
	}
	return report.Error()
}

func runStatus(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	configPath := fs.String("config", defaultPeerConfigPath(), "peer config JSON")
	dbPath := fs.String("db", "", "control-plane SQLite store")
	url := fs.String("url", "", "peer URL override")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token override")
	watch := fs.Bool("watch", false, "refresh until interrupted")
	interval := fs.Duration("interval", 2*time.Second, "watch refresh interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	print := func() error {
		report, err := buildStatusReport(ctx, statusRequest{
			ConfigPath: *configPath,
			DBPath:     *dbPath,
			URL:        *url,
			RPCToken:   *rpcToken,
			Client:     client,
		})
		fmt.Print(report)
		return err
	}
	if !*watch {
		return print()
	}
	if *interval <= 0 {
		return fmt.Errorf("--interval must be positive")
	}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		if err := print(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			fmt.Println()
		}
	}
}

func runDebug(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce debug <job|bundle>")
	}
	switch args[0] {
	case "job":
		return runDebugJob(ctx, args[1:])
	case "bundle":
		return runDebugBundle(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce debug <job|bundle>")
	}
}

func runDebugJob(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("debug job", flag.ContinueOnError)
	dbPath := fs.String("db", defaultControlStorePath(), "control-plane SQLite store")
	jsonOut := fs.Bool("json", false, "print JSON")
	jobID, flagArgs, err := splitDebugJobArgs(args)
	if err != nil {
		return err
	}
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if jobID == "" {
		return fmt.Errorf("usage: myce debug job <job-id> [--db path] [--json]")
	}
	report, err := buildJobDebugReport(ctx, *dbPath, jobID)
	if err != nil {
		return err
	}
	if *jsonOut {
		return writePrettyJSON(os.Stdout, report)
	}
	printJobDebugReport(os.Stdout, report)
	return nil
}

func splitDebugJobArgs(args []string) (string, []string, error) {
	var jobID string
	flagArgs := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			flagArgs = append(flagArgs, arg)
			if debugJobFlagTakesValue(arg) {
				i++
				if i >= len(args) {
					return "", nil, fmt.Errorf("%s requires a value", arg)
				}
				flagArgs = append(flagArgs, args[i])
			}
			continue
		}
		if jobID != "" {
			return "", nil, fmt.Errorf("usage: myce debug job <job-id> [--db path] [--json]")
		}
		jobID = arg
	}
	return jobID, flagArgs, nil
}

func debugJobFlagTakesValue(arg string) bool {
	if strings.Contains(arg, "=") {
		return false
	}
	return arg == "-db" || arg == "--db"
}

func runDebugBundle(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("debug bundle", flag.ContinueOnError)
	configPath := fs.String("config", defaultPeerConfigPath(), "peer config JSON")
	dbPath := fs.String("db", "", "control-plane SQLite store")
	jobID := fs.String("job", "", "job id to focus")
	out := fs.String("out", "", "output directory")
	url := fs.String("url", "", "peer URL override")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token override")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	if err := writeDebugBundle(ctx, debugBundleRequest{
		ConfigPath: *configPath,
		DBPath:     *dbPath,
		JobID:      *jobID,
		OutDir:     *out,
		URL:        *url,
		RPCToken:   *rpcToken,
		Client:     client,
	}); err != nil {
		return err
	}
	fmt.Printf("debug-bundle\t%s\n", *out)
	return nil
}

type doctorRequest struct {
	ConfigPath string
	DBPath     string
	URL        string
	RPCToken   string
	Client     *http.Client
}

type doctorReport struct {
	Checks []doctorCheck `json:"checks"`
}

type doctorCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message"`
	Next    string `json:"next,omitempty"`
}

func (r doctorReport) Error() error {
	for _, check := range r.Checks {
		if check.Status == "fail" {
			return fmt.Errorf("doctor found failed checks")
		}
	}
	return nil
}

func buildDoctorReport(ctx context.Context, req doctorRequest) (doctorReport, error) {
	var report doctorReport
	add := func(name, status, message, next string) {
		report.Checks = append(report.Checks, doctorCheck{Name: name, Status: status, Message: message, Next: next})
	}
	loaded, cfgErr := loadOperatorConfig(req.ConfigPath)
	if cfgErr != nil {
		add("config", "fail", cfgErr.Error(), "check "+req.ConfigPath)
	} else {
		add("config", "ok", loaded.Path, "-")
	}
	cfg := loaded.Config
	baseURL := firstNonEmpty(req.URL, adminBaseURL(cfg.Listen))
	rpcToken := firstNonEmpty(req.RPCToken, cfg.RPCToken)
	if cfgErr == nil {
		if cfg.Listen == "" {
			add("listen", "warn", "listen address is empty", "set listen in "+loaded.Path)
		} else {
			add("listen", "ok", cfg.Listen, "-")
		}
		if !isLoopbackListen(cfg.Listen) {
			if cfg.RPCToken == "" {
				add("rpc-token", "fail", "non-loopback listener has no rpc_token", "set rpc_token in "+loaded.Path)
			} else {
				add("rpc-token", "ok", "configured", "-")
			}
			if cfg.GatewayToken == "" && len(cfg.GatewayProjectTokens) == 0 {
				add("gateway-token", "fail", "non-loopback listener has no gateway token", "set gateway_token in "+loaded.Path)
			} else {
				add("gateway-token", "ok", "configured", "-")
			}
		} else if cfg.GatewayToken == "" && len(cfg.GatewayProjectTokens) == 0 {
			add("gateway-token", "warn", "not configured for loopback listener", "set gateway_token before exposing this peer")
		} else {
			add("gateway-token", "ok", "configured", "-")
		}
	}
	storePath := firstNonEmpty(req.DBPath, cfg.StorePath, defaultControlStorePath())
	store, storeErr := storesqlite.Open(storePath)
	if storeErr != nil {
		add("store", "fail", storeErr.Error(), "check --db "+storePath)
	} else {
		defer store.Close()
		add("store", "ok", storePath, "-")
		addStoreDoctorChecks(ctx, store, &report)
	}
	if baseURL == "" {
		add("peer-snapshot", "warn", "no peer URL available", "pass --url or set listen in peer config")
	} else {
		if snap, err := fetchNodeSnapshot(ctx, clientOrDefault(req.Client), baseURL, rpcToken); err != nil {
			add("peer-snapshot", "fail", err.Error(), "myce doctor --url "+baseURL+" --rpc-token <token>")
		} else {
			add("peer-snapshot", "ok", fmt.Sprintf("%s instances=%d", snap.Node.ID, len(snap.Instances)), "-")
		}
	}
	return report, nil
}

func addStoreDoctorChecks(ctx context.Context, store *storesqlite.Store, report *doctorReport) {
	add := func(name, status, message, next string) {
		report.Checks = append(report.Checks, doctorCheck{Name: name, Status: status, Message: message, Next: next})
	}
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		add("nodes", "fail", err.Error(), "inspect store")
	} else {
		missingAddress := 0
		for _, node := range nodes {
			if node.Address == "" {
				missingAddress++
			}
		}
		status := "ok"
		message := fmt.Sprintf("%d known", len(nodes))
		if missingAddress > 0 {
			status = "warn"
			message = fmt.Sprintf("%d known, %d missing address", len(nodes), missingAddress)
		}
		add("nodes", status, message, "myce nodes list")
	}
	presets, err := store.ListPresets(ctx)
	if err != nil {
		add("presets", "fail", err.Error(), "myce models list")
	} else {
		bad := 0
		for _, preset := range presets {
			if preset.Backend == "" || preset.ContextLength <= 0 || preset.ModelRef == "" {
				bad++
			}
		}
		if bad > 0 {
			add("presets", "fail", fmt.Sprintf("%d configured, %d missing placement metadata", len(presets), bad), "fix preset backend/model_ref/context_length")
		} else {
			add("presets", "ok", fmt.Sprintf("%d configured", len(presets)), "myce models list")
		}
	}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		add("jobs", "fail", err.Error(), "myce jobs list")
	} else {
		counts := jobStatusCounts(jobs)
		status := "ok"
		if counts[domain.JobQueued]+counts[domain.JobPlacing]+counts[domain.JobLoading]+counts[domain.JobRunning] > 0 {
			status = "warn"
		}
		add("jobs", status, formatJobCounts(counts), "myce jobs list")
	}
	records, err := store.Snapshot(ctx)
	if err != nil {
		add("registry", "fail", err.Error(), "inspect job registry")
	} else {
		cleanup := 0
		for _, rec := range records {
			if rec.CleanupRequired {
				cleanup++
			}
		}
		if cleanup > 0 {
			add("registry", "warn", fmt.Sprintf("%d records, %d need cleanup", len(records), cleanup), "myce debug bundle --out /tmp/mycelium-debug")
		} else {
			add("registry", "ok", fmt.Sprintf("%d records", len(records)), "-")
		}
	}
	if _, err := store.Metrics(ctx, ""); err != nil {
		add("run-metrics", "fail", err.Error(), "inspect telemetry schema")
	} else {
		add("run-metrics", "ok", "readable", "myce telemetry samples --limit 20")
	}
	if _, err := store.Samples(ctx, domain.SessionMetricQuery{Limit: 1}); err != nil {
		add("session-metrics", "fail", err.Error(), "inspect telemetry schema")
	} else {
		add("session-metrics", "ok", "readable", "myce telemetry samples --limit 20")
	}
}

type statusRequest struct {
	ConfigPath string
	DBPath     string
	URL        string
	RPCToken   string
	Client     *http.Client
}

func buildStatusReport(ctx context.Context, req statusRequest) (string, error) {
	var out strings.Builder
	loaded, cfgErr := loadOperatorConfig(req.ConfigPath)
	cfg := loaded.Config
	configPath := req.ConfigPath
	if configPath == "" {
		configPath = defaultPeerConfigPath()
	}
	if cfgErr != nil {
		fmt.Fprintf(&out, "config\t%s\tfail\t%s\n", configPath, cfgErr)
	} else {
		fmt.Fprintf(&out, "config\t%s\tok\n", loaded.Path)
		fmt.Fprintf(&out, "peer\t%s\tlisten=%s\tcompute=%t\tgateway_auth=%t\n", emptyDash(cfg.ID), emptyDash(cfg.Listen), cfg.Compute, cfg.GatewayToken != "" || len(cfg.GatewayProjectTokens) > 0)
	}
	storePath := firstNonEmpty(req.DBPath, cfg.StorePath, defaultControlStorePath())
	store, err := storesqlite.Open(storePath)
	if err != nil {
		fmt.Fprintf(&out, "store\t%s\tfail\t%s\n", storePath, err)
		return out.String(), err
	}
	defer store.Close()
	fmt.Fprintf(&out, "store\t%s\tok\n", storePath)
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		return out.String(), err
	}
	fmt.Fprintf(&out, "queue\t%s\n", formatJobCounts(jobStatusCounts(jobs)))
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		return out.String(), err
	}
	instances, err := store.ListInstances(ctx)
	if err != nil {
		return out.String(), err
	}
	fmt.Fprintf(&out, "nodes\t%d\n", len(nodes))
	for _, node := range nodes {
		fmt.Fprintf(&out, "node\t%s\t%s\t%s\taccelerators=%d\theartbeat=%s\n", node.ID, node.Status, emptyDash(node.Address), len(node.Accelerators), formatTimeDash(node.HeartbeatAt))
	}
	fmt.Fprintf(&out, "instances\t%d\n", len(instances))
	for _, inst := range instances {
		fmt.Fprintf(&out, "instance\t%s\t%s\t%s\t%s\tinflight=%d\tloading=%t\n", inst.ID, inst.NodeID, inst.PresetID, inst.State, inst.InFlight, inst.Loading)
	}
	baseURL := firstNonEmpty(req.URL, adminBaseURL(cfg.Listen))
	if baseURL != "" {
		snap, err := fetchNodeSnapshot(ctx, clientOrDefault(req.Client), baseURL, firstNonEmpty(req.RPCToken, cfg.RPCToken))
		if err != nil {
			fmt.Fprintf(&out, "snapshot\tfail\t%s\n", err)
		} else {
			fmt.Fprintf(&out, "snapshot\tok\t%s\tinstances=%d\n", snap.Node.ID, len(snap.Instances))
		}
	}
	return out.String(), nil
}

type jobDebugReport struct {
	Job             domain.Job                    `json:"job"`
	Parent          *domain.Job                   `json:"parent,omitempty"`
	Children        []domain.Job                  `json:"children,omitempty"`
	Registry        *domain.JobRecord             `json:"registry,omitempty"`
	RunMetrics      []domain.RunMetric            `json:"run_metrics,omitempty"`
	SessionTimeline []domain.SessionMetric        `json:"session_timeline,omitempty"`
	Recommendations []domain.RecommendationRecord `json:"recommendations,omitempty"`
	Hints           []string                      `json:"hints,omitempty"`
}

func buildJobDebugReport(ctx context.Context, dbPath, jobID string) (jobDebugReport, error) {
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		return jobDebugReport{}, err
	}
	defer store.Close()
	job, err := store.Job(ctx, jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return jobDebugReport{}, fmt.Errorf("job %q not found", jobID)
		}
		return jobDebugReport{}, err
	}
	report := jobDebugReport{Job: job}
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		return jobDebugReport{}, err
	}
	for _, candidate := range jobs {
		if candidate.ID == job.ParentID {
			parent := candidate
			report.Parent = &parent
		}
		if candidate.ParentID == job.ID {
			report.Children = append(report.Children, candidate)
		}
	}
	records, err := store.Snapshot(ctx)
	if err != nil {
		return jobDebugReport{}, err
	}
	for _, rec := range records {
		if rec.JobID == job.ID {
			recCopy := rec
			report.Registry = &recCopy
			break
		}
	}
	metrics, err := store.Metrics(ctx, "")
	if err != nil {
		return jobDebugReport{}, err
	}
	for _, metric := range metrics {
		if metric.JobID == job.ID {
			report.RunMetrics = append(report.RunMetrics, metric)
		}
	}
	samples, err := store.Samples(ctx, domain.SessionMetricQuery{})
	if err != nil {
		return jobDebugReport{}, err
	}
	for _, sample := range samples {
		if sample.JobID == job.ID {
			report.SessionTimeline = append(report.SessionTimeline, sample)
		}
	}
	recs, err := store.ListRecommendations(ctx, job.Project)
	if err != nil {
		return jobDebugReport{}, err
	}
	for _, rec := range recs {
		if job.Project == "" || rec.ProjectID == job.Project {
			report.Recommendations = append(report.Recommendations, rec)
		}
	}
	report.Hints = jobDebugHints(report)
	return report, nil
}

func printJobDebugReport(w io.Writer, report jobDebugReport) {
	owner := "-"
	if report.Registry != nil && report.Registry.AssignedNode != "" {
		owner = report.Registry.AssignedNode
	} else if len(report.RunMetrics) > 0 && report.RunMetrics[0].NodeID != "" {
		owner = report.RunMetrics[0].NodeID
	}
	fmt.Fprintf(w, "job\t%s\t%s\tmodel=%s\tproject=%s\towner=%s\n", report.Job.ID, report.Job.Status, emptyDash(report.Job.Model), emptyDash(report.Job.Project), owner)
	if report.Parent != nil {
		fmt.Fprintf(w, "parent\t%s\t%s\n", report.Parent.ID, report.Parent.Status)
	}
	for _, child := range report.Children {
		fmt.Fprintf(w, "child\t%s\t%s\t%s\n", child.ID, child.TaskType, child.Status)
	}
	if report.Registry != nil {
		fmt.Fprintf(w, "registry\t%s\tcoordinator=%s\tassigned=%s\tcleanup=%t\tfence=%d\t%s\n", report.Registry.Status, emptyDash(report.Registry.Coordinator), emptyDash(report.Registry.AssignedNode), report.Registry.CleanupRequired, report.Registry.Fence, emptyDash(report.Registry.RecoveryNote))
	}
	events := jobTimelineEvents(report)
	for _, event := range events {
		fmt.Fprintf(w, "timeline\t%s\t%s\t%s\t%s\n", formatTimeDash(event.At), event.Kind, event.Subject, event.Detail)
	}
	for _, hint := range report.Hints {
		fmt.Fprintf(w, "hint\t%s\n", hint)
	}
}

type jobTimelineEvent struct {
	At      time.Time
	Kind    string
	Subject string
	Detail  string
}

func jobTimelineEvents(report jobDebugReport) []jobTimelineEvent {
	var events []jobTimelineEvent
	for _, progress := range report.Job.Progress {
		detail := progress.Message
		if detail == "" {
			detail = "-"
		}
		events = append(events, jobTimelineEvent{At: progress.At, Kind: "progress", Subject: progress.Stage, Detail: detail})
	}
	if report.Registry != nil {
		events = append(events, jobTimelineEvent{At: report.Registry.UpdatedAt, Kind: "registry", Subject: string(report.Registry.Status), Detail: emptyDash(report.Registry.RecoveryNote)})
	}
	for _, metric := range report.RunMetrics {
		detail := fmt.Sprintf("node=%s instance=%s preset=%s tps=%.2f ttft_ms=%d context=%d", emptyDash(metric.NodeID), emptyDash(metric.InstanceID), emptyDash(metric.PresetID), metric.TokensPerSec, metric.TTFTms, metric.ContextUsed)
		events = append(events, jobTimelineEvent{At: metric.At, Kind: "run-metric", Subject: emptyDash(metric.NodeID), Detail: detail})
	}
	for _, sample := range report.SessionTimeline {
		detail := fmt.Sprintf("session=%s seq=%d node=%s instance=%s elapsed_ms=%d error=%s", sample.SessionID, sample.Sequence, emptyDash(sample.NodeID), emptyDash(sample.InstanceID), sample.ElapsedMS, emptyDash(sample.Error))
		events = append(events, jobTimelineEvent{At: sample.At, Kind: "sample", Subject: string(sample.Phase), Detail: detail})
	}
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].At.IsZero() {
			return false
		}
		if events[j].At.IsZero() {
			return true
		}
		return events[i].At.Before(events[j].At)
	})
	return events
}

func jobDebugHints(report jobDebugReport) []string {
	var text []string
	if report.Job.Error != "" {
		text = append(text, report.Job.Error)
	}
	if report.Registry != nil {
		text = append(text, report.Registry.CleanupError, report.Registry.RecoveryNote)
	}
	for _, sample := range report.SessionTimeline {
		text = append(text, sample.Error)
	}
	joined := strings.ToLower(strings.Join(text, "\n"))
	var hints []string
	for _, rule := range []struct {
		needle string
		hint   string
	}{
		{"no fit", "placement failed because no candidate fit the request"},
		{"context overflow", "request exceeded the selected model context and should be requeued or retuned"},
		{"stale fence", "owner rejected a stale admission fence; coordinator should re-plan"},
		{"unreachable", "owner or peer was unreachable; inspect registry recovery evidence"},
		{"health timeout", "backend did not become healthy before the load timeout"},
		{"unauthorized", "auth failed; check rpc_token or gateway_token configuration"},
		{"rpc token", "peer RPC auth failed; check rpc_token configuration"},
	} {
		if strings.Contains(joined, rule.needle) {
			hints = append(hints, rule.hint)
		}
	}
	return uniqueStrings(hints)
}

type debugBundleRequest struct {
	ConfigPath string
	DBPath     string
	JobID      string
	OutDir     string
	URL        string
	RPCToken   string
	Client     *http.Client
}

type debugBundleManifest struct {
	GeneratedAt time.Time `json:"generated_at"`
	ConfigPath  string    `json:"config_path"`
	StorePath   string    `json:"store_path"`
	JobID       string    `json:"job_id,omitempty"`
	Files       []string  `json:"files"`
	Issues      []string  `json:"issues,omitempty"`
}

func writeDebugBundle(ctx context.Context, req debugBundleRequest) error {
	if err := os.MkdirAll(req.OutDir, 0700); err != nil {
		return err
	}
	if err := os.Chmod(req.OutDir, 0700); err != nil {
		return err
	}
	manifest := debugBundleManifest{GeneratedAt: clock.System{}.Now().UTC(), ConfigPath: req.ConfigPath, JobID: req.JobID}
	addFile := func(name string) string {
		manifest.Files = append(manifest.Files, name)
		return filepath.Join(req.OutDir, name)
	}
	loaded, cfgErr := loadOperatorConfig(req.ConfigPath)
	cfg := loaded.Config
	if req.ConfigPath == "" {
		req.ConfigPath = defaultPeerConfigPath()
	}
	manifest.ConfigPath = req.ConfigPath
	if cfgErr != nil {
		manifest.Issues = append(manifest.Issues, cfgErr.Error())
	} else {
		if err := writeRedactedConfigFile(req.ConfigPath, addFile("config.redacted.json")); err != nil {
			return err
		}
	}
	storePath := firstNonEmpty(req.DBPath, cfg.StorePath, defaultControlStorePath())
	manifest.StorePath = storePath
	store, err := storesqlite.Open(storePath)
	if err != nil {
		manifest.Issues = append(manifest.Issues, err.Error())
	} else {
		defer store.Close()
		if err := writeStoreBundleFiles(ctx, store, req.JobID, addFile); err != nil {
			return err
		}
	}
	baseURL := firstNonEmpty(req.URL, adminBaseURL(cfg.Listen))
	if baseURL != "" {
		if snap, err := fetchNodeSnapshot(ctx, clientOrDefault(req.Client), baseURL, firstNonEmpty(req.RPCToken, cfg.RPCToken)); err != nil {
			manifest.Issues = append(manifest.Issues, err.Error())
		} else if err := writePrettyJSONFile(addFile("peer_snapshot.json"), snap); err != nil {
			return err
		}
	}
	status, statusErr := buildStatusReport(ctx, statusRequest{ConfigPath: req.ConfigPath, DBPath: req.DBPath, URL: req.URL, RPCToken: req.RPCToken, Client: req.Client})
	if statusErr != nil {
		manifest.Issues = append(manifest.Issues, statusErr.Error())
	}
	if err := os.WriteFile(addFile("status.txt"), []byte(status), 0600); err != nil {
		return err
	}
	return writePrettyJSONFile(addFile("manifest.json"), manifest)
}

func writeStoreBundleFiles(ctx context.Context, store *storesqlite.Store, jobID string, addFile func(string) string) error {
	jobs, err := store.ListJobs(ctx)
	if err != nil {
		return err
	}
	if jobID != "" {
		jobs = filterJobs(jobs, func(job domain.Job) bool { return job.ID == jobID || job.ParentID == jobID })
	}
	if err := writePrettyJSONFile(addFile("jobs.json"), jobs); err != nil {
		return err
	}
	records, err := store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if jobID != "" {
		records = filterRecords(records, func(rec domain.JobRecord) bool { return rec.JobID == jobID })
	}
	if err := writePrettyJSONFile(addFile("registry.json"), records); err != nil {
		return err
	}
	metrics, err := store.Metrics(ctx, "")
	if err != nil {
		return err
	}
	if jobID != "" {
		metrics = filterRunMetrics(metrics, func(metric domain.RunMetric) bool { return metric.JobID == jobID })
	}
	if err := writePrettyJSONFile(addFile("run_metrics.json"), limitTail(metrics, operatorRecentLimit)); err != nil {
		return err
	}
	samples, err := store.Samples(ctx, domain.SessionMetricQuery{})
	if err != nil {
		return err
	}
	if jobID != "" {
		samples = filterSessionMetrics(samples, func(sample domain.SessionMetric) bool { return sample.JobID == jobID })
	}
	if err := writePrettyJSONFile(addFile("session_metrics.json"), limitTail(samples, operatorRecentLimit)); err != nil {
		return err
	}
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		return err
	}
	if err := writePrettyJSONFile(addFile("nodes.json"), nodes); err != nil {
		return err
	}
	instances, err := store.ListInstances(ctx)
	if err != nil {
		return err
	}
	if err := writePrettyJSONFile(addFile("instances.json"), instances); err != nil {
		return err
	}
	recs, err := store.ListRecommendations(ctx, "")
	if err != nil {
		return err
	}
	if jobID != "" {
		project := ""
		for _, job := range jobs {
			if job.ID == jobID {
				project = job.Project
				break
			}
		}
		if project != "" {
			recs = filterRecommendations(recs, func(rec domain.RecommendationRecord) bool { return rec.ProjectID == project })
		}
	}
	return writePrettyJSONFile(addFile("recommendations.json"), recs)
}

func fetchNodeSnapshot(ctx context.Context, client *http.Client, baseURL, rpcToken string) (domain.NodeSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(catalogPeerBaseURL(baseURL), "/")+"/snapshot", nil)
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
	data, err := readControlHTTPBody(resp.Body, "snapshot response body")
	if err != nil {
		return domain.NodeSnapshot{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return domain.NodeSnapshot{}, fmt.Errorf("snapshot %s: %s", baseURL, strings.TrimSpace(string(data)))
	}
	var snap domain.NodeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return domain.NodeSnapshot{}, err
	}
	if snap.Node.ID == "" {
		return domain.NodeSnapshot{}, fmt.Errorf("snapshot %s returned empty node id", baseURL)
	}
	return snap, nil
}

func loadOperatorConfig(path string) (loadedOperatorConfig, error) {
	if path == "" {
		path = defaultPeerConfigPath()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return loadedOperatorConfig{Path: path}, fmt.Errorf("read peer config %s: %w", path, err)
	}
	var cfg operatorPeerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return loadedOperatorConfig{Path: path}, fmt.Errorf("parse peer config %s: %w", path, err)
	}
	if cfg.StorePath == "" {
		cfg.StorePath = defaultControlStorePath()
	}
	if cfg.CatalogDir == "" {
		cfg.CatalogDir = defaultCatalogStore()
	}
	return loadedOperatorConfig{Path: path, Config: cfg}, nil
}

func defaultPeerConfigPath() string {
	return filepath.Join(defaultMyceliumHome(), "peer.json")
}

func isLoopbackListen(listen string) bool {
	host := listenHost(listen)
	if host == "" {
		return true
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func listenHost(listen string) string {
	if listen == "" {
		return ""
	}
	if strings.HasPrefix(listen, "http://") || strings.HasPrefix(listen, "https://") {
		u, err := neturl.Parse(listen)
		if err == nil {
			return u.Hostname()
		}
	}
	host, _, err := net.SplitHostPort(listen)
	if err == nil {
		return strings.Trim(host, "[]")
	}
	if strings.Contains(listen, ":") {
		return ""
	}
	return listen
}

func clientOrDefault(client *http.Client) *http.Client {
	if client != nil {
		return client
	}
	return http.DefaultClient
}

func jobStatusCounts(jobs []domain.Job) map[domain.JobStatus]int {
	counts := map[domain.JobStatus]int{}
	for _, job := range jobs {
		counts[job.Status]++
	}
	return counts
}

func formatJobCounts(counts map[domain.JobStatus]int) string {
	order := []domain.JobStatus{domain.JobQueued, domain.JobPlacing, domain.JobLoading, domain.JobRunning, domain.JobDone, domain.JobPreempted, domain.JobFailed}
	parts := make([]string, 0, len(order))
	for _, status := range order {
		parts = append(parts, fmt.Sprintf("%s=%d", status, counts[status]))
	}
	return strings.Join(parts, " ")
}

func formatTimeDash(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func emptyDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func writePrettyJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func writePrettyJSONFile(path string, v any) error {
	var buf bytes.Buffer
	if err := writePrettyJSON(&buf, v); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0600)
}

func writeRedactedConfigFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	return writePrettyJSONFile(dst, redactJSON(value))
}

func redactJSON(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, val := range typed {
			if secretKey(key) {
				out[key] = "[REDACTED]"
				continue
			}
			out[key] = redactJSON(val)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, val := range typed {
			out[i] = redactJSON(val)
		}
		return out
	default:
		return value
	}
}

func secretKey(key string) bool {
	lower := strings.ToLower(key)
	for _, marker := range []string{"token", "secret", "password", "api_key", "apikey", "private_storage_key", "authorization"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func filterJobs(values []domain.Job, keep func(domain.Job) bool) []domain.Job {
	out := values[:0]
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}

func filterRecords(values []domain.JobRecord, keep func(domain.JobRecord) bool) []domain.JobRecord {
	out := values[:0]
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}

func filterRunMetrics(values []domain.RunMetric, keep func(domain.RunMetric) bool) []domain.RunMetric {
	out := values[:0]
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}

func filterSessionMetrics(values []domain.SessionMetric, keep func(domain.SessionMetric) bool) []domain.SessionMetric {
	out := values[:0]
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}

func filterRecommendations(values []domain.RecommendationRecord, keep func(domain.RecommendationRecord) bool) []domain.RecommendationRecord {
	out := values[:0]
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}

func limitTail[T any](values []T, limit int) []T {
	if limit <= 0 || len(values) <= limit {
		return values
	}
	return values[len(values)-limit:]
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
