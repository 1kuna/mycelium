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
	"strconv"
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
		return runDebugJob(ctx, args[1:], client)
	case "bundle":
		return runDebugBundle(ctx, args[1:], client)
	default:
		return fmt.Errorf("usage: myce debug <job|bundle>")
	}
}

func runDebugJob(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("debug job", flag.ContinueOnError)
	configPath := fs.String("config", defaultPeerConfigPath(), "peer config JSON")
	dbPath := fs.String("db", "", "control-plane SQLite store")
	jsonOut := fs.Bool("json", false, "print JSON")
	preflightPath := fs.String("preflight", "", "redacted gateway preflight evidence JSON")
	ownerTelemetry := fs.Bool("owner-telemetry", false, "fetch selected-job run/session telemetry from the owner peer")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token override")
	consumerRun := fs.String("consumer-run", "", "consumer batch/run id to attach to the debug report")
	consumerItem := fs.String("consumer-item", "", "consumer item/row/document id to attach to the debug report")
	consumerStatus := fs.String("consumer-status", "", "consumer outcome: ok, timeout, error, canceled, or partial")
	consumerElapsedMS := fs.Int("consumer-elapsed-ms", 0, "consumer-observed elapsed milliseconds")
	consumerError := fs.String("consumer-error", "", "sanitized consumer error class or short message")
	consumerResult := fs.String("consumer-result", "", "consumer validation result: pass, fail, or unknown")
	consumerQualityGate := fs.String("consumer-quality-gate", "", "sanitized quality or validation gate name")
	jobID, flagArgs, err := splitDebugJobArgs(args)
	if err != nil {
		return err
	}
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	var preflight *gatewayPreflightEvidence
	if *preflightPath != "" {
		evidence, err := readGatewayPreflightEvidence(*preflightPath)
		if err != nil {
			return err
		}
		preflight = &evidence
		evidenceJobID := preflightEvidenceJobID(evidence)
		if evidenceJobID == "" {
			return fmt.Errorf("preflight evidence %s has no X-Myc-Job header", *preflightPath)
		}
		if jobID != "" && jobID != evidenceJobID {
			return fmt.Errorf("preflight evidence job %q does not match requested job %q", evidenceJobID, jobID)
		}
		jobID = evidenceJobID
	}
	if jobID == "" {
		return fmt.Errorf("usage: myce debug job <job-id> [--db path] [--json] [--preflight path] [--owner-telemetry]")
	}
	storePath := debugJobStorePath(*configPath, *dbPath)
	consumer, err := consumerEvidenceFromFlags(*consumerRun, *consumerItem, *consumerStatus, *consumerElapsedMS, *consumerError, *consumerResult, *consumerQualityGate)
	if err != nil {
		return err
	}
	if consumer == nil && preflight != nil {
		consumer = preflight.Consumer
	}
	report, err := buildJobDebugReport(ctx, storePath, jobID)
	if err != nil {
		return err
	}
	report.Preflight = preflight
	report.Consumer = consumer
	if *ownerTelemetry {
		token := *rpcToken
		if token == "" {
			if loaded, err := loadOperatorConfig(*configPath); err == nil {
				token = loaded.Config.RPCToken
			}
		}
		report.OwnerTelemetry = fetchJobOwnerTelemetry(ctx, storePath, report, token, clientOrDefault(client))
	}
	report.Diagnosis = diagnoseJobDebugReport(report)
	report.Optimization = summarizeJobOptimizationEvidence(report)
	if *jsonOut {
		return writePrettyJSON(os.Stdout, report)
	}
	printJobDebugReport(os.Stdout, report)
	return nil
}

func debugJobStorePath(configPath, dbPath string) string {
	if dbPath != "" {
		return dbPath
	}
	if loaded, err := loadOperatorConfig(configPath); err == nil && loaded.Config.StorePath != "" {
		return loaded.Config.StorePath
	}
	return defaultControlStorePath()
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
	return arg == "-config" || arg == "--config" || arg == "-db" || arg == "--db" || arg == "-preflight" || arg == "--preflight" || arg == "-rpc-token" || arg == "--rpc-token" || arg == "--consumer-run" || arg == "--consumer-item" || arg == "--consumer-status" || arg == "--consumer-elapsed-ms" || arg == "--consumer-error" || arg == "--consumer-result" || arg == "--consumer-quality-gate"
}

func runDebugBundle(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("debug bundle", flag.ContinueOnError)
	configPath := fs.String("config", defaultPeerConfigPath(), "peer config JSON")
	dbPath := fs.String("db", "", "control-plane SQLite store")
	jobID := fs.String("job", "", "job id to focus")
	preflightPath := fs.String("preflight", "", "redacted gateway preflight evidence JSON")
	ownerTelemetry := fs.Bool("owner-telemetry", false, "fetch selected-job run/session telemetry from the owner peer")
	out := fs.String("out", "", "output directory")
	url := fs.String("url", "", "peer URL override")
	rpcToken := fs.String("rpc-token", "", "peer RPC bearer token override")
	consumerRun := fs.String("consumer-run", "", "consumer batch/run id to attach to the debug report")
	consumerItem := fs.String("consumer-item", "", "consumer item/row/document id to attach to the debug report")
	consumerStatus := fs.String("consumer-status", "", "consumer outcome: ok, timeout, error, canceled, or partial")
	consumerElapsedMS := fs.Int("consumer-elapsed-ms", 0, "consumer-observed elapsed milliseconds")
	consumerError := fs.String("consumer-error", "", "sanitized consumer error class or short message")
	consumerResult := fs.String("consumer-result", "", "consumer validation result: pass, fail, or unknown")
	consumerQualityGate := fs.String("consumer-quality-gate", "", "sanitized quality or validation gate name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	consumer, err := consumerEvidenceFromFlags(*consumerRun, *consumerItem, *consumerStatus, *consumerElapsedMS, *consumerError, *consumerResult, *consumerQualityGate)
	if err != nil {
		return err
	}
	var preflightConsumer *consumerEvidence
	if *preflightPath != "" {
		evidence, err := readGatewayPreflightEvidence(*preflightPath)
		if err != nil {
			return err
		}
		preflightConsumer = evidence.Consumer
		evidenceJobID := preflightEvidenceJobID(evidence)
		if evidenceJobID == "" {
			return fmt.Errorf("preflight evidence %s has no X-Myc-Job header", *preflightPath)
		}
		if *jobID != "" && *jobID != evidenceJobID {
			return fmt.Errorf("preflight evidence job %q does not match requested job %q", evidenceJobID, *jobID)
		}
		*jobID = evidenceJobID
	}
	if consumer == nil {
		consumer = preflightConsumer
	}
	if err := writeDebugBundle(ctx, debugBundleRequest{
		ConfigPath:     *configPath,
		DBPath:         *dbPath,
		JobID:          *jobID,
		PreflightPath:  *preflightPath,
		OwnerTelemetry: *ownerTelemetry,
		Consumer:       consumer,
		OutDir:         *out,
		URL:            *url,
		RPCToken:       *rpcToken,
		Client:         client,
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
	Preflight       *gatewayPreflightEvidence     `json:"preflight,omitempty"`
	Consumer        *consumerEvidence             `json:"consumer,omitempty"`
	OwnerTelemetry  *ownerTelemetryReport         `json:"owner_telemetry,omitempty"`
	Diagnosis       *jobDebugDiagnosis            `json:"diagnosis,omitempty"`
	Optimization    *jobOptimizationEvidence      `json:"optimization,omitempty"`
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
	if report.Preflight != nil {
		printPreflightEvidenceSummary(w, *report.Preflight)
	}
	if report.OwnerTelemetry != nil {
		printOwnerTelemetrySummary(w, *report.OwnerTelemetry)
	}
	if report.Consumer != nil {
		printConsumerEvidenceSummary(w, *report.Consumer)
	}
	if report.Diagnosis != nil {
		fmt.Fprintf(w, "diagnosis\t%s\t%s\tnext=%s\n", report.Diagnosis.Class, report.Diagnosis.Summary, report.Diagnosis.Next)
	}
	if report.Optimization != nil {
		printOptimizationEvidenceSummary(w, *report.Optimization)
	}
	events := jobTimelineEvents(report)
	for _, event := range events {
		fmt.Fprintf(w, "timeline\t%s\t%s\t%s\t%s\n", formatTimeDash(event.At), event.Kind, event.Subject, event.Detail)
	}
	for _, hint := range report.Hints {
		fmt.Fprintf(w, "hint\t%s\n", hint)
	}
}

type consumerEvidence struct {
	RunID       string `json:"run_id,omitempty"`
	ItemID      string `json:"item_id,omitempty"`
	Status      string `json:"status,omitempty"`
	ElapsedMS   int    `json:"elapsed_ms,omitempty"`
	Error       string `json:"error,omitempty"`
	Result      string `json:"result,omitempty"`
	QualityGate string `json:"quality_gate,omitempty"`
}

func consumerEvidenceFromFlags(runID, itemID, status string, elapsedMS int, errorText, result, qualityGate string) (*consumerEvidence, error) {
	if elapsedMS < 0 {
		return nil, fmt.Errorf("--consumer-elapsed-ms must be non-negative")
	}
	status = strings.ToLower(strings.TrimSpace(status))
	switch status {
	case "", "ok", "timeout", "error", "canceled", "partial":
	default:
		return nil, fmt.Errorf("--consumer-status must be one of ok, timeout, error, canceled, or partial")
	}
	result = strings.ToLower(strings.TrimSpace(result))
	switch result {
	case "", "pass", "fail", "unknown":
	default:
		return nil, fmt.Errorf("--consumer-result must be one of pass, fail, or unknown")
	}
	evidence := consumerEvidence{
		RunID:       strings.TrimSpace(runID),
		ItemID:      strings.TrimSpace(itemID),
		Status:      status,
		ElapsedMS:   elapsedMS,
		Error:       strings.TrimSpace(errorText),
		Result:      result,
		QualityGate: strings.TrimSpace(qualityGate),
	}
	if evidence.RunID == "" && evidence.ItemID == "" && evidence.Status == "" && evidence.ElapsedMS == 0 && evidence.Error == "" && evidence.Result == "" && evidence.QualityGate == "" {
		return nil, nil
	}
	return &evidence, nil
}

func printConsumerEvidenceSummary(w io.Writer, evidence consumerEvidence) {
	fmt.Fprintf(w, "consumer\trun=%s\titem=%s\tstatus=%s\telapsed_ms=%d\terror=%s\tresult=%s\tquality_gate=%s\n", emptyDash(evidence.RunID), emptyDash(evidence.ItemID), emptyDash(evidence.Status), evidence.ElapsedMS, emptyDash(evidence.Error), emptyDash(evidence.Result), emptyDash(evidence.QualityGate))
}

type ownerTelemetryReport struct {
	NodeID          string                 `json:"node_id"`
	Address         string                 `json:"address,omitempty"`
	RunMetrics      []domain.RunMetric     `json:"run_metrics,omitempty"`
	SessionTimeline []domain.SessionMetric `json:"session_timeline,omitempty"`
	Error           string                 `json:"error,omitempty"`
}

func printOwnerTelemetrySummary(w io.Writer, report ownerTelemetryReport) {
	status := "ok"
	if report.Error != "" {
		status = "fail"
	}
	fmt.Fprintf(w, "owner-telemetry\t%s\tnode=%s\taddress=%s\tmetrics=%d\tsamples=%d", status, emptyDash(report.NodeID), emptyDash(report.Address), len(report.RunMetrics), len(report.SessionTimeline))
	if len(report.RunMetrics) > 0 {
		metric := report.RunMetrics[0]
		fmt.Fprintf(w, "\tttft_ms=%d\tload_ms=%d\ttps=%.2f\tcontext=%d", metric.TTFTms, metric.LoadWallClockMS, metric.TokensPerSec, metric.ContextUsed)
	}
	if len(report.SessionTimeline) > 0 {
		fmt.Fprintf(w, "\tphases=%s", ownerTelemetryPhases(report.SessionTimeline))
	}
	if report.Error != "" {
		fmt.Fprintf(w, "\terror=%s", report.Error)
	}
	fmt.Fprintln(w)
}

func ownerTelemetryPhases(samples []domain.SessionMetric) string {
	phases := make([]string, 0, len(samples))
	for _, sample := range samples {
		phases = append(phases, string(sample.Phase))
	}
	return strings.Join(phases, ",")
}

type jobDebugDiagnosis struct {
	Class   string `json:"class"`
	Summary string `json:"summary"`
	Next    string `json:"next"`
}

type jobOptimizationEvidence struct {
	JobID                   string                               `json:"job_id"`
	Project                 string                               `json:"project,omitempty"`
	Consumer                *consumerEvidence                    `json:"consumer,omitempty"`
	Gateway                 string                               `json:"gateway,omitempty"`
	Model                   string                               `json:"model,omitempty"`
	Request                 gatewayPreflightRequestEvidence      `json:"request,omitempty"`
	Expectations            *gatewayPreflightExpectationEvidence `json:"expectations,omitempty"`
	NodeID                  string                               `json:"node_id,omitempty"`
	Backend                 string                               `json:"backend,omitempty"`
	InstanceID              string                               `json:"instance_id,omitempty"`
	PresetID                string                               `json:"preset_id,omitempty"`
	Decision                string                               `json:"decision,omitempty"`
	Attempts                int                                  `json:"attempts,omitempty"`
	ColdLoad                *bool                                `json:"cold_load,omitempty"`
	QueueWaitMS             int                                  `json:"queue_wait_ms,omitempty"`
	LoadWallMS              int                                  `json:"load_wall_ms,omitempty"`
	TTFTMS                  int                                  `json:"ttft_ms,omitempty"`
	UpstreamWallMS          int                                  `json:"upstream_wall_ms,omitempty"`
	UsageAvailable          bool                                 `json:"usage_available"`
	UsagePromptTokens       int                                  `json:"usage_prompt_tokens,omitempty"`
	UsageCompletionTokens   int                                  `json:"usage_completion_tokens,omitempty"`
	UsageTotalTokens        int                                  `json:"usage_total_tokens,omitempty"`
	ContextUsed             int                                  `json:"context_used,omitempty"`
	ContextCap              int                                  `json:"context_cap,omitempty"`
	MinContextCap           int                                  `json:"min_context_cap,omitempty"`
	ContextAdequate         bool                                 `json:"context_adequate,omitempty"`
	FinishReason            string                               `json:"finish_reason,omitempty"`
	JSONContent             bool                                 `json:"json_content,omitempty"`
	ToolCallCount           int                                  `json:"tool_call_count,omitempty"`
	ToolCallNames           []string                             `json:"tool_call_names,omitempty"`
	TextualToolCall         bool                                 `json:"textual_tool_call,omitempty"`
	OwnerTelemetryAvailable bool                                 `json:"owner_telemetry_available"`
	MetricSource            string                               `json:"metric_source,omitempty"`
	SessionPhases           []string                             `json:"session_phases,omitempty"`
	FailureClass            string                               `json:"failure_class,omitempty"`
	FailureLayer            string                               `json:"failure_layer,omitempty"`
	ConsumerStatus          string                               `json:"consumer_status,omitempty"`
	ConsumerError           string                               `json:"consumer_error,omitempty"`
	ConsumerResult          string                               `json:"consumer_result,omitempty"`
	ConsumerQualityGate     string                               `json:"consumer_quality_gate,omitempty"`
}

func summarizeJobOptimizationEvidence(report jobDebugReport) *jobOptimizationEvidence {
	out := &jobOptimizationEvidence{
		JobID:    report.Job.ID,
		Project:  report.Job.Project,
		Model:    report.Job.Model,
		Consumer: report.Consumer,
	}
	if report.Preflight != nil {
		out.Gateway = report.Preflight.Gateway
		out.Project = firstNonEmpty(report.Preflight.Project, out.Project)
		out.Model = firstNonEmpty(report.Preflight.Model, out.Model)
		out.Request = report.Preflight.Request
		out.Expectations = report.Preflight.Expectations
		out.Decision = preflightHeader(report, "X-Myc-Decision")
		out.NodeID = preflightHeader(report, "X-Myc-Node")
		out.Backend = preflightHeader(report, "X-Myc-Backend")
		out.InstanceID = preflightHeader(report, "X-Myc-Instance")
		out.Attempts = preflightHeaderInt(report, "X-Myc-Attempts")
		out.QueueWaitMS = preflightHeaderInt(report, "X-Myc-Queue-Wait-Ms")
		out.UpstreamWallMS = preflightHeaderInt(report, "X-Myc-Upstream-Wall-Ms")
		if cold, ok := preflightHeaderBool(report, "X-Myc-Cold-Load"); ok {
			out.ColdLoad = &cold
		}
		out.FinishReason = report.Preflight.FinishReason
		out.JSONContent = report.Preflight.JSONContent
		out.ToolCallCount = report.Preflight.ToolCallCount
		out.ToolCallNames = report.Preflight.ToolCallNames
		out.TextualToolCall = report.Preflight.TextualToolCall
		out.UsageAvailable = report.Preflight.UsageAvailable
		out.UsagePromptTokens = report.Preflight.Usage.PromptTokens
		out.UsageCompletionTokens = report.Preflight.Usage.CompletionTokens
		out.UsageTotalTokens = report.Preflight.Usage.TotalTokens
		out.ContextUsed = report.Preflight.Usage.TotalTokens
		out.ContextCap = report.Preflight.Request.ContextCap
		if report.Preflight.Expectations != nil {
			out.MinContextCap = report.Preflight.Expectations.MinContextCap
		}
	}
	if report.Consumer != nil {
		out.ConsumerStatus = report.Consumer.Status
		out.ConsumerError = report.Consumer.Error
		out.ConsumerResult = report.Consumer.Result
		out.ConsumerQualityGate = report.Consumer.QualityGate
	}
	if metric, ok, source := primaryRunMetric(report); ok {
		out.MetricSource = source
		out.NodeID = firstNonEmpty(out.NodeID, metric.NodeID)
		out.Backend = firstNonEmpty(out.Backend, string(metric.Backend))
		out.InstanceID = firstNonEmpty(out.InstanceID, metric.InstanceID)
		out.PresetID = metric.PresetID
		out.Project = firstNonEmpty(out.Project, metric.Project)
		out.TTFTMS = metric.TTFTms
		out.LoadWallMS = metric.LoadWallClockMS
		if out.ContextUsed == 0 {
			out.ContextUsed = metric.ContextUsed
		}
		out.OwnerTelemetryAvailable = source == "owner"
	}
	out.SessionPhases = sessionPhaseNames(primarySessionTimeline(report))
	if report.Diagnosis != nil {
		out.FailureClass = report.Diagnosis.Class
		out.FailureLayer = failureLayerForDiagnosis(*report.Diagnosis, report)
	}
	if out.MinContextCap > 0 {
		out.ContextAdequate = out.ContextCap >= out.MinContextCap
		if !out.ContextAdequate {
			out.FailureClass = "consumer_contract_context_too_small"
			out.FailureLayer = "consumer_contract"
		}
	}
	if out.ConsumerResult == "fail" && (out.FailureLayer == "" || out.FailureLayer == "none" || out.FailureLayer == "consumer") {
		out.FailureClass = "consumer_quality_failed"
		out.FailureLayer = "quality"
	}
	return out
}

func primaryRunMetric(report jobDebugReport) (domain.RunMetric, bool, string) {
	if report.OwnerTelemetry != nil && report.OwnerTelemetry.Error == "" && len(report.OwnerTelemetry.RunMetrics) > 0 {
		return report.OwnerTelemetry.RunMetrics[0], true, "owner"
	}
	if len(report.RunMetrics) > 0 {
		return report.RunMetrics[0], true, "coordinator"
	}
	return domain.RunMetric{}, false, ""
}

func primarySessionTimeline(report jobDebugReport) []domain.SessionMetric {
	if report.OwnerTelemetry != nil && report.OwnerTelemetry.Error == "" && len(report.OwnerTelemetry.SessionTimeline) > 0 {
		return report.OwnerTelemetry.SessionTimeline
	}
	return report.SessionTimeline
}

func sessionPhaseNames(samples []domain.SessionMetric) []string {
	if len(samples) == 0 {
		return nil
	}
	phases := make([]string, 0, len(samples))
	for _, sample := range samples {
		phases = append(phases, string(sample.Phase))
	}
	return phases
}

func failureLayerForDiagnosis(diagnosis jobDebugDiagnosis, report jobDebugReport) string {
	if report.Consumer != nil && report.Consumer.Status != "" && report.Consumer.Status != "ok" && strings.HasPrefix(diagnosis.Class, "owner_completed") {
		return "consumer"
	}
	switch diagnosis.Class {
	case "owner_telemetry_unavailable":
		return "telemetry"
	case "owner_failed":
		return "owner"
	case "cleanup_required", "inflight_or_stale":
		return "runtime"
	case "job_done_without_owner_complete", "insufficient_evidence":
		return "evidence"
	default:
		if strings.HasPrefix(diagnosis.Class, "owner_completed") {
			return "none"
		}
		return ""
	}
}

func printOptimizationEvidenceSummary(w io.Writer, evidence jobOptimizationEvidence) {
	fmt.Fprintf(w, "optimization\tjob=%s\tproject=%s\tconsumer_status=%s\tconsumer_result=%s\tquality_gate=%s\tnode=%s\tpreset=%s\tmodel=%s\tcontext_cap=%d\tmin_context=%d\tcontext_ok=%t\tstream=%t\ttool=%t\tusage=%d/%d/%d\tload_ms=%d\tttft_ms=%d\tupstream_ms=%d\tfailure_layer=%s\tfailure_class=%s\n",
		emptyDash(evidence.JobID),
		emptyDash(evidence.Project),
		emptyDash(evidence.ConsumerStatus),
		emptyDash(evidence.ConsumerResult),
		emptyDash(evidence.ConsumerQualityGate),
		emptyDash(evidence.NodeID),
		emptyDash(evidence.PresetID),
		emptyDash(evidence.Model),
		evidence.ContextCap,
		evidence.MinContextCap,
		evidence.ContextAdequate,
		evidence.Request.Streaming,
		evidence.Request.ToolRequested,
		evidence.UsagePromptTokens,
		evidence.UsageCompletionTokens,
		evidence.UsageTotalTokens,
		evidence.LoadWallMS,
		evidence.TTFTMS,
		evidence.UpstreamWallMS,
		emptyDash(evidence.FailureLayer),
		emptyDash(evidence.FailureClass),
	)
}

func diagnoseJobDebugReport(report jobDebugReport) *jobDebugDiagnosis {
	if report.OwnerTelemetry != nil && report.OwnerTelemetry.Error != "" {
		return &jobDebugDiagnosis{
			Class:   "owner_telemetry_unavailable",
			Summary: report.OwnerTelemetry.Error,
			Next:    "fix peer RPC auth/address or preserve request-time preflight headers",
		}
	}
	if report.Registry != nil && report.Registry.CleanupRequired {
		return &jobDebugDiagnosis{
			Class:   "cleanup_required",
			Summary: "registry still requires cleanup on the assigned owner",
			Next:    "preserve a debug bundle and inspect owner state before retrying scale work",
		}
	}
	if ownerTelemetryHasPhase(report.OwnerTelemetry, domain.TelemetryPhaseComplete) {
		return &jobDebugDiagnosis{
			Class:   completedDiagnosisClass(report),
			Summary: completedDiagnosisSummary(report),
			Next:    "if the consumer timed out or missed summary output, classify it as downstream of completed owner inference",
		}
	}
	if ownerTelemetryHasPhase(report.OwnerTelemetry, domain.TelemetryPhaseError) || report.Job.Status == domain.JobFailed || (report.Registry != nil && report.Registry.Status == domain.JobFailed) {
		return &jobDebugDiagnosis{
			Class:   "owner_failed",
			Summary: firstNonEmpty(jobFailureText(report), "owner or coordinator recorded a failed job"),
			Next:    "inspect hints, owner session error samples, and backend logs for the selected owner",
		}
	}
	if report.Job.Status == domain.JobDone || (report.Registry != nil && report.Registry.Status == domain.JobDone) {
		return &jobDebugDiagnosis{
			Class:   "job_done_without_owner_complete",
			Summary: "durable job is done but owner completion telemetry was not recovered",
			Next:    "rerun with --owner-telemetry and valid peer RPC auth or inspect the owner control DB",
		}
	}
	if report.Job.Status == domain.JobPlacing || report.Job.Status == domain.JobLoading || report.Job.Status == domain.JobRunning || (report.Registry != nil && (report.Registry.Status == domain.JobPlacing || report.Registry.Status == domain.JobLoading || report.Registry.Status == domain.JobRunning)) {
		return &jobDebugDiagnosis{
			Class:   "inflight_or_stale",
			Summary: "job or registry is still non-terminal",
			Next:    "wait a bounded interval, rerun debug job, then preserve a bundle if state remains non-terminal",
		}
	}
	return &jobDebugDiagnosis{
		Class:   "insufficient_evidence",
		Summary: "job state does not identify owner completion, owner failure, or stale cleanup",
		Next:    "preserve X-Myc-Job/preflight evidence and rerun with --owner-telemetry",
	}
}

func ownerTelemetryHasPhase(report *ownerTelemetryReport, phase domain.TelemetryPhase) bool {
	if report == nil || report.Error != "" {
		return false
	}
	for _, sample := range report.SessionTimeline {
		if sample.Phase == phase {
			return true
		}
	}
	return false
}

func completedDiagnosisClass(report jobDebugReport) string {
	if preflightHeader(report, "X-Myc-Cold-Load") == "true" {
		return "owner_completed_cold_load"
	}
	if report.OwnerTelemetry != nil && len(report.OwnerTelemetry.RunMetrics) > 0 && report.OwnerTelemetry.RunMetrics[0].LoadWallClockMS > 0 {
		return "owner_completed_cold_load"
	}
	return "owner_completed_warm"
}

func completedDiagnosisSummary(report jobDebugReport) string {
	owner := jobOwnerNode(report)
	loadMS, ttftMS := 0, 0
	if report.OwnerTelemetry != nil && len(report.OwnerTelemetry.RunMetrics) > 0 {
		metric := report.OwnerTelemetry.RunMetrics[0]
		loadMS = metric.LoadWallClockMS
		ttftMS = metric.TTFTms
	}
	placement := "warm"
	if preflightHeader(report, "X-Myc-Cold-Load") == "true" || loadMS > 0 {
		placement = "cold"
	}
	parts := []string{
		fmt.Sprintf("owner %s completed %s inference", emptyDash(owner), placement),
		fmt.Sprintf("load_ms=%d", loadMS),
		fmt.Sprintf("ttft_ms=%d", ttftMS),
	}
	if report.Consumer != nil && report.Consumer.Status != "" && report.Consumer.Status != "ok" {
		parts = append(parts, "consumer_status="+report.Consumer.Status)
	}
	if upstreamMS := preflightHeader(report, "X-Myc-Upstream-Wall-Ms"); upstreamMS != "" {
		parts = append(parts, "upstream_ms="+upstreamMS)
	}
	if structured := preflightHeader(report, "X-Myc-Structured-Output"); structured != "" {
		parts = append(parts, "structured="+structured)
	}
	return strings.Join(parts, "; ")
}

func jobFailureText(report jobDebugReport) string {
	if report.Job.Error != "" {
		return report.Job.Error
	}
	if report.Registry != nil {
		if report.Registry.CleanupError != "" {
			return report.Registry.CleanupError
		}
		if report.Registry.RecoveryNote != "" {
			return report.Registry.RecoveryNote
		}
	}
	if report.OwnerTelemetry != nil {
		for _, sample := range report.OwnerTelemetry.SessionTimeline {
			if sample.Error != "" {
				return sample.Error
			}
		}
	}
	for _, sample := range report.SessionTimeline {
		if sample.Error != "" {
			return sample.Error
		}
	}
	return ""
}

func preflightHeader(report jobDebugReport, key string) string {
	if report.Preflight == nil || report.Preflight.Headers == nil {
		return ""
	}
	if value := report.Preflight.Headers[key]; value != "" {
		return value
	}
	for name, value := range report.Preflight.Headers {
		if strings.EqualFold(name, key) {
			return value
		}
	}
	return ""
}

func preflightHeaderInt(report jobDebugReport, key string) int {
	value := strings.TrimSpace(preflightHeader(report, key))
	if value == "" {
		return 0
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return n
}

func preflightHeaderBool(report jobDebugReport, key string) (bool, bool) {
	value := strings.TrimSpace(preflightHeader(report, key))
	if value == "" {
		return false, false
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, false
	}
	return parsed, true
}

func printPreflightEvidenceSummary(w io.Writer, evidence gatewayPreflightEvidence) {
	headers := evidence.Headers
	fmt.Fprintf(w, "preflight\tstatus=%d\tmodel=%s\tproject=%s\tfinish=%s\tcontent=%d\treasoning=%d\ttool_calls=%d\ttextual_tool=%t\tjson=%t\n", evidence.StatusCode, emptyDash(evidence.Model), emptyDash(evidence.Project), emptyDash(evidence.FinishReason), evidence.ContentLength, evidence.ReasoningLength, evidence.ToolCallCount, evidence.TextualToolCall, evidence.JSONContent)
	if evidence.Request.MaxTokens != 0 || evidence.Request.StructuredOutput != "" || evidence.Request.ToolRequested || evidence.Request.Streaming {
		fmt.Fprintf(w, "preflight-request\tmax_tokens=%d\tstream=%t\tstructured=%s\ttool_requested=%t\ttool_name=%s\ttool_choice=%s\n",
			evidence.Request.MaxTokens,
			evidence.Request.Streaming,
			emptyDash(evidence.Request.StructuredOutput),
			evidence.Request.ToolRequested,
			emptyDash(evidence.Request.ToolName),
			emptyDash(evidence.Request.ToolChoice),
		)
	}
	if evidence.Expectations != nil {
		fmt.Fprintf(w, "preflight-expectations\texpect_node=%s\trequire_json=%t\trequire_tool_call=%t\treject_textual_tool_call=%t\tmin_prompt_tokens=%d\n",
			emptyDash(evidence.Expectations.ExpectNode),
			evidence.Expectations.RequireJSON,
			evidence.Expectations.RequireToolCall,
			evidence.Expectations.RejectTextualToolCall,
			evidence.Expectations.MinPromptTokens,
		)
	}
	if evidence.Streaming {
		fmt.Fprintf(w, "preflight-stream\tdone=%t\tusage_available=%t\n", evidence.StreamDone, evidence.UsageAvailable)
	}
	fmt.Fprintf(w, "preflight-route\tnode=%s\tbackend=%s\tinstance=%s\tdecision=%s\tattempts=%s\tcold=%s\tqueue_ms=%s\tupstream_ms=%s\tstructured=%s\tguided=%s\n",
		emptyDash(headers["X-Myc-Node"]),
		emptyDash(headers["X-Myc-Backend"]),
		emptyDash(headers["X-Myc-Instance"]),
		emptyDash(headers["X-Myc-Decision"]),
		emptyDash(headers["X-Myc-Attempts"]),
		emptyDash(headers["X-Myc-Cold-Load"]),
		emptyDash(headers["X-Myc-Queue-Wait-Ms"]),
		emptyDash(headers["X-Myc-Upstream-Wall-Ms"]),
		emptyDash(headers["X-Myc-Structured-Output"]),
		emptyDash(headers["X-Myc-Guided-Decoding-Requested"]),
	)
	if headers["X-Myc-Tools-Requested"] != "" || headers["X-Myc-Tool-Choice"] != "" || headers["X-Myc-VLLM-Tool-Parser-Configured"] != "" || headers["X-Myc-VLLM-Auto-Tool-Choice-Configured"] != "" {
		fmt.Fprintf(w, "preflight-tools\trequested=%s\tchoice=%s\tvllm_parser=%s\tvllm_auto=%s\n",
			emptyDash(headers["X-Myc-Tools-Requested"]),
			emptyDash(headers["X-Myc-Tool-Choice"]),
			emptyDash(headers["X-Myc-VLLM-Tool-Parser-Configured"]),
			emptyDash(headers["X-Myc-VLLM-Auto-Tool-Choice-Configured"]),
		)
	}
	if evidence.ToolCallCount > 0 || evidence.TextualToolCall {
		fmt.Fprintf(w, "preflight-tool-result\tcount=%d\tnames=%s\ttextual=%t\n",
			evidence.ToolCallCount,
			emptyDash(strings.Join(evidence.ToolCallNames, ",")),
			evidence.TextualToolCall,
		)
	}
	if evidence.Usage.TotalTokens != 0 || evidence.Usage.PromptTokens != 0 || evidence.Usage.CompletionTokens != 0 {
		fmt.Fprintf(w, "preflight-usage\tprompt=%d\tcompletion=%d\ttotal=%d\n", evidence.Usage.PromptTokens, evidence.Usage.CompletionTokens, evidence.Usage.TotalTokens)
	} else if evidence.Streaming {
		fmt.Fprintln(w, "preflight-usage\tunavailable")
	}
}

func readGatewayPreflightEvidence(path string) (gatewayPreflightEvidence, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return gatewayPreflightEvidence{}, fmt.Errorf("read gateway preflight evidence: %w", err)
	}
	var evidence gatewayPreflightEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		return gatewayPreflightEvidence{}, fmt.Errorf("decode gateway preflight evidence: %w", err)
	}
	return evidence, nil
}

func preflightEvidenceJobID(evidence gatewayPreflightEvidence) string {
	if evidence.Headers == nil {
		return ""
	}
	if value := evidence.Headers["X-Myc-Job"]; value != "" {
		return value
	}
	for name, value := range evidence.Headers {
		if strings.EqualFold(name, "X-Myc-Job") {
			return value
		}
	}
	return ""
}

func copyPreflightEvidenceFile(src, dst string) error {
	evidence, err := readGatewayPreflightEvidence(src)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(dst, append(data, '\n'), 0600)
}

func fetchJobOwnerTelemetry(ctx context.Context, dbPath string, report jobDebugReport, rpcToken string, client *http.Client) *ownerTelemetryReport {
	owner := jobOwnerNode(report)
	out := &ownerTelemetryReport{NodeID: owner}
	if owner == "" {
		out.Error = "owner node is not known"
		return out
	}
	if rpcToken == "" {
		out.Error = "rpc token is required for owner telemetry"
		return out
	}
	store, err := storesqlite.Open(dbPath)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	defer store.Close()
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	for _, node := range nodes {
		if node.ID == owner {
			out.Address = node.Address
			break
		}
	}
	if out.Address == "" {
		out.Error = fmt.Sprintf("owner node %q has no known address", owner)
		return out
	}
	metrics, err := fetchOwnerRunMetrics(ctx, client, out.Address, rpcToken)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	for _, metric := range metrics {
		if metric.JobID == report.Job.ID {
			out.RunMetrics = append(out.RunMetrics, metric)
		}
	}
	samples, err := fetchOwnerSessionMetrics(ctx, client, out.Address, rpcToken, report.Job.ID)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	for _, sample := range samples {
		if sample.JobID == report.Job.ID {
			out.SessionTimeline = append(out.SessionTimeline, sample)
		}
	}
	return out
}

func jobOwnerNode(report jobDebugReport) string {
	if report.Registry != nil && report.Registry.AssignedNode != "" {
		return report.Registry.AssignedNode
	}
	if report.Preflight != nil {
		if node := report.Preflight.Headers["X-Myc-Node"]; node != "" {
			return node
		}
		for name, value := range report.Preflight.Headers {
			if strings.EqualFold(name, "X-Myc-Node") {
				return value
			}
		}
	}
	if len(report.RunMetrics) > 0 && report.RunMetrics[0].NodeID != "" {
		return report.RunMetrics[0].NodeID
	}
	return ""
}

func fetchOwnerRunMetrics(ctx context.Context, client *http.Client, address, rpcToken string) ([]domain.RunMetric, error) {
	var metrics []domain.RunMetric
	if err := fetchOwnerTelemetryJSON(ctx, client, address, rpcToken, "/telemetry/metrics", &metrics); err != nil {
		return nil, err
	}
	return metrics, nil
}

func fetchOwnerSessionMetrics(ctx context.Context, client *http.Client, address, rpcToken, jobID string) ([]domain.SessionMetric, error) {
	var samples []domain.SessionMetric
	path := "/telemetry/samples"
	if jobID != "" {
		values := neturl.Values{}
		values.Set("job_id", jobID)
		path += "?" + values.Encode()
	}
	if err := fetchOwnerTelemetryJSON(ctx, client, address, rpcToken, path, &samples); err != nil {
		return nil, err
	}
	return samples, nil
}

func fetchOwnerTelemetryJSON(ctx context.Context, client *http.Client, address, rpcToken, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(catalogPeerBaseURL(address), "/")+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+rpcToken)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := readControlHTTPBody(resp.Body, "owner telemetry response body")
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("owner telemetry %s: %s", address, strings.TrimSpace(string(data)))
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode owner telemetry %s: %w", address, err)
	}
	return nil
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
	ConfigPath     string
	DBPath         string
	JobID          string
	PreflightPath  string
	OwnerTelemetry bool
	Consumer       *consumerEvidence
	OutDir         string
	URL            string
	RPCToken       string
	Client         *http.Client
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
	if req.PreflightPath != "" {
		if err := copyPreflightEvidenceFile(req.PreflightPath, addFile("preflight.json")); err != nil {
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
	if req.OwnerTelemetry && req.JobID != "" {
		report, err := buildJobDebugReport(ctx, storePath, req.JobID)
		if err != nil {
			manifest.Issues = append(manifest.Issues, err.Error())
		} else {
			if req.PreflightPath != "" {
				if evidence, err := readGatewayPreflightEvidence(req.PreflightPath); err == nil {
					report.Preflight = &evidence
				} else {
					manifest.Issues = append(manifest.Issues, err.Error())
				}
			}
			report.Consumer = req.Consumer
			report.OwnerTelemetry = fetchJobOwnerTelemetry(ctx, storePath, report, firstNonEmpty(req.RPCToken, cfg.RPCToken), clientOrDefault(req.Client))
			report.Diagnosis = diagnoseJobDebugReport(report)
			report.Optimization = summarizeJobOptimizationEvidence(report)
			if err := writePrettyJSONFile(addFile("job_debug.json"), report); err != nil {
				return err
			}
			if report.Optimization != nil {
				if err := writePrettyJSONFile(addFile("optimization.json"), report.Optimization); err != nil {
					return err
				}
			}
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
