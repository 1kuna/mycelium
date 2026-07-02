package controlcli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const routeContractSchemaVersion = "mycelium.route_contracts.v1"

type routeContractFile struct {
	SchemaVersion string                       `json:"schema_version"`
	EvidenceRoot  string                       `json:"evidence_root,omitempty"`
	Contracts     []routeCertificationContract `json:"contracts"`
}

type routeCertificationContract struct {
	Name                  string `json:"name"`
	URL                   string `json:"url,omitempty"`
	Model                 string `json:"model"`
	Project               string `json:"project,omitempty"`
	Prompt                string `json:"prompt,omitempty"`
	MaxTokens             int    `json:"max_tokens,omitempty"`
	ContextCap            int    `json:"context_cap,omitempty"`
	MinContextCap         int    `json:"min_context_cap,omitempty"`
	ToolName              string `json:"tool_name,omitempty"`
	ToolChoice            string `json:"tool_choice,omitempty"`
	Stream                bool   `json:"stream,omitempty"`
	ExpectNode            string `json:"expect_node,omitempty"`
	RequireJSON           bool   `json:"require_json,omitempty"`
	RequireToolCall       bool   `json:"require_tool_call,omitempty"`
	RejectTextualToolCall bool   `json:"reject_textual_tool_call,omitempty"`
	RequireLiveModel      bool   `json:"require_live_model,omitempty"`
	MinPromptTokens       int    `json:"min_prompt_tokens,omitempty"`
	ExpectFinishReason    string `json:"expect_finish_reason,omitempty"`
	MinContentLength      int    `json:"min_content_length,omitempty"`
	MaxReasoningLength    *int   `json:"max_reasoning_length,omitempty"`
	EvidenceRoot          string `json:"evidence_root,omitempty"`
	RetentionDays         int    `json:"retention_days,omitempty"`
	ConsumerRun           string `json:"consumer_run,omitempty"`
	ConsumerItem          string `json:"consumer_item,omitempty"`
	ConsumerStatus        string `json:"consumer_status,omitempty"`
	ConsumerElapsedMS     int    `json:"consumer_elapsed_ms,omitempty"`
	ConsumerError         string `json:"consumer_error,omitempty"`
	ConsumerResult        string `json:"consumer_result,omitempty"`
	ConsumerQualityGate   string `json:"consumer_quality_gate,omitempty"`
}

type routeCertificationStatusReport struct {
	ContractsPath string                     `json:"contracts_path"`
	EvidenceRoot  string                     `json:"evidence_root"`
	Contracts     []routeCertificationStatus `json:"contracts"`
}

type routeCertificationStatus struct {
	Name              string                        `json:"name"`
	Result            string                        `json:"result"`
	Model             string                        `json:"model"`
	Project           string                        `json:"project,omitempty"`
	MinContextCap     int                           `json:"min_context_cap,omitempty"`
	Stream            bool                          `json:"stream"`
	RequireJSON       bool                          `json:"require_json,omitempty"`
	RequireToolCall   bool                          `json:"require_tool_call,omitempty"`
	RequireLiveModel  bool                          `json:"require_live_model,omitempty"`
	EvidencePath      string                        `json:"evidence_path,omitempty"`
	EvidenceAgeSec    int64                         `json:"evidence_age_sec,omitempty"`
	Failures          []string                      `json:"failures,omitempty"`
	Verification      *gatewayPreflightVerification `json:"verification,omitempty"`
	NextOperatorCheck string                        `json:"next_operator_check,omitempty"`
}

func runRoute(ctx context.Context, args []string, client *http.Client) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: myce route <certify|status>")
	}
	switch args[0] {
	case "certify":
		return runRouteCertify(ctx, args[1:], client)
	case "status":
		return runRouteStatus(args[1:])
	default:
		return fmt.Errorf("usage: myce route <certify|status>")
	}
}

func runRouteCertify(ctx context.Context, args []string, client *http.Client) error {
	fs := flag.NewFlagSet("route certify", flag.ContinueOnError)
	contractsPath := fs.String("contracts", defaultRouteContractsPath(), "route contracts JSON")
	evidenceRootOverride := fs.String("evidence-root", "", "override evidence root")
	urlOverride := fs.String("url", "", "gateway URL override")
	gatewayToken := fs.String("gateway-token", "", "Mycelium gateway bearer token")
	gatewayTokenEnv := fs.String("gateway-token-env", "", "environment variable containing the Mycelium gateway bearer token")
	gatewayTokenFile := fs.String("gateway-token-file", "", "file containing the Mycelium gateway bearer token")
	configPath := fs.String("config", "", "peer config JSON to read gateway_token from")
	contractName := ""
	parseArgs := args
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		contractName = args[0]
		parseArgs = args[1:]
	}
	if err := fs.Parse(parseArgs); err != nil {
		return err
	}
	switch {
	case contractName != "" && fs.NArg() == 0:
	case contractName == "" && fs.NArg() == 1:
		contractName = fs.Arg(0)
	default:
		return fmt.Errorf("usage: myce route certify [flags] <contract-name>")
	}
	contracts, err := loadRouteContracts(*contractsPath)
	if err != nil {
		return err
	}
	contract, ok := findRouteContract(contracts.Contracts, contractName)
	if !ok {
		return fmt.Errorf("route contract %q not found", contractName)
	}
	if err := validateRouteContract(contract); err != nil {
		return err
	}
	gatewayURL := firstNonEmpty(*urlOverride, contract.URL)
	if gatewayURL == "" {
		return fmt.Errorf("route contract %q requires url or --url", contract.Name)
	}
	evidenceRoot := routeEvidenceRoot(contracts, contract, *evidenceRootOverride)
	evidenceDir := filepath.Join(evidenceRoot, contract.Name, time.Now().UTC().Format("20060102T150405Z"))
	preflightArgs := routeContractPreflightArgs(contract, gatewayURL, evidenceDir)
	if *gatewayToken != "" {
		preflightArgs = append(preflightArgs, "--gateway-token", *gatewayToken)
	}
	if *gatewayTokenEnv != "" {
		preflightArgs = append(preflightArgs, "--gateway-token-env", *gatewayTokenEnv)
	}
	if *gatewayTokenFile != "" {
		preflightArgs = append(preflightArgs, "--gateway-token-file", *gatewayTokenFile)
	}
	if *configPath != "" {
		preflightArgs = append(preflightArgs, "--config", *configPath)
	}
	fmt.Printf("route-certify\tcontract=%s\tevidence=%s\n", contract.Name, evidenceDir)
	return runGatewayPreflight(ctx, preflightArgs, client)
}

func runRouteStatus(args []string) error {
	fs := flag.NewFlagSet("route status", flag.ContinueOnError)
	contractsPath := fs.String("contracts", defaultRouteContractsPath(), "route contracts JSON")
	evidenceRootOverride := fs.String("evidence-root", "", "override evidence root")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: myce route status [flags]")
	}
	report, err := buildRouteCertificationStatus(*contractsPath, *evidenceRootOverride, time.Now().UTC())
	if err != nil {
		return err
	}
	if *jsonOut {
		if err := writePrettyJSON(os.Stdout, report); err != nil {
			return err
		}
		return routeCertificationStatusError(report)
	}
	printRouteCertificationStatus(os.Stdout, report)
	return routeCertificationStatusError(report)
}

func routeCertificationStatusError(report routeCertificationStatusReport) error {
	for _, contract := range report.Contracts {
		if contract.Result != "certified" {
			return fmt.Errorf("route certification incomplete")
		}
	}
	return nil
}

func loadRouteContracts(path string) (routeContractFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return routeContractFile{}, fmt.Errorf("read route contracts: %w", err)
	}
	var contracts routeContractFile
	if err := json.Unmarshal(data, &contracts); err != nil {
		return routeContractFile{}, fmt.Errorf("decode route contracts: %w", err)
	}
	if contracts.SchemaVersion != "" && contracts.SchemaVersion != routeContractSchemaVersion {
		return routeContractFile{}, fmt.Errorf("unsupported route contract schema_version %q", contracts.SchemaVersion)
	}
	seen := map[string]struct{}{}
	for _, contract := range contracts.Contracts {
		if err := validateRouteContract(contract); err != nil {
			return routeContractFile{}, err
		}
		if _, ok := seen[contract.Name]; ok {
			return routeContractFile{}, fmt.Errorf("duplicate route contract %q", contract.Name)
		}
		seen[contract.Name] = struct{}{}
	}
	return contracts, nil
}

func validateRouteContract(contract routeCertificationContract) error {
	if strings.TrimSpace(contract.Name) == "" {
		return fmt.Errorf("route contract name is required")
	}
	if strings.TrimSpace(contract.Model) == "" {
		return fmt.Errorf("route contract %q requires model", contract.Name)
	}
	if contract.MaxTokens < 0 || contract.ContextCap < 0 || contract.MinContextCap < 0 || contract.MinPromptTokens < 0 || contract.MinContentLength < 0 || contract.RetentionDays < 0 || contract.ConsumerElapsedMS < 0 {
		return fmt.Errorf("route contract %q has negative numeric field", contract.Name)
	}
	if contract.MaxReasoningLength != nil && *contract.MaxReasoningLength < 0 {
		return fmt.Errorf("route contract %q has negative max_reasoning_length", contract.Name)
	}
	return nil
}

func findRouteContract(contracts []routeCertificationContract, name string) (routeCertificationContract, bool) {
	for _, contract := range contracts {
		if contract.Name == name {
			return contract, true
		}
	}
	return routeCertificationContract{}, false
}

func routeContractPreflightArgs(contract routeCertificationContract, gatewayURL, evidenceDir string) []string {
	args := []string{
		"--url", gatewayURL,
		"--model", contract.Model,
		"--out", evidenceDir,
	}
	appendStringFlag := func(name, value string) {
		if strings.TrimSpace(value) != "" {
			args = append(args, name, value)
		}
	}
	appendIntFlag := func(name string, value int) {
		if value > 0 {
			args = append(args, name, fmt.Sprintf("%d", value))
		}
	}
	appendBoolFlag := func(name string, value bool) {
		if value {
			args = append(args, name)
		}
	}
	appendStringFlag("--project", contract.Project)
	appendStringFlag("--prompt", contract.Prompt)
	appendIntFlag("--max-tokens", contract.MaxTokens)
	appendIntFlag("--context-cap", contract.ContextCap)
	appendStringFlag("--tool-name", contract.ToolName)
	appendStringFlag("--tool-choice", contract.ToolChoice)
	appendStringFlag("--expect-node", contract.ExpectNode)
	appendBoolFlag("--require-json", contract.RequireJSON)
	appendBoolFlag("--require-tool-call", contract.RequireToolCall)
	appendBoolFlag("--reject-textual-tool-call", contract.RejectTextualToolCall)
	appendBoolFlag("--require-live-model", contract.RequireLiveModel)
	appendIntFlag("--min-prompt-tokens", contract.MinPromptTokens)
	appendIntFlag("--min-context-cap", contract.MinContextCap)
	appendStringFlag("--expect-finish-reason", contract.ExpectFinishReason)
	appendIntFlag("--min-content-length", contract.MinContentLength)
	if contract.MaxReasoningLength != nil {
		args = append(args, "--max-reasoning-length", fmt.Sprintf("%d", *contract.MaxReasoningLength))
	}
	appendBoolFlag("--stream", contract.Stream)
	appendStringFlag("--consumer-run", contract.ConsumerRun)
	appendStringFlag("--consumer-item", contract.ConsumerItem)
	appendStringFlag("--consumer-status", contract.ConsumerStatus)
	appendIntFlag("--consumer-elapsed-ms", contract.ConsumerElapsedMS)
	appendStringFlag("--consumer-error", contract.ConsumerError)
	appendStringFlag("--consumer-result", contract.ConsumerResult)
	appendStringFlag("--consumer-quality-gate", contract.ConsumerQualityGate)
	return args
}

func buildRouteCertificationStatus(contractsPath, evidenceRootOverride string, now time.Time) (routeCertificationStatusReport, error) {
	contracts, err := loadRouteContracts(contractsPath)
	if err != nil {
		return routeCertificationStatusReport{}, err
	}
	report := routeCertificationStatusReport{
		ContractsPath: contractsPath,
		EvidenceRoot:  routeEvidenceRoot(contracts, routeCertificationContract{}, evidenceRootOverride),
	}
	for _, contract := range contracts.Contracts {
		status := routeCertificationStatus{
			Name:             contract.Name,
			Model:            contract.Model,
			Project:          contract.Project,
			MinContextCap:    contract.MinContextCap,
			Stream:           contract.Stream,
			RequireJSON:      contract.RequireJSON,
			RequireToolCall:  contract.RequireToolCall,
			RequireLiveModel: contract.RequireLiveModel,
		}
		evidencePath, modTime, ok, err := latestRoutePreflightEvidence(routeEvidenceRoot(contracts, contract, evidenceRootOverride), contract.Name)
		if err != nil {
			return routeCertificationStatusReport{}, err
		}
		if !ok {
			status.Result = "missing_evidence"
			status.NextOperatorCheck = fmt.Sprintf("myce route certify %s --contracts %s", contract.Name, contractsPath)
			report.Contracts = append(report.Contracts, status)
			continue
		}
		status.EvidencePath = evidencePath
		status.EvidenceAgeSec = int64(now.Sub(modTime).Seconds())
		evidence, err := readGatewayPreflightEvidence(evidencePath)
		if err != nil {
			status.Result = "invalid_evidence"
			status.Failures = []string{err.Error()}
			report.Contracts = append(report.Contracts, status)
			continue
		}
		verification, err := verifyGatewayPreflightEvidence(evidence, routeContractExpectationEvidence(contract))
		if err != nil {
			status.Result = "invalid_evidence"
			status.Failures = []string{err.Error()}
			report.Contracts = append(report.Contracts, status)
			continue
		}
		failures := append([]string(nil), verification.Failures...)
		failures = append(failures, routeContractEvidenceFailures(contract, evidence)...)
		verification.Failures = failures
		verification.Passed = len(failures) == 0
		if verification.Passed {
			verification.Result = "pass"
			status.Result = "certified"
		} else {
			verification.Result = "fail"
			status.Result = "failed"
			status.Failures = failures
		}
		status.Verification = &verification
		report.Contracts = append(report.Contracts, status)
	}
	return report, nil
}

func routeEvidenceRoot(contracts routeContractFile, contract routeCertificationContract, override string) string {
	for _, value := range []string{override, contract.EvidenceRoot, contracts.EvidenceRoot} {
		if strings.TrimSpace(value) != "" {
			return expandHomePath(value)
		}
	}
	return defaultRouteEvidenceRoot()
}

func latestRoutePreflightEvidence(root, contractName string) (string, time.Time, bool, error) {
	contractDir := filepath.Join(root, contractName)
	var latestPath string
	var latestMod time.Time
	err := filepath.WalkDir(contractDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return filepath.SkipDir
			}
			return err
		}
		if entry.IsDir() || entry.Name() != "preflight.json" {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if latestPath == "" || info.ModTime().After(latestMod) || (info.ModTime().Equal(latestMod) && path > latestPath) {
			latestPath = path
			latestMod = info.ModTime()
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return "", time.Time{}, false, fmt.Errorf("scan route evidence: %w", err)
	}
	if latestPath == "" {
		return "", time.Time{}, false, nil
	}
	return latestPath, latestMod, true, nil
}

func routeContractExpectationEvidence(contract routeCertificationContract) gatewayPreflightExpectationEvidence {
	expectation := gatewayPreflightExpectationEvidence{
		ExpectNode:            contract.ExpectNode,
		RequireJSON:           contract.RequireJSON,
		RequireToolCall:       contract.RequireToolCall,
		RejectTextualToolCall: contract.RejectTextualToolCall,
		RequireLiveModel:      contract.RequireLiveModel,
		MinPromptTokens:       contract.MinPromptTokens,
		MinContextCap:         contract.MinContextCap,
		ExpectFinishReason:    contract.ExpectFinishReason,
		MinContentLength:      contract.MinContentLength,
	}
	if contract.MaxReasoningLength != nil {
		value := *contract.MaxReasoningLength
		expectation.MaxReasoningLength = &value
	}
	return expectation
}

func routeContractEvidenceFailures(contract routeCertificationContract, evidence gatewayPreflightEvidence) []string {
	var failures []string
	if evidence.Model != contract.Model {
		failures = append(failures, fmt.Sprintf("expected model=%s got %s", contract.Model, emptyDash(evidence.Model)))
	}
	if contract.Project != "" && evidence.Project != contract.Project {
		failures = append(failures, fmt.Sprintf("expected project=%s got %s", contract.Project, emptyDash(evidence.Project)))
	}
	if evidence.Request.Streaming != contract.Stream {
		failures = append(failures, fmt.Sprintf("expected streaming=%t got %t", contract.Stream, evidence.Request.Streaming))
	}
	if contract.ContextCap > 0 && evidence.Request.ContextCap != contract.ContextCap {
		failures = append(failures, fmt.Sprintf("expected context_cap=%d got %d", contract.ContextCap, evidence.Request.ContextCap))
	}
	if contract.ToolName != "" && evidence.Request.ToolName != contract.ToolName {
		failures = append(failures, fmt.Sprintf("expected tool_name=%s got %s", contract.ToolName, emptyDash(evidence.Request.ToolName)))
	}
	if contract.ToolChoice != "" && evidence.Request.ToolChoice != contract.ToolChoice {
		failures = append(failures, fmt.Sprintf("expected tool_choice=%s got %s", contract.ToolChoice, emptyDash(evidence.Request.ToolChoice)))
	}
	return failures
}

func printRouteCertificationStatus(w interface{ Write([]byte) (int, error) }, report routeCertificationStatusReport) {
	fmt.Fprintf(w, "route-status\tcontracts=%s\tevidence_root=%s\n", report.ContractsPath, report.EvidenceRoot)
	for _, contract := range report.Contracts {
		fmt.Fprintf(w, "route\tname=%s\tresult=%s\tmodel=%s\tproject=%s\tmin_context=%d\tstream=%t\tjson=%t\ttool=%t\tlive_model=%t\tevidence=%s\tfailures=%d\tnext=%s\n",
			contract.Name,
			contract.Result,
			contract.Model,
			emptyDash(contract.Project),
			contract.MinContextCap,
			contract.Stream,
			contract.RequireJSON,
			contract.RequireToolCall,
			contract.RequireLiveModel,
			emptyDash(contract.EvidencePath),
			len(contract.Failures),
			emptyDash(contract.NextOperatorCheck),
		)
		for _, failure := range contract.Failures {
			fmt.Fprintf(w, "route-failure\tname=%s\t%s\n", contract.Name, failure)
		}
	}
}

func defaultRouteContractsPath() string {
	return filepath.Join(defaultMyceliumHome(), "route-contracts.json")
}

func defaultRouteEvidenceRoot() string {
	return filepath.Join(defaultMyceliumHome(), "evidence")
}

func expandHomePath(path string) string {
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return home
		}
		return path
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	return path
}
