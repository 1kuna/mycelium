//go:build smoke

package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/internal/gateway"
)

func TestVisionAutonomousFleetSmoke(t *testing.T) {
	gatewayURL := strings.TrimRight(os.Getenv("MYCELIUM_VISION_GATEWAY"), "/")
	model := strings.TrimSpace(os.Getenv("MYCELIUM_VISION_MODEL"))
	if gatewayURL == "" || model == "" {
		t.Skip("set MYCELIUM_VISION_GATEWAY and MYCELIUM_VISION_MODEL for the real autonomous fleet vision smoke")
	}
	requests := smokeEnvInt(t, "MYCELIUM_VISION_REQUESTS", 200)
	concurrency := smokeEnvInt(t, "MYCELIUM_VISION_CONCURRENCY", 32)
	minNodes := smokeEnvInt(t, "MYCELIUM_VISION_MIN_NODES", 2)
	maxWaitMS := smokeEnvInt(t, "MYCELIUM_VISION_MAX_WAIT_MS", 600000)
	maxTokens := smokeEnvInt(t, "MYCELIUM_VISION_MAX_TOKENS", 8)
	if requests <= 0 || concurrency <= 0 || minNodes <= 0 || maxWaitMS <= 0 || maxTokens <= 0 {
		t.Fatalf("vision smoke numeric env must be positive: requests=%d concurrency=%d min_nodes=%d max_wait_ms=%d max_tokens=%d", requests, concurrency, minNodes, maxWaitMS, maxTokens)
	}
	token := visionSmokeGatewayToken(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxWaitMS)*time.Millisecond+20*time.Minute)
	defer cancel()

	nodes, instances := runVisionChatBatch(t, ctx, gatewayURL, token, model, requests, concurrency, maxWaitMS, maxTokens)
	if len(nodes) < minNodes {
		t.Fatalf("vision smoke used %d distinct nodes, want >= %d: %v", len(nodes), minNodes, mapKeys(nodes))
	}
	if len(instances) < 2 {
		t.Fatalf("vision smoke used %d distinct instances, want >= 2: %v", len(instances), mapKeys(instances))
	}
	t.Logf("vision smoke served %d requests across nodes=%v instances=%v without operator commands", requests, mapKeys(nodes), mapKeys(instances))

	if os.Getenv("MYCELIUM_VISION_EXPECT_REAP") == "1" {
		waitVisionIdleReap(t, ctx, mapKeys(instances), gatewayURL)
	}
}

func runVisionChatBatch(t *testing.T, ctx context.Context, gatewayURL, token, model string, requests, concurrency, maxWaitMS, maxTokens int) (map[string]bool, map[string]bool) {
	t.Helper()
	jobs := make(chan int)
	var mu sync.Mutex
	nodes := map[string]bool{}
	instances := map[string]bool{}
	errs := make(chan error, requests)
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				nodeID, instanceID, err := visionChatOnce(ctx, gatewayURL, token, model, id, maxWaitMS, maxTokens)
				if err != nil {
					errs <- err
					continue
				}
				mu.Lock()
				nodes[nodeID] = true
				instances[instanceID] = true
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < requests; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(errs)
	var failures []string
	for err := range errs {
		failures = append(failures, err.Error())
		if len(failures) >= 10 {
			break
		}
	}
	if len(failures) > 0 {
		t.Fatalf("vision smoke request failures: %s", strings.Join(failures, "\n"))
	}
	return nodes, instances
}

func visionChatOnce(ctx context.Context, gatewayURL, token, model string, id, maxWaitMS, maxTokens int) (string, string, error) {
	body, err := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]string{{
			"role":    "user",
			"content": fmt.Sprintf("Reply with ok %d.", id),
		}},
		"max_tokens": maxTokens,
	})
	if err != nil {
		return "", "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(gateway.HeaderMaxWait, strconv.Itoa(maxWaitMS))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("request %d status=%s body=%s", id, resp.Status, data)
	}
	nodeID := resp.Header.Get(gateway.HeaderNode)
	instanceID := resp.Header.Get(gateway.HeaderInstance)
	if nodeID == "" || instanceID == "" {
		return "", "", fmt.Errorf("request %d missing placement headers node=%q instance=%q body=%s", id, nodeID, instanceID, data)
	}
	if !bytes.Contains(data, []byte(`"choices"`)) {
		return "", "", fmt.Errorf("request %d missing OpenAI choices body=%s", id, data)
	}
	return nodeID, instanceID, nil
}

func visionSmokeGatewayToken(t *testing.T) string {
	t.Helper()
	if envName := strings.TrimSpace(os.Getenv("MYCELIUM_VISION_GATEWAY_TOKEN_ENV")); envName != "" {
		value := strings.TrimSpace(os.Getenv(envName))
		if value == "" {
			t.Fatalf("MYCELIUM_VISION_GATEWAY_TOKEN_ENV names unset or empty env %s", envName)
		}
		return value
	}
	if value := strings.TrimSpace(os.Getenv("MYCELIUM_VISION_GATEWAY_TOKEN")); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("MYCELIUM_GATEWAY_TOKEN"))
}

func waitVisionIdleReap(t *testing.T, ctx context.Context, initialInstances []string, gatewayURL string) {
	t.Helper()
	idleTTL := smokeEnvInt(t, "MYCELIUM_VISION_IDLE_TTL_MS", 900000)
	snapshotURLs := visionSnapshotURLs(gatewayURL)
	rpcToken := strings.TrimSpace(os.Getenv("MYCELIUM_VISION_RPC_TOKEN"))
	deadline, cancel := context.WithTimeout(ctx, time.Duration(idleTTL)*time.Millisecond+5*time.Minute)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		remaining, err := visionRemainingInstances(deadline, snapshotURLs, rpcToken, initialInstances)
		if err != nil {
			t.Fatalf("vision reap snapshot: %v", err)
		}
		if len(remaining) == 0 {
			return
		}
		select {
		case <-deadline.Done():
			t.Fatalf("autoscale instances were not reaped after idle ttl: remaining=%v urls=%v", remaining, snapshotURLs)
		case <-ticker.C:
		}
	}
}

func visionSnapshotURLs(gatewayURL string) []string {
	raw := strings.TrimSpace(os.Getenv("MYCELIUM_VISION_SNAPSHOT_URLS"))
	if raw == "" {
		return []string{gatewayURL}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			out = append(out, strings.TrimRight(value, "/"))
		}
	}
	if len(out) == 0 {
		return []string{gatewayURL}
	}
	return out
}

func visionRemainingInstances(ctx context.Context, snapshotURLs []string, rpcToken string, instanceIDs []string) ([]string, error) {
	remaining := map[string]bool{}
	for _, id := range instanceIDs {
		remaining[id] = true
	}
	for _, baseURL := range snapshotURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/snapshot", nil)
		if err != nil {
			return nil, err
		}
		if rpcToken != "" {
			req.Header.Set("Authorization", "Bearer "+rpcToken)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s /snapshot status=%s body=%s", baseURL, resp.Status, data)
		}
		var snap domain.NodeSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return nil, fmt.Errorf("decode %s /snapshot: %w", baseURL, err)
		}
		for _, inst := range snap.Instances {
			delete(remaining, inst.ID)
		}
	}
	return mapKeys(remaining), nil
}

func smokeEnvInt(t *testing.T, key string, fallback int) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return value
}

func mapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}
