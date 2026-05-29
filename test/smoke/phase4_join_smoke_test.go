//go:build smoke

package smoke

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestPhase4JoinedNodeGatewaySmoke(t *testing.T) {
	gatewayURL := os.Getenv("MYCELIUM_JOIN_GATEWAY")
	model := os.Getenv("MYCELIUM_JOIN_MODEL")
	if gatewayURL == "" || model == "" {
		t.Skip("set MYCELIUM_JOIN_GATEWAY and MYCELIUM_JOIN_MODEL after starting a joined node")
	}
	nodesResp, err := http.Get(strings.TrimRight(gatewayURL, "/") + "/nodes")
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	defer nodesResp.Body.Close()
	var nodes []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(nodesResp.Body).Decode(&nodes); err != nil {
		t.Fatalf("decode nodes: %v", err)
	}
	ready := false
	for _, node := range nodes {
		if node.Status == "ready" {
			ready = true
		}
	}
	if !ready {
		t.Fatalf("no ready joined node: %+v", nodes)
	}

	body := []byte(`{"model":` + quote(model) + `,"messages":[{"role":"user","content":"Say hi."}],"max_tokens":1}`)
	resp, err := http.Post(strings.TrimRight(gatewayURL, "/")+"/v1/chat/completions", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gateway post: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read gateway response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gateway status = %s body=%s", resp.Status, data)
	}
	if resp.Header.Get("X-Myc-Node") == "" || !strings.Contains(string(data), `"choices"`) {
		t.Fatalf("gateway response headers=%+v body=%s", resp.Header, data)
	}
}
