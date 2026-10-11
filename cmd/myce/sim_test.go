package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSimCommandNoTokenAndJSON(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "")
	path := filepath.Join(t.TempDir(), "result.json")
	var out bytes.Buffer
	if e := simCommand([]string{"--fleet", "../../examples/sim/fleet.yaml", "--workload", "../../examples/sim/workload.yaml", "--days", "0.01", "--what-if", "split-batch", "--json", path}, &out); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var result map[string]json.RawMessage
	if e = json.Unmarshal(data, &result); e != nil {
		t.Fatal(e)
	}
	if result["baseline"] == nil || result["split"] == nil || out.Len() == 0 {
		t.Fatal("missing comparison output")
	}
}
func TestSimCommandRejectsInputs(t *testing.T) {
	for _, args := range [][]string{{"--what-if", "unknown"}, {"extra"}, {"--days", "0", "--fleet", "../../examples/sim/fleet.yaml", "--workload", "../../examples/sim/workload.yaml"}} {
		if simCommand(args, &bytes.Buffer{}) == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
