package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Daemon resources can outlive a killed test process. Record their endpoint
// before creation and label them so gate custody can positively inventory them.
func gateDockerArgs(t testing.TB, endpoint string, args []string) []string {
	t.Helper()
	marker := os.Getenv("GATE_RUN_ID")
	if marker == "" || len(args) == 0 {
		return args
	}
	index := 0
	if args[0] == "create" || args[0] == "run" {
		index = 1
	} else if len(args) > 1 && (args[0] == "network" || args[0] == "volume") && args[1] == "create" {
		index = 2
	}
	if index == 0 {
		return args
	}
	directory := os.Getenv("GATE_RUNTIME_DIR")
	if directory == "" {
		t.Fatal("gate Docker runtime journal is unavailable")
	}
	row, err := json.Marshal(map[string]string{"marker": marker, "endpoint": endpoint})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := os.OpenFile(filepath.Join(directory, "docker-used.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := journal.Write(append(row, '\n'))
	closeErr := journal.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal("gate Docker runtime journal was not published", writeErr, closeErr)
	}
	result := append([]string(nil), args[:index]...)
	result = append(result, "--label", "myce.gate="+marker)
	return append(result, args[index:]...)
}
