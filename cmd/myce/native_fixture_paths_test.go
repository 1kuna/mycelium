package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// Observe actual work/checkpoint paths through the payload's existing logs.
// The most recent line names the current attempt after a crash/retry.
func commandFixturePath(t *testing.T, ctx context.Context, c myce.Client, id, kind string) string {
	t.Helper()
	var result string
	for _, line := range strings.Split(readLogs(t, ctx, c, id), "\n") {
		if path, ok := strings.CutPrefix(line, "myce fixture "+kind+": "); ok {
			if !filepath.IsAbs(path) {
				t.Fatalf("command reported a relative %s path: %q", kind, path)
			}
			result = path
		}
	}
	return result
}

// Read the live physical append file, including any other attempt's appends on
// this fixture worker. Nonresumable checkpoint dirs survive work recreation and
// service restart; neither input staging nor accepted output hashes reset them.
func commandFixtureAppendBytes(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	assigned := filepath.Dir(path)
	root := filepath.Dir(assigned)
	attempts, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, attempt := range attempts {
		dir := filepath.Join(root, attempt.Name())
		if !attempt.IsDir() || dir == assigned {
			continue
		}
		other, err := os.ReadFile(filepath.Join(dir, filepath.Base(path)))
		if os.IsNotExist(err) {
			continue // The intentionally crashed first attempt never appends output.
		}
		if err != nil {
			return nil, err
		}
		data = append(data, other...)
	}
	return data, nil
}
