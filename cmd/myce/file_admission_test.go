package main

import (
	"bytes"
	"context"
	"mycelium/internal/myce"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestFileAdmissionCommand(t *testing.T) {
	root := t.TempDir()
	q, err := myce.Open(filepath.Join(root, "queue.db"), filepath.Join(root, "checkpoints"), myce.RealClock{}, myce.WithFileStore(filepath.Join(root, "files"), 185<<30, true), myce.WithFileStoreProductionReserve(16<<30))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	api := httptest.NewServer(myce.API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	for _, action := range []string{"observe", "status", "rollback"} {
		var output bytes.Buffer
		if err := fileAdmissionCommand(context.Background(), []string{action, "--queue", api.URL, "--token", "test"}, &output); err != nil {
			t.Fatal(action, err)
		}
		if !bytes.Contains(output.Bytes(), []byte(`"limit_bytes": 198642237440`)) || !bytes.Contains(output.Bytes(), []byte(`"production_reserve_bytes": 17179869184`)) {
			t.Fatal("CLI lost effective values", output.String())
		}
	}
	if err := fileAdmissionCommand(context.Background(), []string{"activate", "--queue", api.URL, "--token", "test"}, &bytes.Buffer{}); err == nil {
		t.Fatal("CLI activated without evidence")
	}
}
