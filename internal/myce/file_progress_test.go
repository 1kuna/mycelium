package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileProgressHashCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var progress FileProgress
	ctx = WithFileProgress(ctx, func(p FileProgress) {
		progress = p
		if p.Bytes > 0 {
			cancel()
		}
	})
	_, err = IdentifyFileContext(ctx, path)
	if !errors.Is(err, context.Canceled) || progress.Phase != "hashing" || progress.Bytes <= 0 || progress.Bytes >= progress.Total || progress.Total != 1<<20 {
		t.Fatalf("hash cancellation lost progress: %+v, %v", progress, err)
	}
}
