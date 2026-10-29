package myce

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestFileCheckpointRequestTiming(t *testing.T) {
	q, _ := fixture(t)
	ref, err := IdentifyFile("file_checkpoint.go")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("file_checkpoint.go")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
		t.Fatal(err)
	}
	s := &requestSpan{started: time.Now()}
	ctx := context.WithValue(context.Background(), requestSpanKey{}, s)
	err = q.publishCheckpoint(ctx, checkpointPublication{Checkpoint: FileCheckpoint{Generation: 0, Files: FileManifest{"checkpoint": ref}}})
	if !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	if !s.measured || s.hold <= 0 {
		t.Fatal("checkpoint transaction was not attributed")
	}
}
