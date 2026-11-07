package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestDevCommandWaitsForWorkers(t *testing.T) {
	binary := devTestBinary(t)
	source, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "command-started")
	entered, release := make(chan struct{}, 2), make(chan struct{})
	probes := syntheticWorkerProbes()
	hostMemory := probes.HostMemory
	probes.HostMemory = func(reserve float64) (myce.MemorySource, error) {
		entered <- struct{}{}
		<-release // Hold both workers before they can advertise any targets.
		return hostMemory(reserve)
	}
	myce.SetWorkerProbeDefaults(probes)
	defer installSyntheticHost()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- devCommandSession(ctx, []string{"sh", "-ec", `cd "$1"; printf started > "$2"; exec "$3" spec check --resolve .mycelium/profiles/quickstart.yaml`, "fixture", source, marker, binary}, nil, io.Discard, io.Discard)
	}()
	joined := false
	defer func() {
		if !joined {
			cancel()
			select {
			case <-release:
			default:
				close(release)
			}
			<-done
		}
	}()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("workers did not reach initial inventory", ctx.Err())
		}
	}
	// Give the old immediate-launch path a chance to run while both workers
	// remain blocked. This is a test observation window, not a readiness sleep.
	if err := waitPause(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	_, premature := os.Stat(marker)
	close(release)
	err = <-done
	joined = true
	if premature == nil {
		t.Error("user command started before either dev worker checked in")
	} else if !os.IsNotExist(premature) {
		t.Error(premature)
	}
	if err != nil {
		t.Fatal("first target-required operation failed", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("user command never started", err)
	}
}
