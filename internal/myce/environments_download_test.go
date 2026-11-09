package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEnvironmentStallWatchdogRetriesOnce(t *testing.T) {
	// Budget one installation, including directory blocks and ready.json.
	// APFS and Linux filesystems charge these tiny entries differently.
	footprint := t.TempDir()
	for _, dir := range []string{"bin", ".Trash"} {
		if err := os.Mkdir(filepath.Join(footprint, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(footprint, "partial-wheel-install"), make([]byte, 8<<10), 0600); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(footprint, "ready.json", EnvironmentReport{Key: strings.Repeat("a", 64), State: "ready"}); err != nil {
		t.Fatal(err)
	}
	limit, err := environmentDiskUsage(footprint)
	if err != nil {
		t.Fatal(err)
	}
	for _, recover := range []bool{true, false} {
		t.Run(fmt.Sprint("recover=", recover), func(t *testing.T) {
			clock := &workerFakeClock{now: time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)}
			c := &EnvironmentCache{Root: t.TempDir(), Limit: limit, Clock: clock}
			calls := 0
			c.Run = func(ctx context.Context, _ []string, dir string, _ []string, _, _ io.Writer) error {
				calls++
				if err := os.WriteFile(filepath.Join(dir, "partial-wheel-install"), make([]byte, 8<<10), 0600); err != nil {
					return err
				}
				if calls == 2 && recover {
					return nil
				}
				clock.Advance(5*time.Minute + time.Second)
				<-ctx.Done()
				return ctx.Err()
			}
			r := EnvironmentRecipe{Key: strings.Repeat("a", 64), Build: []string{"fake-stalled-installer"}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, release, err := c.Acquire(ctx, r)
			if release != nil {
				release()
			}
			if calls != 2 || (recover && err != nil) || (!recover && !errors.Is(err, errEnvironmentBuildStalled)) {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
			log, readErr := os.ReadFile(filepath.Join(c.Root, r.Key+".build.log"))
			if readErr != nil || !strings.Contains(string(log), "no log or download-cache growth for 5 minutes") {
				t.Fatal("missing stall diagnostic", readErr)
			}
			t.Logf("fake clock: %d attempts; recovered=%v; bounded failure=%v", calls, recover, errors.Is(err, errEnvironmentBuildStalled))
		})
	}
}
