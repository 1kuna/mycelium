package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"mycelium/internal/myce"
)

func TestFreshNASResolutionBoundAndCancellation(t *testing.T) {
	// The process-global signal relay must be initialized outside fake time.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)
	for _, dry := range []bool{false, true} {
		for _, cancelled := range []bool{false, true} {
			name := "real"
			if dry {
				name = "dry"
			}
			if cancelled {
				name += "/cancel"
			} else {
				name += "/deadline"
			}
			t.Run(name, func(t *testing.T) {
				home := t.TempDir()
				synctest.Test(t, func(t *testing.T) {
					start := time.Now()
					var deadline time.Time
					p := myce.MachineProbe{OS: "linux", Arch: "arm64", Command: func(ctx context.Context, n string, args ...string) ([]byte, error) {
						if n != "cat" {
							return nil, exec.ErrNotFound
						}
						deadline, _ = ctx.Deadline()
						if cancelled {
							return nil, context.Canceled
						}
						<-ctx.Done()
						return nil, ctx.Err()
					}}
					args := []string{"install", "worker", "--gpu", "cpu"}
					if dry {
						args = append(args, "--dry-run")
					}
					err := setupCommandWithProbe(args, p, home)
					if cancelled {
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("cancellation lost: %v", err)
						}
					} else {
						var typed *myce.PreflightError
						if !errors.As(err, &typed) || typed.Code != "nas_unresponsive" {
							t.Errorf("deadline misclassified: %v", err)
						}
						if elapsed := time.Since(start); elapsed != 5*time.Second {
							t.Errorf("elapsed=%s want 5s", elapsed)
						}
					}
					if deadline.Sub(start) != 5*time.Second {
						t.Errorf("probe budget=%s want 5s", deadline.Sub(start))
					}
				})
				entries, err := os.ReadDir(home)
				if err != nil || len(entries) != 0 {
					t.Fatalf("refusal wrote files: entries=%v err=%v", entries, err)
				}
			})
		}
	}
}
