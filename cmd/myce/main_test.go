package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunDelegatesToControlCLI(t *testing.T) {
	err := run(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "usage: myce") {
		t.Fatalf("run err = %v", err)
	}
	if err := run(nil, []string{"models", "list", "--db", filepath.Join(t.TempDir(), "control.db")}); err != nil {
		t.Fatalf("nil context run: %v", err)
	}
}

func TestSignalContextCancels(t *testing.T) {
	ctx, stop := signalContext(nil)
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatalf("signal context did not cancel")
	}
}

func TestRunMainUsesSignalAwareExecution(t *testing.T) {
	var stderr bytes.Buffer
	if code := runMain([]string{"bogus"}, &stderr); code != 1 || !strings.Contains(stderr.String(), "unknown myce command") {
		t.Fatalf("runMain code=%d stderr=%q", code, stderr.String())
	}
}

func TestMainExitCodeReportsErrorsAndSuccess(t *testing.T) {
	var stderr bytes.Buffer
	if code := mainExitCode(context.Background(), nil, &stderr); code != 1 || !strings.Contains(stderr.String(), "usage: myce") {
		t.Fatalf("error exit code=%d stderr=%q", code, stderr.String())
	}
	stderr.Reset()
	dbPath := filepath.Join(t.TempDir(), "control.db")
	if code := mainExitCode(context.Background(), []string{"models", "list", "--db", dbPath}, &stderr); code != 0 || stderr.Len() != 0 {
		t.Fatalf("success exit code=%d stderr=%q", code, stderr.String())
	}
}
