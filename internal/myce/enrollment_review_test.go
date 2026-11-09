package myce

import (
	"context"
	"os"
	"testing"
)

func TestEnrollmentLaunchctlCapturedNestedState(t *testing.T) {
	b, err := os.ReadFile("testdata/launchctl-running.txt")
	if err != nil {
		t.Fatal(err)
	}
	health, err := EnrollmentServiceHealth(context.Background(), ServiceConfig{Detection: Detection{OS: "darwin"}}, func(context.Context, string, ...string) ([]byte, error) { return b, nil })
	if err != nil || health != "123/1" {
		t.Fatal("running top-level service rejected by nested state", health, err)
	}
}
