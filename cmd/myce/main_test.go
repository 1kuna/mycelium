package main

import (
	"context"
	"strings"
	"testing"
)

func TestRunDelegatesToControlCLI(t *testing.T) {
	err := run(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "usage: myce") {
		t.Fatalf("run err = %v", err)
	}
}
