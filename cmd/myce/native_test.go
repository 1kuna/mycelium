package main

import (
	"strings"
	"testing"
)

func TestResolveNativeCLIRequiresExactCustody(t *testing.T) {
	for _, args := range [][]string{nil, {"--state-dir", t.TempDir()}, {"--state-dir", t.TempDir(), "--attempt-id", "exact", "--timeout", "0s"}, {"--state-dir", t.TempDir(), "--attempt-id", "../escape"}} {
		if err := resolveNativeCommand(args); err == nil {
			t.Fatal("accepted missing or invalid custody", args)
		}
	}
	if err := run([]string{"resolve-native", "--confirm-gone"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatal("operator assertion bypass accepted", err)
	}
}
