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

func TestResolveCustodianStartupCLIRequiresExactCustody(t *testing.T) {
	for _, args := range [][]string{nil, {"--state-dir", t.TempDir()}, {"--state-dir", t.TempDir(), "--custodian-id", "../escape"}, {"--state-dir", t.TempDir(), "--custodian-id", "exact", "--timeout", "0s"}} {
		if err := resolveCustodianStartupCommand(args); err == nil {
			t.Fatal("accepted invalid recovery", args)
		}
	}
	if err := run([]string{"resolve-custodian-startup", "--confirm-gone"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatal("operator assertion accepted", err)
	}
}

func TestCustodianHandleInspectorCLIIsReadOnlyAndExact(t *testing.T) {
	for _, args := range [][]string{nil, {"--state-dir", "/unused", "--custodian-id", "../escape", "--owner-uid", "1"}, {"--state-dir", "/unused", "--custodian-id", "0123456789abcdef0123456789abcdef", "--owner-uid", "0"}, {"--confirm-gone"}} {
		if err := inspectCustodianHandlesCommand(args); err == nil {
			t.Fatal("invalid internal inspector accepted", args)
		}
	}
}
