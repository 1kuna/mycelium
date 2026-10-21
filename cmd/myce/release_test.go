package main

import (
	"runtime"
	"testing"

	"mycelium/internal/myce"
)

func TestReleaseVersionRefusesWrongArtifactProtocolOrTarget(t *testing.T) {
	for _, args := range [][]string{
		{"--expect-version", "wrong-version"},
		{"--expect-os", "wrong-os"},
		{"--expect-arch", "wrong-arch"},
		{"--expect-start-gate", "wrong-protocol"},
		{"--expect-attempt-protocol", "wrong-protocol"},
	} {
		if err := versionCommand(args); err == nil {
			t.Fatal("wrong artifact accepted", args)
		}
	}
	captureJoiningOutput(t, func() error {
		return versionCommand([]string{"--expect-version", myce.BuildVersion, "--expect-os", runtime.GOOS, "--expect-arch", runtime.GOARCH, "--expect-start-gate", myce.StartGateProtocol, "--expect-attempt-protocol", myce.AttemptProtocol, "--require-native-sensing"})
	})
}
