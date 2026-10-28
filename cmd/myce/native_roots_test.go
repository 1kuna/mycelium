package main

import (
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestJoinNativeRootFlags(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "test-only-token")
	for _, tc := range []struct {
		name  string
		flags []string
		want  string
	}{
		{"minimal-root", []string{"--native-minimal-root"}, "heartbeat must be positive"},
		{"minimal-root-disabled", []string{"--native-minimal-root=false"}, "heartbeat must be positive"},
		{"repeated", []string{"--native-read-root", "/fixture/models,version-2", "--native-read-root", "/fixture/other models", "--native-exec-root", "/fixture/runtime,version-2", "--native-exec-root", "/fixture/other runtime"}, "heartbeat must be positive"},
		{"clear", []string{"--native-read-root=/fixture/models", "--native-read-root=", "--native-exec-root=/fixture/runtime", "--native-exec-root="}, "heartbeat must be positive"},
		{"relative", []string{"--native-read-root=relative"}, "native roots must be absolute"},
		{"glob", []string{"--native-exec-root=/fixture/*"}, "without globs or tilde"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"join", "--gpu=fake", "--heartbeat=0"}, tc.flags...)
			// Stop before worker.Run, queue contact or any payload launch.
			err := runWithProbe(args, myce.MachineProbe{OS: "linux", Arch: "amd64"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("join flags: %v; want %q", err, tc.want)
			}
		})
	}
}
