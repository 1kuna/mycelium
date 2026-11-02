package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestTopLevelUsage(t *testing.T) {
	if os.Getenv("MYCE_USAGE_CHILD") == "1" {
		os.Args = append([]string{"myce"}, os.Args[3:]...)
		main()
		os.Exit(0)
	}
	home := t.TempDir()
	runCLI := func(arg string) (string, int) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestTopLevelUsage$", "--", arg)
		cmd.Env = []string{"HOME=" + home, "PATH=" + os.Getenv("PATH"), "MYCE_USAGE_CHILD=1"}
		out, err := cmd.CombinedOutput()
		if err == nil {
			return string(out), 0
		}
		if e, ok := err.(*exec.ExitError); ok {
			return string(out), e.ExitCode()
		}
		t.Fatal(err)
		return "", -1
	}
	version, code := runCLI("version")
	if code != 0 || version == "" {
		t.Fatalf("version: exit %d: %s", code, version)
	}
	for _, arg := range []string{"--version", "-v"} {
		out, code := runCLI(arg)
		if code != 0 || out != version {
			t.Errorf("%s: exit %d, output %q; want %q", arg, code, out, version)
		}
	}
	for _, tc := range []struct {
		arg  string
		code int
	}{
		{"--help", 0}, {"-h", 0}, {"help", 0}, {"--bogus", 2}, {"bogus", 2},
	} {
		out, code := runCLI(tc.arg)
		if code != tc.code || !strings.Contains(out, "usage: myce ") || strings.Contains(out, "MYCE_TOKEN") {
			t.Errorf("%s: exit %d, output %q", tc.arg, code, out)
		}
	}
}
