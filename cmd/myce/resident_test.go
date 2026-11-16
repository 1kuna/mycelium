package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestResidentCommandRequiresAttempt(t *testing.T) {
	t.Setenv("MYCE_RESIDENT_PIPE", "")
	t.Setenv("MYCE_RESIDENT_KEY", "")
	var output bytes.Buffer
	if err := residentCommand([]string{"--path", "/infer"}, strings.NewReader("input"), &output); err == nil || !strings.Contains(err.Error(), "running stage") || output.Len() != 0 {
		t.Fatal("unbound helper did not fail locally", err)
	}
	for _, path := range []string{"https://elsewhere/infer", "//elsewhere/infer", "not-a-path"} {
		if err := residentCommand([]string{"--path", path}, strings.NewReader("input"), &output); err == nil || !strings.Contains(err.Error(), "local engine endpoint") {
			t.Fatal("helper accepted an arbitrary destination", path, err)
		}
	}
}
