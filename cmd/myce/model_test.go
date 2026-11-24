package main

import (
	"errors"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestRetiredModelCommands(t *testing.T) {
	for _, tc := range []struct {
		command, code, replacement string
	}{
		{"fetch", "model_fetch_retired", "upstream client"},
		{"approve-local", "model_local_source_retired", "configured model root"},
		{"sync", "model_sync_retired", "on demand"},
		{"pin", "model_pin_retired", "idle_timeout"},
		{"unpin", "model_pin_retired", "idle_timeout"},
	} {
		t.Run(tc.command, func(t *testing.T) {
			err := run([]string{"model", tc.command})
			var refusal *myce.HTTPError
			if !errors.As(err, &refusal) || refusal.Code != tc.code || !strings.Contains(refusal.Message, tc.replacement) {
				t.Fatalf("model %s refusal = %v", tc.command, err)
			}
		})
	}
}
