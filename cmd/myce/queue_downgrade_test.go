package main

import (
	"io"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestQueueDowngradeArguments(t *testing.T) {
	for _, args := range [][]string{{}, {"--db", "unused.db", "--to", "v1"}, {"--db", "unused.db", "extra"}} {
		if err := queueDowngradeCommand(args, io.Discard); err == nil || !strings.Contains(err.Error(), "usage:") {
			t.Fatal("accepted unsafe downgrade arguments", args, err)
		}
	}
	if err := runWithProbe([]string{"queue", "downgrade"}, myce.MachineProbe{}); err == nil || !strings.Contains(err.Error(), "queue downgrade") {
		t.Fatal("queue subcommand not routed", err)
	}
}
