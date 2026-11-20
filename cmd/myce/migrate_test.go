package main

import (
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestMigrateWorkerUnitRequiresExplicitInstallation(t *testing.T) {
	for _, args := range [][]string{{"migrate"}, {"migrate", "worker-unit"}, {"migrate", "queue"}, {"migrate", "worker-unit", "extra"}} {
		probe := myce.MachineProbe{OS: "darwin"}
		if err := runWithProbe(args, probe); err == nil || !strings.Contains(err.Error(), "migrate") {
			t.Fatal(args, err)
		}
	}
}
