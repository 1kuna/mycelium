package main

import (
	"bytes"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestCancellationScopeStatus(t *testing.T) {
	release := "pending"
	j := &myce.Job{Cancellation: &myce.Cancellation{WillNotRun: true, Release: &release}, CancellationScope: &myce.CancellationScope{
		Pieces:  []myce.CancellationPiece{{Key: "future", WillNotRun: true, Release: "not_applicable", Dependents: []string{"consumer"}}},
		Members: []myce.CancellationMember{{JobID: "consumer", State: "stopping", WillNotRun: true, Release: "pending"}},
	}}
	var out bytes.Buffer
	printCancellationScope(&out, j)
	for _, want := range []string{"cancellation will_not_run=true release=pending", "piece future materialized=false accepted=false", "release=not_applicable dependents=consumer", "member consumer state=stopping will_not_run=true release=pending"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q: %s", want, out.String())
		}
	}
}
