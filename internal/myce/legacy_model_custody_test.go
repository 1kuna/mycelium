package myce

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLegacyModelCustodyRequiresExactTerminalRecord(t *testing.T) {
	spool := filepath.Join(t.TempDir(), "spool")
	dir := custodianDir(spool)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	id := ID()
	execution := HostEngineExecution{Machine: "worker", Session: "session", Attempt: "engine", Model: "test", Device: "0"}
	request := modelCustodianRequest{Protocol: custodianProtocol, ID: id, Execution: execution}
	request.Original.Name = execution.Model
	if err := saveWorkerJSON(dir, custodianName(id, "model-request"), request); err != nil {
		t.Fatal(err)
	}
	requirePending := func(want int) {
		t.Helper()
		pending, err := modelRequests(spool)
		if err != nil || len(pending) != want {
			t.Fatalf("pending legacy custody = %v, %v, want %d records", pending, err, want)
		}
	}
	requirePending(1)

	snapshot := modelCustodianSnapshot{
		custodianSnapshot: custodianSnapshot{Protocol: custodianProtocol, ID: id, Finished: true},
		Execution:         execution,
		Report:            ModelReport{Name: execution.Model, Device: execution.Device, Attempt: "other", State: "unloaded"},
	}
	if err := saveWorkerJSON(dir, custodianName(id, "snapshot"), snapshot); err != nil {
		t.Fatal(err)
	}
	requirePending(1)

	snapshot.Report.Attempt = execution.Attempt
	if err := saveWorkerJSON(dir, custodianName(id, "snapshot"), snapshot); err != nil {
		t.Fatal(err)
	}
	requirePending(0)
}
