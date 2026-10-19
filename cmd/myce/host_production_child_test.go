package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestHostProductionChildEntryReadOnlyAndFailedClose(t *testing.T) {
	// Exercise main's startup guard and dispatch, not either package's TestMain.
	binary := filepath.Join(t.TempDir(), "myce")
	if out, err := exec.Command("go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	_, _, config := localIdentityFixture(t)
	account, err := myce.LookupWorkerAccount("")
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotHostFiles(t, filepath.Dir(config.Config))
	for _, operation := range []string{"status", "close"} {
		t.Run(operation, func(t *testing.T) {
			request := myce.HostMutation{Operation: operation, WorkerUser: account.Name, ConfigPath: config.Config, Timeout: 5 * time.Second}
			if operation == "close" {
				// A full CAS tuple reaches the real HostMutationExecutor, then
				// fails before intent/discovery. Provisioning already made its lock.
				request.CloseExpected = &myce.HostRevision{Revision: 99, Operation: "not-current"}
			}
			input, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, "_host-worker-mutation")
			cmd.Stdin = bytes.NewReader(input)
			cmd.Env = myce.WorkerChildEnvironment(account)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err = cmd.Run(); err != nil || stderr.Len() != 0 {
				t.Fatalf("child failed: %v, stderr bytes=%d", err, stderr.Len())
			}
			var response struct {
				Version   int                     `json:"version"`
				Operation string                  `json:"operation"`
				Result    myce.HostMutationResult `json:"result"`
				Code      string                  `json:"code"`
			}
			decoder := json.NewDecoder(&stdout)
			decoder.DisallowUnknownFields()
			if err = decoder.Decode(&response); err != nil {
				t.Fatal(err)
			}
			if response.Version != 1 || response.Operation != operation {
				t.Fatal("wrong protocol envelope")
			}
			if operation == "status" {
				e := response.Result.Envelope
				if response.Code != "" || response.Result.Revision != 1 || e == nil || e.Desired.State == nil || *e.Desired.State != "closed" || e.Desired.Revision == nil || *e.Desired.Revision != 1 || e.Accepted != nil {
					t.Fatal(response)
				}
			} else if response.Code != "revision" || response.Result.Revision != 0 || response.Result.CleanupConfirmed {
				t.Fatal(response)
			}
			if !reflect.DeepEqual(before, snapshotHostFiles(t, filepath.Dir(config.Config))) {
				t.Fatal("production child wrote fixture files")
			}
		})
	}
}
