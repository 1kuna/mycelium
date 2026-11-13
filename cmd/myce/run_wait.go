package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"mycelium/internal/myce"
)

// Exit codes under --wait/--out match the Python client's run_entry:
// 0 success, the command's own code on command failure, otherwise 1 for a
// failed/cancelled job. Submission refusals keep 64, 65, 69, 74, 75, 78.
// 125 means the runner failed; 126 means the waiter detached and the job
// continues. This reverses #1341 F: signals never request cancellation.
const runInfraExit = 125
const runDetachedExit = 126

// detachRunJob performs no queue mutation, including after an uncertain submit.
func detachRunJob(id, project, key string) error {
	if id == "" {
		id = "ID unknown (submission reply interrupted)"
	}
	return waitError(runDetachedExit, fmt.Sprintf("detached; job %s continues; re-attach: rerun the same command with the same --key %q (project %q); cancel explicitly: myce cancel --project %q --key %q", id, key, project, project, key))
}

// redactRunError never repeats credentials from transport/remote diagnostic
// text, preserving the exit code.
func redactRunError(err error, token string) error {
	if err == nil || token == "" {
		return err
	}
	return &exitError{exitCode(err), errors.New(strings.ReplaceAll(err.Error(), token, "[redacted]"))}
}

// runOutputManifest carries inline --output names as the job's one stored
// input. A plain command returns declared outputs through the verified file
// store only when it uses stored files (Spec.Validate refuses plain outputs
// otherwise), so this small manifest is that transport's carrier; it also lists
// the expected files beside the command in its private working folder.
const runOutputManifest = ".myce-outputs"

// attachRunOutputs declares outputs on an inline spec and writes the manifest
// to a private temporary file for upload. The caller runs cleanup.
func attachRunOutputs(s *myce.Spec, outputs []string) (string, func(), error) {
	s.Outputs = map[string]string{}
	for _, name := range outputs {
		if !profileRelative(name) || name == runOutputManifest {
			return "", nil, waitError(64, "--output must be a clean relative path")
		}
		s.Outputs[name] = "results"
	}
	dir, err := os.MkdirTemp("", "myce-run-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	path := filepath.Join(dir, "outputs")
	names := slices.Sorted(maps.Keys(s.Outputs))
	if err = os.WriteFile(path, []byte(strings.Join(names, "\n")+"\n"), 0600); err != nil {
		cleanup()
		return "", nil, err
	}
	ref, err := myce.IdentifyFile(path)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	s.Files = myce.FileManifest{runOutputManifest: ref}
	return path, cleanup, nil
}

// runReceiptWait completes an adapter/profile submission in the foreground:
// the unchanged receipt path submits, then this waits, streams logs, fetches
// verified outputs and returns the command's exit code.
func runReceiptWait(ctx context.Context, submit func(io.Writer) error, parentPID int, outDir, config, joining, address, token string, out, errOut io.Writer) (resultErr error) {
	var receipt bytes.Buffer
	submitErr := submit(&receipt)
	var r profileReceipt
	parsed := json.Unmarshal(receipt.Bytes(), &r) == nil && r.Key != "" && r.Project != ""
	if token == "" {
		token = os.Getenv("MYCE_TOKEN")
	}
	if ctx.Err() == nil && submitErr != nil && !(exitCode(submitErr) == 74 && parsed && r.JobID != "") {
		// Keep stdout for command logs; the retained receipt explains recovery.
		_, _ = errOut.Write(receipt.Bytes())
		return submitErr
	}
	if ctx.Err() != nil && !parsed {
		return waitError(runDetachedExit, "detached before a receipt was written; submission may have been accepted; rerun the same command with the same key to recover its job ID; cancel explicitly with myce cancel --project PROJECT --key KEY")
	}
	if ctx.Err() == nil && (!parsed || r.JobID == "") {
		return waitError(runInfraExit, "submission returned no job receipt")
	}
	if ctx.Err() != nil {
		return detachRunJob(r.JobID, r.Project, r.Key)
	}
	c, err := runClient(config, joining, address, token)
	if err != nil {
		return waitError(runInfraExit, "job "+r.JobID+" accepted, but its queue authority is unavailable: "+err.Error())
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	defer func() { resultErr = redactRunError(resultErr, c.Token) }()
	if submitErr != nil {
		// Only the optional detached observer failed; this foreground wait replaces it.
		fmt.Fprintln(errOut, "run: "+submitErr.Error()+"; waiting in the foreground")
	}
	reused := ""
	if r.SubmissionReused {
		reused = " (reused)"
	}
	fmt.Fprintf(errOut, "%s submitted%s: project %s, key %s\n", r.JobID, reused, r.Project, r.Key)
	return followRunJob(ctx, c, &myce.Job{ID: r.JobID, State: r.Status, Spec: r.Spec}, r.Project, r.Key, parentPID, true, outDir, out, errOut)
}

// followRunJob waits for an accepted job, printing state changes to errOut and
// command logs to out, detaches on interrupt and settles its exit code.
func followRunJob(ctx context.Context, c myce.Client, j *myce.Job, project, key string, parentPID int, contract bool, outDir string, out, errOut io.Writer) error {
	id := j.ID
	observerCtx, stopObserver := waitParentContext(ctx, parentPID, time.Second, os.Getppid)
	defer stopObserver()
	var after int64
	lastStatus := ""
	j, err := c.WaitSubmitted(observerCtx, j, func(j *myce.Job) error {
		line := j.ID + " " + j.State + " " + myce.JobReason(j)
		if line != lastStatus {
			if _, e := fmt.Fprintln(errOut, strings.ReplaceAll(line, c.Token, "[redacted]")); e != nil {
				return e
			}
			lastStatus = line
		}
		return drainJobLogs(observerCtx, c, j.ID, &after, out)
	})
	if ctx.Err() != nil || observerCtx.Err() != nil {
		return detachRunJob(id, project, key)
	}
	if err != nil {
		if contract {
			return waitError(runInfraExit, err.Error())
		}
		return err
	}
	if j.State == "succeeded" {
		if outDir == "" {
			return nil
		}
		if len(j.OutputRefs) == 0 {
			fmt.Fprintf(errOut, "%s declared no outputs; nothing to fetch\n", j.ID)
			return nil
		}
		if err = fetchAcceptedResult(ctx, c, j.ID, outDir, errOut); err != nil {
			if ctx.Err() != nil {
				return detachRunJob(id, project, key)
			}
			return waitError(runInfraExit, err.Error())
		}
		fmt.Fprintf(errOut, "%s outputs verified: %d file(s) in %s\n", j.ID, len(j.OutputRefs), outDir)
		return nil
	}
	message := fmt.Sprintf("job %s %s: %s", j.ID, j.State, myce.JobReason(j))
	if cause := j.ExecutionCause; j.State == "failed" && cause != nil && cause.Class == "command_exit" && cause.ExitCode != nil && *cause.ExitCode > 0 && *cause.ExitCode <= 255 {
		return waitError(*cause.ExitCode, message)
	}
	return waitError(1, message)
}
