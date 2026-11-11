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

// runInfraExit is the --wait/--out exit code for every outcome that is not the
// command's own result: usage, submission refusal, queue or transport failure,
// a job lost or cancelled elsewhere, output download/verification failure and
// an unconfirmed interrupt. 125 follows the container-runner convention
// (docker/podman run) for "the runner failed, not the command".
const runInfraExit = 125

// runCancelBudget bounds interrupt cleanup. It matches the usual grace between
// SIGTERM and SIGKILL in container runtimes and CI, so the confirmation can be
// printed before a supervisor kills this process.
const runCancelBudget = 10 * time.Second

// runOwnExit marks the command's own exit code (or a confirmed interrupt) so
// the --wait contract passes it through instead of reporting runInfraExit.
type runOwnExit struct{ err *exitError }

func (e *runOwnExit) Error() string { return e.err.Error() }
func (e *runOwnExit) Unwrap() error { return e.err }

func ownExit(code int, message string) error {
	return &runOwnExit{&exitError{code, errors.New(message)}}
}

// runWaitContract maps every non-command failure to runInfraExit.
func runWaitContract(err error) error {
	var own *runOwnExit
	if err == nil || errors.As(err, &own) {
		return err
	}
	return &exitError{runInfraExit, err}
}

// redactRunError never repeats credentials from transport/remote diagnostic
// text, preserving the exit code and whether it is the command's own.
func redactRunError(err error, token string) error {
	if err == nil || token == "" {
		return err
	}
	e := &exitError{exitCode(err), errors.New(strings.ReplaceAll(err.Error(), token, "[redacted]"))}
	var own *runOwnExit
	if errors.As(err, &own) {
		return &runOwnExit{e}
	}
	return e
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
			return "", nil, errors.New("--output must be a clean relative path")
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
		return errors.New("run interrupted before a receipt was written; rerun the same command to recover or cancel it")
	}
	if ctx.Err() == nil && (!parsed || r.JobID == "") {
		return errors.New("submission returned no job receipt")
	}
	c, err := runClient(config, joining, address, token)
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	defer func() { resultErr = redactRunError(resultErr, c.Token) }()
	if ctx.Err() != nil {
		// Cancel the intent by key even when the job was not yet submitted.
		return cancelRunJob(c, r.Project, r.Key, true)
	}
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
// command logs to out, cancels it on interrupt and settles its exit code.
func followRunJob(ctx context.Context, c myce.Client, j *myce.Job, project, key string, parentPID int, contract bool, outDir string, out, errOut io.Writer) error {
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
	if ctx.Err() != nil {
		return cancelRunJob(c, project, key, contract)
	}
	if observerCtx.Err() != nil {
		return waitError(4, "run observer parent exited; job continues")
	}
	if err != nil {
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
			return err
		}
		fmt.Fprintf(errOut, "%s outputs verified: %d file(s) in %s\n", j.ID, len(j.OutputRefs), outDir)
		return nil
	}
	message := fmt.Sprintf("job %s %s: %s", j.ID, j.State, myce.JobReason(j))
	if cause := j.ExecutionCause; j.State == "failed" && cause != nil && cause.Class == "command_exit" && cause.ExitCode != nil && *cause.ExitCode > 0 && *cause.ExitCode <= 255 {
		return ownExit(*cause.ExitCode, message)
	}
	if contract {
		return waitError(runInfraExit, message)
	}
	return waitError(1, message)
}

// cancelRunJob requests durable cancellation by submission key after an
// interrupt. Under the --wait contract it exits 130 only when the queue confirms
// the job will not run, and polls the idempotent cancel until a running
// process's release is confirmed or the cleanup budget ends.
func cancelRunJob(c myce.Client, project, key string, contract bool) error {
	cleanup, stop := context.WithTimeout(context.Background(), runCancelBudget)
	defer stop()
	unconfirmed := "run interrupted; cancellation unconfirmed; cancel via /api/jobs/cancel with project " + project + " and submission_key " + key
	ack, err := c.CancelSubmission(cleanup, project, key)
	if !contract {
		if err != nil || !ack.WillNotRun {
			return waitError(130, unconfirmed)
		}
		return waitError(130, "run interrupted; cancellation acknowledged (running cleanup may still be pending)")
	}
	if err != nil || !ack.WillNotRun || ack.State == "unknown" {
		return waitError(runInfraExit, unconfirmed)
	}
	for ack.Release != nil && *ack.Release == "pending" {
		// A short poll keeps the confirmation inside the budget; each call is one
		// small idempotent control request.
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-cleanup.Done():
			timer.Stop()
			return ownExit(130, "run interrupted; cancellation confirmed (job will not run); process release still pending after "+runCancelBudget.String())
		case <-timer.C:
		}
		if next, e := c.CancelSubmission(cleanup, project, key); e == nil && next.WillNotRun {
			ack = next
		}
	}
	release := "not_applicable"
	if ack.Release != nil {
		release = *ack.Release
	}
	return ownExit(130, "run interrupted; cancellation confirmed (release "+release+")")
}
