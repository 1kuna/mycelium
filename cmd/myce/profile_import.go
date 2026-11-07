package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

// Only protocol classifications enter the receipt. Paths, URLs and underlying
// transport messages can contain credentials and are deliberately excluded.
type profileImportFailure struct {
	Code           string  `json:"code"`
	Phase          string  `json:"phase"`
	Input          string  `json:"input"`
	Cause          string  `json:"cause"`
	ProgressBytes  int64   `json:"progress_bytes"`
	SizeBytes      int64   `json:"size_bytes"`
	ProgressKind   string  `json:"progress_kind"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	NextStep       string  `json:"next_step"`
}

type profileImportProgress struct {
	phase, input string
	bytes, total int64
	started      time.Time
}

func (p *profileImportProgress) begin(phase, input string, bytes, total int64) {
	p.phase, p.input, p.bytes, p.total, p.started = phase, input, bytes, total, time.Now()
}

func (p *profileImportProgress) report(v myce.FileProgress) {
	if p.phase != v.Phase {
		p.begin(v.Phase, p.input, v.Bytes, v.Total)
	}
	p.bytes, p.total = v.Bytes, v.Total
}

func (p *profileImportProgress) failure(out io.Writer, r *profileReceipt, dir string, code int, cause error, token string) error {
	input := p.input
	if token != "" {
		input = strings.ReplaceAll(input, token, "[redacted]")
	}
	kind := "uploaded"
	if p.phase == "hashing" {
		kind = "hashed"
	} else if p.phase == "upload" {
		kind = "acknowledged"
	} else if p.phase == "freezing" {
		kind = "read_for_copy"
	}
	next := "for shared data use shared_inputs.NAME.nas with verified sha256 and size_bytes, relative to each worker's --checkpoint-root, to bypass queue upload and commit hashing"
	if r.Key == "" {
		next = "local preparation stopped before saving intent; " + next
	} else if p.phase == "arming" {
		next = "job retained; check notifier configuration and retry the same command and saved key; " + next
	} else {
		next = "retain the saved key and receipt for recovery; reconcile that key before replacing an uncertain submission; " + next
	}
	reason := myce.ClientErrorCause(cause)
	// Capacity refusals already expose actionable required/free-byte details.
	// Keep that established diagnostic alongside the classified import cause.
	capacity := ""
	var refusal *myce.HTTPError
	if errors.As(cause, &refusal) && (refusal.Code == "file_store_capacity" || refusal.Code == "project_file_store_quota") {
		capacity = "; " + refusal.Code + ": " + refusal.Message
		if token != "" {
			capacity = strings.ReplaceAll(capacity, token, "[redacted]")
		}
	}
	var exited *exec.ExitError
	if p.phase == "arming" && errors.As(cause, &exited) {
		reason = "notifier_check_failed"
	} else if p.phase == "arming" && errors.Is(cause, exec.ErrNotFound) {
		reason = "notifier_not_found"
	}
	f := &profileImportFailure{Code: "input_import_failed", Phase: p.phase, Input: input,
		Cause: reason, ProgressBytes: p.bytes, SizeBytes: p.total,
		ProgressKind: kind, ElapsedSeconds: time.Since(p.started).Seconds(),
		NextStep: next}
	r.ImportFailure = f
	if r.Key != "" && dir != "" {
		// A registration error can race an observer which already acquired
		// custody. Never overwrite that observer's newer terminal receipt.
		persist := true
		if p.phase == "arming" {
			lock, err := profileLock(filepath.Join(dir, "watch.lock"))
			if err != nil {
				if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
					return err
				}
				persist = false
			} else {
				defer lock.Close()
			}
		}
		if persist {
			if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), r); err != nil {
				return err
			}
		}
	}
	return profileFailure(out, r, code, fmt.Sprintf("%s: phase=%s input=%q cause=%s progress=%d/%d bytes (%s) elapsed=%.3fs%s; %s", f.Code, f.Phase, f.Input, f.Cause, f.ProgressBytes, f.SizeBytes, f.ProgressKind, f.ElapsedSeconds, capacity, f.NextStep))
}

func profileImportContext(ctx context.Context, p *profileImportProgress) context.Context {
	return myce.WithFileProgress(ctx, p.report)
}
