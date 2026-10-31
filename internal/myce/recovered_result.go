package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"
)

// RecoveryRequest fences the failed execution and names already uploaded bytes.
// RequestedBy and Source are operator assertions, not authentication evidence.
type RecoveryRequest struct {
	Execution
	Machine     string         `json:"machine"`
	Session     string         `json:"session"`
	Key         string         `json:"key"`
	RequestedBy string         `json:"requested_by"`
	Source      RecoverySource `json:"source"`
	Reason      string         `json:"reason"`
	Files       FileManifest   `json:"files"`
}

type RecoverySource struct {
	Kind     string `json:"kind"`     // upload or nas; bytes always use verified uploads.
	Location string `json:"location"` // A relative source label, never a host absolute path.
}

// RecoveryPlan exposes declared output names, without worker paths or commands.
type RecoveryPlan struct {
	Execution
	Machine string   `json:"machine"`
	Session string   `json:"session"`
	Outputs []string `json:"outputs"`
}

// RecoveredResult is immutable recovery-time evidence, never execution success.
type RecoveredResult struct {
	RecoveryRequest
	ResultID       string    `json:"result_id"`
	Kind           string    `json:"kind"`
	RecoveredBy    string    `json:"recovered_by"`
	RecoveredAt    time.Time `json:"recovered_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	ManifestSHA256 string    `json:"manifest_sha256"`
}

func recoveryError(status int, code, message string) error {
	return &HTTPError{Status: status, Code: code, Message: message}
}

func recoveryText(s string) bool {
	return strings.TrimSpace(s) != "" && !strings.ContainsFunc(s, unicode.IsControl)
}

// Caller holds q.mu. A new attempt or terminal-state change invalidates recovery.
func recoveryFence(j *Job, p RecoveryRequest) error {
	if j == nil || j.State != "failed" || j.Attempt != p.Attempt || j.Machine != p.Machine || j.Session != p.Session || p.Attempt <= 0 || p.Machine == "" || p.Session == "" {
		return recoveryError(409, "recovery_stale", "recovery requires the exact retained failed execution")
	}
	for name := range p.Files {
		if _, ok := j.Spec.Outputs[name]; !ok {
			return recoveryError(400, "recovery_invalid", "recovery file must name a declared output")
		}
	}
	return nil
}

func recoveryReplay(j *Job, p RecoveryRequest) (*RecoveredResult, error) {
	if j == nil {
		return nil, nil
	}
	for _, r := range j.RecoveredResults {
		if r.Key == p.Key {
			if !reflect.DeepEqual(r.RecoveryRequest, p) {
				return nil, recoveryError(409, "recovery_conflict", "recovery key already identifies different evidence; use a new key to append another record")
			}
			copy := r
			copy.Files = maps.Clone(r.Files)
			return &copy, nil
		}
	}
	return nil, nil
}

// recoverResult is called only behind API operator authentication. Share the
// collector's publication fence, hash outside q.mu, then recheck inside commit.
func (q *Queue) recoverResult(ctx context.Context, p RecoveryRequest) (*RecoveredResult, error) {
	if !recoveryText(p.Key) || !recoveryText(p.RequestedBy) || !recoveryText(p.Reason) || !recoveryText(p.Source.Location) || (p.Source.Kind != "upload" && p.Source.Kind != "nas") || !safeRelative(p.Source.Location) || len(p.Files) == 0 {
		return nil, recoveryError(400, "recovery_invalid", "recovery requires key, requester, reason, upload/nas relative source label and named files")
	}
	if err := p.Files.validate(); err != nil {
		return nil, recoveryError(400, "recovery_invalid", err.Error())
	}
	p.Files = maps.Clone(p.Files)
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	unlock := requestReadLock(ctx, &q.mu)
	j := q.s.Jobs[p.ID]
	replay, err := recoveryReplay(j, p)
	if err == nil && replay == nil {
		err = recoveryFence(j, p)
	}
	unlock()
	if replay != nil || err != nil {
		return replay, err
	}
	if err = q.verifyManifest(ctx, p.Files); err != nil {
		return nil, recoveryError(400, "recovery_verification_failed", "recovery bytes are missing or do not match SHA-256 and size")
	}
	manifest, _ := json.Marshal(p.Files) // encoding/json sorts manifest keys.
	digest := sha256.Sum256(manifest)
	idBytes, _ := json.Marshal([]string{p.ID, p.Key})
	id := sha256.Sum256(idBytes)
	var out *RecoveredResult
	err = q.updateRequest(ctx, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		j := q.touchJob(p.ID)
		var err error
		out, err = recoveryReplay(j, p)
		if err != nil || out != nil {
			return err
		}
		if err = recoveryFence(j, p); err != nil {
			return err
		}
		now := q.clock.Now()
		r := RecoveredResult{RecoveryRequest: p, ResultID: hex.EncodeToString(id[:]), Kind: "recovered", RecoveredBy: "operator", RecoveredAt: now, ExpiresAt: now.Add(q.fileRetention), ManifestSHA256: hex.EncodeToString(digest[:])}
		j.RecoveredResults = append(j.RecoveredResults, r)
		r.Files = maps.Clone(r.Files)
		out = &r
		return nil
	}, nil)
	return out, err
}

func (a API) registerRecoveredResults(mux *requestMux) {
	mux.HandleFunc("GET /api/jobs/{id}/recovery-plan", func(w http.ResponseWriter, r *http.Request) {
		unlock := requestReadLock(r.Context(), &a.Queue.mu)
		j := a.Queue.s.Jobs[r.PathValue("id")]
		var plan RecoveryPlan
		var err error
		if j == nil {
			err = recoveryError(404, "job_unknown", "job not found")
		} else {
			plan = RecoveryPlan{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session}
			for name := range j.Spec.Outputs {
				plan.Outputs = append(plan.Outputs, name)
			}
			err = recoveryFence(j, RecoveryRequest{Execution: plan.Execution, Machine: plan.Machine, Session: plan.Session})
		}
		unlock()
		slices.Sort(plan.Outputs)
		respondOperation(w, plan, err, "recovery_invalid")
	})
	mux.HandleFunc("POST /api/jobs/{id}/recovered-results", func(w http.ResponseWriter, r *http.Request) {
		var p RecoveryRequest
		if !decode(w, r, &p) {
			return
		}
		if p.ID != r.PathValue("id") {
			respond(w, nil, recoveryError(400, "recovery_invalid", "URL and execution job ID must match"))
			return
		}
		// Do not persist the authenticating secret in operator-supplied metadata.
		metadata, _ := json.Marshal(p)
		if a.Token != "" && strings.Contains(string(metadata), a.Token) {
			respond(w, nil, recoveryError(400, "recovery_invalid", "recovery metadata must not contain credentials"))
			return
		}
		out, err := a.Queue.recoverResult(r.Context(), p)
		respondOperation(w, out, err, "recovery_invalid")
	})
	mux.HandleFunc("GET /api/jobs/{id}/recovered-results/{result}", func(w http.ResponseWriter, r *http.Request) {
		j, err := a.Queue.jobSnapshot(r.PathValue("id"), r.Context())
		if err != nil {
			respondOperation(w, nil, err, "status_unavailable")
			return
		}
		for _, result := range j.RecoveredResults {
			if result.ResultID == r.PathValue("result") {
				if r.URL.Query().Get("require_files") == "true" && !a.Queue.clock.Now().Before(result.ExpiresAt) {
					respond(w, nil, resultExpired())
					return
				}
				respond(w, result, nil) // Metadata survives byte expiry.
				return
			}
		}
		respond(w, nil, recoveryError(404, "recovery_unknown", "recovered result not found"))
	})
}

func (c Client) RecoverResult(ctx context.Context, p RecoveryRequest) (*RecoveredResult, error) {
	if err := p.Files.validate(); err != nil {
		return nil, err
	}
	var total int64
	for _, ref := range p.Files {
		total += ref.SizeBytes
	} // validate checks sum overflow.
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{}
	}
	local := *hc
	local.Timeout = c.fileTimeout(total) // Hashing is bulk I/O, not a ten-second control call.
	c.HTTP = &local
	var out RecoveredResult
	err := c.Call(ctx, "POST", "/api/jobs/"+url.PathEscape(p.ID)+"/recovered-results", p, &out)
	return &out, err
}

func (c Client) GetRecoveredResult(ctx context.Context, jobID, resultID string) (*RecoveredResult, error) {
	var out RecoveredResult
	err := c.Call(ctx, "GET", "/api/jobs/"+url.PathEscape(jobID)+"/recovered-results/"+url.PathEscape(resultID), nil, &out)
	return &out, err
}

// DownloadRecoveredResult uses the ordinary atomic, size/hash-verified download.
func (c Client) DownloadRecoveredResult(ctx context.Context, jobID, resultID, name, dest string) error {
	var r RecoveredResult
	if err := c.Call(ctx, "GET", "/api/jobs/"+url.PathEscape(jobID)+"/recovered-results/"+url.PathEscape(resultID)+"?require_files=true", nil, &r); err != nil {
		return err
	}
	ref, ok := r.Files[name]
	if !ok {
		return recoveryError(404, "artifact_unknown", "recovered artifact not found")
	}
	if r.Kind != "recovered" {
		return errors.New("expected recovered result evidence")
	}
	return c.downloadResultFile(ctx, ref, dest)
}
