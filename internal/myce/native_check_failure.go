package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// NativeCheckFailure identifies an accepted automatic qualification failure.
// The optional diagnostic fields are bounded identifiers from its own attempt.
type NativeCheckFailure struct {
	JobID         string `json:"job_id"`
	Attempt       int    `json:"attempt"`
	Code          string `json:"code,omitempty"`
	ExceptionType string `json:"exception_type,omitempty"`
	MissingModule string `json:"missing_module,omitempty"`
}

var (
	nativeCheckCode   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	nativeCheckSymbol = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
)

// Read at most the last four log records of the exact attempt. The normal worker
// replays logs before the completion. Diagnostics are optional and never affect
// result acceptance; use the separate SQLite reader outside q.mu.
func (q *Queue) readNativeCheckFailure(ctx context.Context, r Result) NativeCheckFailure {
	failure := NativeCheckFailure{JobID: r.ID, Attempt: r.Attempt}
	rows, err := q.usageReader.QueryContext(ctx, `SELECT substr(CAST(text AS BLOB), -?), length(CAST(text AS BLOB))
		FROM (SELECT seq, attempt, text FROM logs WHERE job=? ORDER BY seq DESC LIMIT 4)
		WHERE attempt=? ORDER BY seq DESC`, 3*logReplayRecordSize, r.ID, r.Attempt)
	if err != nil {
		return failure
	}
	defer rows.Close()
	var chunks []string
	truncated := false
	for rows.Next() {
		var chunk string
		var size int
		if err := rows.Scan(&chunk, &size); err != nil {
			return failure
		}
		chunks = append(chunks, chunk)
		if len(chunk) < size {
			truncated = true
			break // Older records cannot complete the first line of this chunk.
		}
	}
	if rows.Err() != nil {
		return failure
	}
	slices.Reverse(chunks)
	tail := strings.Join(chunks, "")
	const limit = 64 << 10
	if len(tail) > limit || truncated || len(chunks) == 4 {
		tail = tail[max(0, len(tail)-limit):]
		_, tail, _ = strings.Cut(tail, "\n") // The first line may start before the tail.
	}
	// Use the last valid record. Never copy the raw line or other fields.
	for line := range strings.SplitSeq(tail, "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 || len(line) > 512 || line[0] != '{' {
			continue
		}
		var fields struct {
			Code          string `json:"code"`
			ExceptionType string `json:"exception_type"`
			MissingModule string `json:"missing_module"`
		}
		if json.Unmarshal([]byte(line), &fields) != nil || !nativeCheckCode.MatchString(fields.Code) {
			continue
		}
		failure.Code = fields.Code
		failure.ExceptionType, failure.MissingModule = "", ""
		if len(fields.ExceptionType) <= 128 && nativeCheckSymbol.MatchString(fields.ExceptionType) {
			failure.ExceptionType = fields.ExceptionType
		}
		if len(fields.MissingModule) <= 128 && nativeCheckSymbol.MatchString(fields.MissingModule) {
			failure.MissingModule = fields.MissingModule
		}
	}
	return failure
}

func nativeCheckFailureText(failures []NativeCheckFailure) (evidence, action string) {
	var checks, commands []string
	for _, f := range failures {
		check := fmt.Sprintf("%s (attempt %d", f.JobID, f.Attempt)
		if f.Code != "" {
			check += ", " + f.Code
		}
		if f.ExceptionType != "" {
			check += ", " + f.ExceptionType
		}
		if f.MissingModule != "" {
			check += ", missing module " + f.MissingModule
		}
		checks = append(checks, check+")")
		commands = append(commands, "`myce logs "+f.JobID+"`")
	}
	return strings.Join(checks, "; "), "read " + strings.Join(commands, " or ") + ", then fix the implementation or check, register a new version, and resubmit with a new key"
}
