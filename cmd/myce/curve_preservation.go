package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"

	"mycelium/internal/myce"
)

func curvePreservationCommand(args []string, out io.Writer) error {
	if len(args) > 0 && args[0] == "capture" {
		return curvePreservationCaptureCommand(context.Background(), args[1:], out)
	}
	f := flag.NewFlagSet("release verify-curves", flag.ContinueOnError)
	beforePath := f.String("before", "", "before full status snapshot (server now is cutover boundary)")
	afterPath := f.String("after", "", "after full status snapshot")
	ledgerPath := f.String("ledgers", "", "JSON array of job ledger pages containing accepted learning")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *beforePath == "" || *afterPath == "" || *ledgerPath == "" {
		return errors.New("verify-curves requires --before, --after and --ledgers")
	}
	read := func(path string, value any) error {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if snapshot, ok := value.(*myce.Snapshot); ok {
			return decodeCurvePreservationSnapshot(data, snapshot)
		}
		return json.Unmarshal(data, value)
	}
	var before, after myce.Snapshot
	var pages []myce.JobLedgerPage
	if err := read(*beforePath, &before); err != nil {
		return err
	}
	if err := read(*afterPath, &after); err != nil {
		return err
	}
	if err := read(*ledgerPath, &pages); err != nil {
		return err
	}
	receipt, err := myce.VerifyCurvePreservation(before, after, pages)
	if encodeErr := json.NewEncoder(out).Encode(receipt); encodeErr != nil {
		return encodeErr
	}
	return err
}

// Capture both retained jobs and curves explicitly. Default status is a bounded
// observational view and cannot establish release preservation (#439).
func curvePreservationCaptureCommand(ctx context.Context, args []string, out io.Writer) error {
	f := flag.NewFlagSet("release verify-curves capture", flag.ContinueOnError)
	authority := installedClientFlags(f)
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("capture accepts installed client options only")
	}
	client, err := authority.client()
	if err != nil {
		return err
	}
	var raw json.RawMessage
	if err = client.Call(ctx, "GET", "/api/status?detail=full", nil, &raw); err != nil {
		return err
	}
	var snapshot myce.Snapshot
	if err = decodeCurvePreservationSnapshot(raw, &snapshot); err != nil {
		return err
	}
	// Preserve additional snapshot metadata, including the completeness marker.
	return json.NewEncoder(out).Encode(raw)
}

func decodeCurvePreservationSnapshot(data []byte, out *myce.Snapshot) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, field := range []string{"now", "jobs", "work_curves"} {
		if _, ok := fields[field]; !ok {
			return errors.New("release guard requires full status with jobs and work_curves; capture /api/status?detail=full")
		}
	}
	if err := json.Unmarshal(data, out); err != nil {
		return err
	}
	if raw, ok := fields["jobs_page"]; ok {
		var page struct {
			Mode       string `json:"mode"`
			Total      int    `json:"total"`
			Returned   int    `json:"returned"`
			NextBefore string `json:"next_before"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return err
		}
		if page.Mode != "all" || page.Total != len(out.Jobs) || page.Returned != len(out.Jobs) || page.NextBefore != "" {
			return errors.New("release guard refuses bounded or incomplete status; capture /api/status?detail=full")
		}
	}
	return nil
}
