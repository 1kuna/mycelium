package myce

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	fileCapacityBacking    = "file_store_backing"
	fileCapacityProduction = "file_store_production_reserve"
	fileCapacityProject    = "file_store_project_reservation"
)

type fileCapacityRequestKey struct{}

// FileCapacityEvidence is a refusal-time comparison, never a later status sample.
// Only aggregate operands are retained: no project identities or entitlements.
// Nil measurements are unknown, whereas a measured zero is explicit.
type FileCapacityEvidence struct {
	Bound                    string    `json:"bound,omitempty"`
	StoreLimitBytes          *uint64   `json:"store_limit_bytes,omitempty"`
	StoredBytes              *uint64   `json:"stored_bytes,omitempty"`
	ObservedAt               time.Time `json:"observed_at"`
	JobID                    string    `json:"job_id,omitempty"`
	Attempt                  int       `json:"attempt"`
	RequestID                string    `json:"request_id"`
	Production               bool      `json:"production"`
	Identified               *bool     `json:"identified,omitempty"`
	NeedBytes                uint64    `json:"need_bytes"`
	GrowthBytes              uint64    `json:"growth_bytes"`
	AvailableBytes           uint64    `json:"available_bytes"`
	FreeBytes                uint64    `json:"free_bytes"`
	OutputCommitmentBytes    uint64    `json:"output_commitment_bytes"`
	UploadCommitmentBytes    *uint64   `json:"upload_commitment_bytes,omitempty"`
	StoreFreeBytes           *uint64   `json:"store_free_bytes,omitempty"`
	VolumeFreeBytes          *uint64   `json:"volume_free_bytes,omitempty"`
	DiskFreeBytes            *uint64   `json:"disk_free_bytes,omitempty"`
	DiskFloorBytes           *uint64   `json:"disk_floor_bytes,omitempty"`
	ProductionReserveBytes   uint64    `json:"production_reserve_bytes"`
	ProductionCommittedBytes uint64    `json:"production_committed_bytes"`
	ProductionWithheldBytes  uint64    `json:"production_withheld_bytes"`
	ProjectWithheldBytes     uint64    `json:"project_withheld_bytes"`
}

func capacityPredicate(p string) bool {
	return p == fileCapacityBacking || p == fileCapacityProduction || p == fileCapacityProject
}

func safeFileCapacity(c *RuntimeCause) *FileCapacityEvidence {
	if c == nil || !capacityPredicate(c.Predicate) || c.FileCapacity == nil {
		return nil
	}
	e := *c.FileCapacity
	e.JobID, e.RequestID = safeLogID(e.JobID), safeLogID(e.RequestID)
	if e.Bound != "store_allowance" && e.Bound != "physical_volume" {
		e.Bound = ""
	}
	e.Attempt = max(0, e.Attempt)
	// Deep-copy optional measurements so response/status copies cannot mutate history.
	for _, p := range []**uint64{&e.StoreLimitBytes, &e.StoredBytes, &e.UploadCommitmentBytes, &e.StoreFreeBytes, &e.VolumeFreeBytes, &e.DiskFreeBytes, &e.DiskFloorBytes} {
		if *p != nil {
			v := **p
			*p = &v
		}
	}
	if e.Identified != nil {
		v := *e.Identified
		e.Identified = &v
	}
	return &e
}

func (q *Queue) capacityCause(status FileStoreStatus, predicate string, need, growth, committed uint64, production bool) *RuntimeCause {
	e := &FileCapacityEvidence{ObservedAt: q.clock.Now(), RequestID: ID(), Production: production, NeedBytes: need, GrowthBytes: growth, AvailableBytes: status.FreeBytes, FreeBytes: status.FreeBytes, OutputCommitmentBytes: need - min(need, growth), ProductionReserveBytes: uint64(q.fileStoreProductionReserve), ProductionCommittedBytes: committed}
	if !production {
		e.ProductionWithheldBytes = e.ProductionReserveBytes - min(e.ProductionReserveBytes, committed)
	}
	if status.diskTotal > 0 {
		limit, used := status.LimitBytes, status.UsedBytes
		e.StoreLimitBytes, e.StoredBytes = &limit, &used
		e.Bound = "store_allowance"
		if status.volumeFree < status.allowanceFree {
			e.Bound = "physical_volume"
		}
		store, volume, disk, floor, uploads := status.allowanceFree, status.volumeFree, status.DiskFreeBytes, status.FloorBytes, status.uploadRemaining
		e.StoreFreeBytes, e.VolumeFreeBytes, e.DiskFreeBytes, e.DiskFloorBytes, e.UploadCommitmentBytes = &store, &volume, &disk, &floor, &uploads
	}
	return &RuntimeCause{Operation: "outputs", Class: "http_error", Predicate: predicate, FileCapacity: e}
}

func bindCapacityContext(ctx context.Context, err error) error {
	var h *HTTPError
	if !errors.As(err, &h) || h.Code != "file_store_capacity" || h.Cause == nil {
		return err
	}
	if e := h.Cause.FileCapacity; e != nil {
		if id, ok := ctx.Value(fileCapacityRequestKey{}).(string); ok {
			e.RequestID = id
		}
		identified := ctx.Value(fileStoreAttemptKey{}) != nil || ctx.Value(fileStoreSubmissionKey{}) != nil || ctx.Value(fileStoreOrdinaryKey{}) != nil
		if identified {
			e.Identified = &identified
		}
		if a, ok := ctx.Value(fileStoreAttemptKey{}).(fileStoreAttempt); ok {
			e.JobID, e.Attempt = safeLogID(a.ID), a.Attempt
		}
	}
	return err
}

func capacityText(c *RuntimeCause) string {
	e := safeFileCapacity(c)
	if e == nil {
		return ""
	}
	return fmt.Sprintf("; %s need=%d growth=%d available=%d free=%d output_commitments=%d production_reserve=%d production_committed=%d production_withheld=%d project_withheld=%d", c.Predicate, e.NeedBytes, e.GrowthBytes, e.AvailableBytes, e.FreeBytes, e.OutputCommitmentBytes, e.ProductionReserveBytes, e.ProductionCommittedBytes, e.ProductionWithheldBytes, e.ProjectWithheldBytes)
}
