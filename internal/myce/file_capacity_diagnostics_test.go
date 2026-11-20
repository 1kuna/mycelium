package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Each actual admission arm crosses both HTTP decoders, the worker's durable
// result format, release-only replay and public job/status projection. Project
// input protection is intentionally tested as an input request: exact worker
// output transfers remain exempt from that policy.
func TestFileCapacityDiagnosticsEndToEnd(t *testing.T) {
	for _, tc := range []struct {
		name, predicate, bound             string
		production                         bool
		limit, reserve, project, available uint64
	}{
		{"allowance", fileCapacityBacking, "store_allowance", false, 3, 0, 0, 3},
		{"production_physical", fileCapacityBacking, "physical_volume", true, 100, 8, 0, 3},
		{"reserve", fileCapacityProduction, "store_allowance", false, 10, 8, 0, 2},
		{"project", fileCapacityProject, "store_allowance", false, 10, 2, 6, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			c := retentionClient(t, q)
			s := spec("experiment")
			s.Resumable = false
			j := submit(t, q, s)
			m := machine("worker", "fake")
			check(t, q, m, nil)
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != j.ID {
				t.Fatal("no assignment")
			}
			q.files.limit = int64(tc.limit)
			q.fileStoreProductionReserve = int64(tc.reserve)
			q.files.productionReserve = tc.reserve
			if tc.project > 0 {
				setQuotaPolicy(t, q, "private-policy-owner", ProjectPolicy{ReservedFileBytes: tc.project})
			}
			q.files.diskProbe = func(string) (DiskReading, error) {
				free := uint64(512 << 20)
				if tc.bound == "physical_volume" {
					free = (256 << 20) + 3
				}
				return DiskReading{TotalBytes: 1 << 30, FreeBytes: free}, nil
			}
			_, ref := policyInput(t, "four")
			class := "experiment"
			if tc.production {
				class = "production"
			}
			ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "test", Class: class})
			// Exact attempts supply the job fence except for exempt project-input writes.
			if tc.project == 0 {
				assigned.Spec.Priority = class
				assigned.Spec.Class = class
				if err := q.update(func() error { v := q.touchJob(j.ID); v.Spec.Class = class; v.Spec.Priority = class; return nil }); err != nil {
					t.Fatal(err)
				}
				ctx = withFileStoreAttempt(ctx, *assigned)
			}
			requestID := ID()
			c.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				r.Header.Set(logRequestIDHeader, requestID)
				return http.DefaultTransport.RoundTrip(r)
			})}
			var journal testLogBuffer
			previous := log.Writer()
			log.SetOutput(&journal)
			defer log.SetOutput(previous)
			err := c.PreflightFileStore(ctx, FileManifest{"private-submitted-filename": ref, "duplicate": ref})
			var h *HTTPError
			if !errors.As(err, &h) || h.Status != 507 || h.Code != "file_store_capacity" || h.Cause == nil {
				t.Fatalf("refusal: %v", err)
			}
			e := h.Cause.FileCapacity
			if h.Cause.Predicate != tc.predicate || e.NeedBytes != 4 || e.GrowthBytes != 4 || e.AvailableBytes != tc.available || e.Bound != tc.bound || e.Production != tc.production || e.Identified == nil || !*e.Identified || !e.ObservedAt.Equal(clock.Now()) || e.RequestID != requestID {
				t.Fatalf("snapshot: %+v cause=%+v", e, h.Cause)
			}
			if tc.project == 0 && (e.JobID != j.ID || e.Attempt != assigned.Attempt) {
				t.Fatalf("attempt fence: %+v", e)
			}
			if e.StoreFreeBytes == nil || *e.StoreFreeBytes != tc.limit || e.VolumeFreeBytes == nil || e.DiskFloorBytes == nil || *e.DiskFloorBytes != 256<<20 || e.UploadCommitmentBytes == nil || *e.UploadCommitmentBytes != 0 || e.ProductionReserveBytes != tc.reserve || e.ProjectWithheldBytes != tc.project {
				t.Fatalf("operands: %+v", e)
			}
			want := e.FreeBytes
			switch tc.predicate {
			case fileCapacityProduction:
				want -= min(want, e.ProductionWithheldBytes)
			case fileCapacityProject:
				want -= min(want, e.ProductionWithheldBytes)
				want -= min(want, e.ProjectWithheldBytes)
			}
			if want != e.AvailableBytes || e.NeedBytes <= want {
				t.Fatal("cannot reproduce refusal", e)
			}
			data, _ := json.Marshal(safeRuntimeCause(h.Cause))
			if !strings.Contains(journal.String(), "event=file_store_capacity cause="+string(data)) {
				t.Fatal("journal differs", journal.String())
			}
			for _, secret := range []string{"private-policy-owner", "private-submitted-filename"} {
				if strings.Contains(journal.String(), secret) {
					t.Fatal("journal leaked identity", journal.String())
				}
			}
			// Tus/file responses use a separate decoder from ordinary API calls.
			response := httptest.NewRecorder()
			respond(response, nil, h)
			transferErr := fileTransferError(response.Result(), "generic failure", clock.Now())
			var transfer *HTTPError
			if !errors.As(transferErr, &transfer) || !reflect.DeepEqual(transfer.Cause, h.Cause) {
				t.Fatal("file decoder lost cause", transferErr)
			}
			pending := finalizationResult(result(assigned, true), "output_upload", "", transferErr, false)
			dir := t.TempDir()
			if err := saveWorkerJSON(dir, "pending.result.json", pending); err != nil {
				t.Fatal(err)
			}
			saved, err := os.ReadFile(filepath.Join(dir, "pending.result.json"))
			if err != nil {
				t.Fatal(err)
			}
			var replay Result
			if err := decodeWorkerJSON(saved, &replay); err != nil {
				t.Fatal(err)
			}
			replay.ReleaseOnly = true
			if err := q.Complete(replay); err != nil {
				t.Fatal(err)
			}
			// Later capacity and observation time do not replace this historical snapshot.
			clock.Advance(time.Minute)
			q.files.limit = 1000
			q.files.diskProbe = func(string) (DiskReading, error) { return DiskReading{TotalBytes: 1 << 30, FreeBytes: 512 << 20}, nil }
			for repeat := 0; repeat < 2; repeat++ {
				got, err := c.GetJob(context.Background(), j.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.ExecutionCause == nil || !reflect.DeepEqual(got.ExecutionCause.FileCapacity, e) || !strings.Contains(got.Reason, tc.predicate) {
					t.Fatalf("job cause: %+v", got.ExecutionCause)
				}
				snap, err := q.publicSnapshot()
				if err != nil {
					t.Fatal(err)
				}
				if len(snap.Jobs) != 1 || !reflect.DeepEqual(snap.Jobs[0].ExecutionCause.FileCapacity, e) {
					t.Fatal("status lost snapshot")
				}
				if repeat == 0 {
					if err := q.Complete(replay); err != nil {
						t.Fatal(err)
					}
				}
			}
			// A successful publication supersedes the pending refusal through the normal
			// completion path; neither the worker nor queue reconstructs an old error.
			q.fileStoreProductionReserve = 0
			setQuotaPolicy(t, q, "private-policy-owner", ProjectPolicy{})
			if err := c.PreflightFileStore(ctx, FileManifest{"output": ref}); err != nil {
				t.Fatal("retry", err)
			}
			if err := q.Complete(result(assigned, true)); err != nil {
				t.Fatal(err)
			}
			got, err := c.GetJob(context.Background(), j.ID)
			if err != nil || got.State != "succeeded" || got.ExecutionCause != nil || strings.Contains(got.Reason, tc.predicate) {
				t.Fatalf("pending cause survived success: %+v %v", got, err)
			}
		})
	}
}

func TestFileCapacityDiagnosticsCommitments(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	q.files.limit = 20
	q.deliveryRoot = q.root
	q.fileStoreProductionReserve = 10
	ref := capacityPartial(t, c, "123456", jobInputUploadPurpose)
	// Two output owners share one missing identity; it consumes one commitment.
	_, output := policyInput(t, "four")
	for i := 0; i < 2; i++ {
		s := asyncDeliverySpec()
		s.Disk.OutputBytes = 4
		s.Priority = "production"
		s.Class = "production"
		j := submit(t, q, s)
		if err := q.update(func() error { v := q.touchJob(j.ID); v.PendingOutputRefs = FileManifest{"output": output}; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	_, next := policyInput(t, "123456789")
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "test", Class: "experiment"})
	err := c.PreflightFileStore(ctx, FileManifest{"a": next, "b": next})
	var h *HTTPError
	if !errors.As(err, &h) || h.Cause == nil {
		t.Fatal(err)
	}
	e := h.Cause.FileCapacity
	if h.Cause.Predicate != fileCapacityProduction || e.GrowthBytes != 9 || e.NeedBytes != 13 || e.OutputCommitmentBytes != 4 || e.ProductionCommittedBytes != 4 || e.ProductionWithheldBytes != 6 || e.UploadCommitmentBytes == nil || *e.UploadCommitmentBytes != 4 || e.StoreFreeBytes == nil || *e.StoreFreeBytes != 14 || ref.SizeBytes != 6 {
		t.Fatalf("commitment accounting: %+v", e)
	}
}

func TestFileCapacityDiagnosticsUnknownAndPrivacy(t *testing.T) {
	q, _ := fixture(t)
	cause := q.capacityCause(FileStoreStatus{FreeBytes: 1}, fileCapacityBacking, 2, 2, 0, false)
	cause.FileCapacity.JobID = "private/path"
	cause.FileCapacity.RequestID = "token value"
	cause.FileCapacity.Bound = "private filesystem"
	got := safeRuntimeCause(cause)
	if got.FileCapacity.StoreFreeBytes != nil || got.FileCapacity.DiskFreeBytes != nil || got.FileCapacity.JobID != "" || got.FileCapacity.RequestID != "" || got.FileCapacity.Bound != "" {
		t.Fatalf("unsafe/unknown snapshot: %+v", got)
	}
	cause.Predicate = "arbitrary transport text"
	if safeRuntimeCause(cause).FileCapacity != nil {
		t.Fatal("untyped evidence accepted")
	}
	// Previous clients decode the unchanged code/error envelope, ignoring additions.
	cause.Predicate = fileCapacityBacking
	w := httptest.NewRecorder()
	respond(w, nil, &HTTPError{Status: 507, Code: "file_store_capacity", Message: "capacity refused", Cause: cause})
	var old struct{ Code, Error string }
	if err := json.NewDecoder(w.Result().Body).Decode(&old); err != nil || old.Code != "file_store_capacity" {
		t.Fatal("old client envelope", old, err)
	}
	// New clients reading an old queue retain unknown cause rather than guessing.
	err := fileTransferError(&http.Response{StatusCode: 507, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"code":"file_store_capacity","error":"capacity refused"}`))}, "upload failed")
	var h *HTTPError
	if !errors.As(err, &h) || h.Cause != nil {
		t.Fatal("invented old-queue diagnostic", err)
	}
}

func TestFileCapacityDiagnosticsReclamation(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	retained, _, ref := releaseTerminal(t, q, c, "retained")
	q.files.limit = 10
	_, next := policyInput(t, "four")
	err := c.PreflightFileStore(context.Background(), FileManifest{"input": next})
	var h *HTTPError
	if !errors.As(err, &h) || h.Cause == nil || h.Cause.Predicate != fileCapacityBacking || h.Cause.FileCapacity.FreeBytes != 2 {
		t.Fatal("original shortage", err)
	}
	original, _ := json.Marshal(h.Cause)
	clock.Advance(filePublicationGrace + time.Second)
	if _, err := releaseApply(q, releasePlan(t, q, releaseSelection(retained))); err != nil {
		t.Fatal(err)
	}
	if err := c.PreflightFileStore(context.Background(), FileManifest{"input": next}); err != nil {
		t.Fatal("reclamation did not unblock retry", err)
	}
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("retained bytes not reclaimed", err)
	}
	after, _ := json.Marshal(h.Cause)
	if string(original) != string(after) || h.Cause.FileCapacity.FreeBytes != 2 {
		t.Fatal("reclamation overwrote original refusal")
	}
}

// Real folder finalization used to discard the HTTP error in outputLimitError.
// Keep its fail-once verdict and still preserve the original capacity decision.
func TestFileCapacityDiagnosticsFolderFinalization(t *testing.T) {
	for _, tc := range []struct {
		name, class, predicate string
		limit, reserve         int64
	}{
		{"backing", "experiment", fileCapacityBacking, 10, 0},
		{"production_backing", "production", fileCapacityBacking, 10, 8},
		{"reserve", "experiment", fileCapacityProduction, 20, 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			c := retentionClient(t, q)
			_, input := retentionUpload(t, c, "fixture")
			s := spec(tc.class)
			s.Resumable = false
			s.Files = FileManifest{"input": input}
			s.Outputs = map[string]string{"folder/": "results"}
			submitted := submit(t, q, s)
			m := machine("worker", "fake")
			check(t, q, m, nil)
			j := check(t, q, m, nil).Job
			if j == nil || j.ID != submitted.ID {
				t.Fatal("no assignment")
			}
			j.Spec.Run.Workdir = t.TempDir()
			if err := os.Mkdir(filepath.Join(j.Spec.Run.Workdir, "folder"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(j.Spec.Run.Workdir, "folder", "out.txt"), []byte("four"), 0600); err != nil {
				t.Fatal(err)
			}
			q.files.limit = tc.limit
			q.fileStoreProductionReserve = tc.reserve
			finished, err := (Worker{Client: c}).finishFileResult(*j, result(j, true), t.TempDir())
			if err != nil || finished.Success || finished.ExecutionCause == nil || finished.ExecutionCause.Predicate != tc.predicate || finished.ExecutionCause.FileCapacity == nil || finished.ExecutionCause.FileCapacity.JobID != j.ID {
				t.Fatalf("worker cause: %+v %v", finished, err)
			}
			if err := q.Complete(finished); err != nil {
				t.Fatal(err)
			}
			got, err := c.GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatal(err)
			}
			assertFinalization(t, got, "output_preflight", "limit_exceeded", "")
			if got.ExecutionCause.FileCapacity == nil || got.ExecutionCause.Predicate != tc.predicate || got.ExecutionCause.Finalization.HTTPStatus != 507 {
				t.Fatalf("folder cause lost: %+v", got.ExecutionCause)
			}
		})
	}
}
