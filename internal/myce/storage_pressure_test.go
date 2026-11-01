package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Keep the existing unique-result pressure regression with legacy admission.
func TestStoragePressureProductionResultPreservesUniqueBytes(t *testing.T) {
	q, _ := fixture(t)
	q.deliveryRoot = q.root
	q.files.limit = 16
	client := deliveryClient(t, q)
	_, unique := retentionUpload(t, client, "preserve")
	s := asyncDeliverySpec()
	s.Class, s.Priority = "production", "production"
	s.Disk.OutputBytes = 8
	high := submit(t, q, s)
	m := machine("writer", "fake")
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != high.ID {
		t.Fatal("missing production")
	}
	assigned.Spec.Run.Workdir = t.TempDir()
	if err := os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("123456789"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := (Worker{Client: client}).finishFileResult(*assigned, result(assigned, true), t.TempDir())
	assertFilePolicyRefusal(t, err, "shortfall=1")
	events := alertsOf(q, "production_storage_capacity")
	if len(events) != 1 || events[0].JobID != high.ID || !strings.Contains(events[0].Reason, "deficit=1 bytes") {
		t.Fatalf("missing exact result pressure alert: %+v", events)
	}
	if job(t, q, high.ID).State != "running" {
		t.Fatal("capacity hold manufactured compute completion")
	}
	if err := q.files.verify(context.Background(), unique); err != nil {
		t.Fatal("unique holder deleted", err)
	}
	if err := q.Cancel(high.ID); err != nil {
		t.Fatal(err)
	}
	if _, exists := q.s.Health.Conditions["storage-write/job/"+high.ID]; exists {
		t.Fatal("cancelled writer retained a pressure condition")
	}
}
