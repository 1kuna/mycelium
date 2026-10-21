package myce

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEnrollmentUnprovenDrainNamesOwnerAndResume(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	check(t, q, m, nil)
	owner := "delayed-onboarding-intent"
	status, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain", DrainOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
	if err := saveWorkerJSON(c.State, enrollmentResumeFile, enrollmentResumeReceipt{Host: c.HostID, Name: c.Name, UpdatedAt: clock.Now(), DrainOwner: "different-intent"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	_, err = PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
	if err == nil || !strings.Contains(err.Error(), owner) || !strings.Contains(err.Error(), "myce host resume hypha") {
		t.Fatalf("unproven drain must name its owner and exact resume command: %v", err)
	}
	after, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || !reflect.DeepEqual(after.Policy, status.Policy) {
		t.Fatalf("unproven drain changed: %+v, %v", after.Policy, err)
	}
}

func TestEnrollmentLostReceiptSummaryNamesDrainOwner(t *testing.T) {
	c := ServiceConfig{HostID: "hypha"}
	r := EnrollmentResult{State: "drained", Machine: Machine{CapabilityProfile: &CapabilityProfile{}}}
	// Model the accepted delayed POST after its local intent was retired.
	r.Maintenance = HostMaintenance{Mode: "drained", DrainOwner: "delayed-onboarding-intent"}
	text := r.Summary(c)
	if !strings.Contains(text, r.Maintenance.DrainOwner) || !strings.Contains(text, "myce host resume hypha") {
		t.Fatalf("lost-receipt rerun omitted owner or exact recovery command: %s", text)
	}
}
