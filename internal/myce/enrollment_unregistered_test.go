package myce

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFirstEnrollmentRepairCASPreservesConcurrentOperatorChange(t *testing.T) {
	q, clock := fixture(t)
	api := (API{Queue: q, Token: "test"}).Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			if _, err := q.ChangeHostMaintenance("new-host", HostMaintenanceRequest{Action: "disable"}); err != nil {
				t.Error(err)
			}
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	c := ServiceConfig{Enrollment: true, Name: "new-host", HostID: "new-host", State: t.TempDir()}
	_, err := PrepareEnrollmentUpgrade(context.Background(), c, Client{URL: server.URL, Token: "test"}, clock, time.Second, nil)
	if err == nil {
		t.Fatal("concurrent operator disable lost its CAS fence")
	}
	s, err := q.HostMaintenanceStatus(c.HostID)
	if err != nil || s.Policy.Mode != "disabled" {
		t.Fatal("operator policy changed", err)
	}
}

func TestFirstEnrollmentZeroTimestampIntentRecoversOnlyOwnedDrain(t *testing.T) {
	q, _ := fixture(t)
	c := ServiceConfig{Enrollment: true, Name: "new-host", HostID: "new-host", State: t.TempDir()}
	if err := saveWorkerJSON(c.State, enrollmentResumeFile, enrollmentResumeReceipt{Intent: true, Host: c.HostID, Name: c.Name, DrainOwner: "test-owner"}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.ChangeHostMaintenance(c.HostID, HostMaintenanceRequest{Action: "drain", DrainOwner: "test-owner"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	r, err := ResolveEnrollmentUpgrade(context.Background(), c, Client{URL: server.URL, Token: "test"})
	if err != nil || r.Policy.DrainOwner != "test-owner" || r.Policy.UpdatedAt.IsZero() {
		t.Fatal("owned first drain intent not recovered", err)
	}
}
