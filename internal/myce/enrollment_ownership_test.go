package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// An operator's drain is a separate decision even at the same clock timestamp.
func TestEnrollmentRecoveryPreservesOperatorDrain(t *testing.T) {
	for _, boundary := range []string{"before-post", "after-post", "conflict", "completed-receipt"} {
		for _, advance := range []bool{false, true} {
			t.Run(boundary+map[bool]string{false: "/same-time", true: "/later"}[advance], func(t *testing.T) {
				q, clock := fixture(t)
				m := cpuMachine("hypha", "hypha", "fake", 8, 32)
				check(t, q, m, nil)
				c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
				server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
				defer server.Close()
				client := Client{URL: server.URL, Token: "test"}
				crash := errors.New("simulated process crash")
				transport := &enrollmentOwnershipTransport{base: http.DefaultTransport, before: func(*http.Request) {}, after: func(*http.Request) {}}
				var operator HostMaintenanceStatus
				operatorDrain := func() {
					if advance {
						clock.Advance(time.Second)
					}
					var err error
					operator, err = q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain"})
					if err != nil {
						t.Fatal(err)
					}
				}
				transport.before = func(r *http.Request) {
					if r.Method == "POST" {
						if boundary == "before-post" {
							panic(crash)
						}
						if boundary == "conflict" {
							operatorDrain()
						}
					}
				}
				transport.after = func(r *http.Request) {
					if r.Method == "POST" && boundary == "after-post" {
						panic(crash)
					}
				}
				var prepareErr error
				func() {
					defer func() {
						if v := recover(); v != nil && v != crash {
							panic(v)
						}
					}()
					_, prepareErr = PrepareEnrollmentUpgrade(context.Background(), c, Client{URL: client.URL, Token: client.Token, HTTP: &http.Client{Transport: transport}}, clock, time.Second, nil)
				}()
				if boundary == "conflict" {
					if prepareErr == nil || !strings.Contains(prepareErr.Error(), "409") {
						t.Fatalf("expected CAS conflict: %v", prepareErr)
					}
				} else {
					operatorDrain()
				}
				receipt, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
				if err == nil {
					err = FinishEnrollmentUpgrade(context.Background(), c, client, receipt)
				}
				if boundary == "conflict" {
					err = errors.Join(prepareErr, err)
				}
				if err == nil || !strings.Contains(err.Error(), "myce host resume hypha") {
					t.Errorf("missing preserved-drain explanation and resume command: %v", err)
				}
				status, e := q.HostMaintenanceStatus(m.HostID)
				if e != nil || status.Policy.Mode != "drained" || !status.Policy.UpdatedAt.Equal(operator.Policy.UpdatedAt) {
					t.Fatalf("operator drain changed: %+v; %v", status.Policy, e)
				}
			})
		}
	}
}

// Unlike a crash-only transport, ordinary responses must remain readable.
type enrollmentOwnershipTransport struct {
	base          http.RoundTripper
	before, after func(*http.Request)
}

func (tr *enrollmentOwnershipTransport) RoundTrip(r *http.Request) (response *http.Response, err error) {
	tr.before(r)
	response, err = tr.base.RoundTrip(r)
	if err == nil {
		defer func() {
			if v := recover(); v != nil {
				response.Body.Close()
				panic(v)
			}
		}()
		tr.after(r)
	}
	return
}

func TestEnrollmentResumeRejectsOperatorRace(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	check(t, q, m, nil)
	c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
	server := httptest.NewServer((API{Queue: q, Token: "test"}).Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	receipt, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &enrollmentOwnershipTransport{base: http.DefaultTransport, after: func(*http.Request) {}, before: func(r *http.Request) {
		if r.Method == "POST" {
			if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
				t.Fatal(err)
			}
		}
	}}
	client.HTTP = &http.Client{Transport: transport}
	err = FinishEnrollmentUpgrade(context.Background(), c, client, receipt)
	if err == nil || !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "myce host resume hypha") {
		t.Fatalf("operator drain not fenced atomically: %v", err)
	}
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.Mode != "drained" {
		t.Fatalf("operator drain resumed: %+v, %v", status.Policy, err)
	}
}

func TestEnrollmentLegacyOwnershipFailsClosed(t *testing.T) {
	for _, legacy := range []string{"queue", "receipt"} {
		t.Run(legacy, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("hypha", "hypha", "fake", 8, 32)
			check(t, q, m, nil)
			c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
			api := (API{Queue: q, Token: "test"}).Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if legacy == "queue" && r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/maintenance") {
					var request HostMaintenanceRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					request.DrainOwner = "" // Model a queue that does not store ownership.
					status, err := q.ChangeHostMaintenance(m.HostID, request)
					respond(w, status, err)
					return
				}
				api.ServeHTTP(w, r)
			}))
			defer server.Close()
			client := Client{URL: server.URL, Token: "test"}
			if legacy == "receipt" {
				status, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain"})
				if err != nil {
					t.Fatal(err)
				}
				if err = saveWorkerJSON(c.State, enrollmentResumeFile, enrollmentResumeReceipt{Host: m.HostID, Name: m.ID, UpdatedAt: status.Policy.UpdatedAt}); err != nil {
					t.Fatal(err)
				}
			}
			_, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
			if err == nil || !strings.Contains(err.Error(), "myce host resume hypha") {
				t.Fatalf("legacy ambiguity not reported: %v", err)
			}
			status, err := q.HostMaintenanceStatus(m.HostID)
			if err != nil || status.Policy.Mode != "drained" {
				t.Fatalf("unproven drain resumed: %+v, %v", status.Policy, err)
			}
		})
	}
}

func TestDrainOwnershipSurvivesQueueRestart(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(dir, "queue.db"), filepath.Join(dir, "checkpoints"), clock)
	if err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("hypha", "hypha", "fake", 8, 32)
	check(t, q, m, nil)
	drained, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain", DrainOwner: "intent-1"})
	if err != nil {
		q.Close()
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(filepath.Join(dir, "queue.db"), filepath.Join(dir, "checkpoints"), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.DrainOwner != "intent-1" {
		t.Fatalf("queue lost ownership proof: %+v %v", status.Policy, err)
	}
	if _, err = q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &drained.Policy.UpdatedAt, IfDrainOwner: "intent-1"}); err != nil {
		t.Fatal("owned resume after restart refused", err)
	}
	status, err = q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.Mode != "enabled" || status.Policy.DrainOwner != "" {
		t.Fatalf("resume retained drain ownership: %+v %v", status.Policy, err)
	}
}
