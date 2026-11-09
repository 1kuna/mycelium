package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnrollmentDrainIntentSurvivesCrash(t *testing.T) {
	for _, afterPost := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-post", true: "after-post"}[afterPost], func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("hypha", "hypha", "fake", 8, 32)
			check(t, q, m, nil)
			c := ServiceConfig{Name: m.ID, HostID: m.HostID, State: t.TempDir()}
			api := (API{Queue: q, Token: "test"}).Handler()
			crash := errors.New("simulated process crash")
			transport := &enrollmentCrashTransport{base: http.DefaultTransport}
			server := httptest.NewServer(api)
			defer server.Close()
			transport.before = func(r *http.Request) {
				if r.Method != "POST" {
					return
				}
				receiptPath := filepath.Join(c.State, enrollmentResumeFile)
				st, statErr := os.Stat(receiptPath)
				if statErr != nil || st.Mode().Perm() != 0600 {
					t.Error("drain intent receipt is not private", statErr)
				}
				b, e := os.ReadFile(receiptPath)
				var receipt map[string]any
				_ = json.Unmarshal(b, &receipt)
				if e != nil || receipt["intent"] != true {
					t.Error("drain POST preceded durable intent receipt", e)
				}
				if !afterPost {
					panic(crash)
				}
			}
			transport.after = func(r *http.Request) {
				if afterPost && r.Method == "POST" {
					panic(crash)
				}
			}
			func() {
				defer func() {
					if recover() != crash {
						t.Error("crash seam not reached")
					}
				}()
				_, _ = PrepareEnrollmentUpgrade(context.Background(), c, Client{URL: server.URL, Token: "test", HTTP: &http.Client{Transport: transport}}, clock, time.Second, nil)
			}()
			client := Client{URL: server.URL, Token: "test"}
			receipt, err := PrepareEnrollmentUpgrade(context.Background(), c, client, clock, time.Second, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = FinishEnrollmentUpgrade(context.Background(), c, client, receipt); err != nil {
				t.Fatal(err)
			}
			status, err := q.HostMaintenanceStatus(m.HostID)
			if err != nil || status.Policy.Mode != "enabled" {
				t.Fatal("crash rerun abandoned owned drain", status.Policy.Mode, err)
			}
		})
	}
}

type enrollmentCrashTransport struct {
	base          http.RoundTripper
	before, after func(*http.Request)
}

func (tr *enrollmentCrashTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	tr.before(r)
	response, err := tr.base.RoundTrip(r)
	if err == nil {
		if r.Method == "POST" {
			defer response.Body.Close()
		}
		tr.after(r)
	}
	return response, err
}

func TestEnrollmentRecoveryRequiresRestoredVersion(t *testing.T) {
	m := readyEnrollmentMachine()
	m.Enrollment.Version = "v1.0.0"
	dir := t.TempDir()
	if err := saveEnrollmentReceipt(dir, m); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	budget := 0
	observations := 0
	options := EnrollmentWait{Clock: clock, Timeout: 36 * time.Second, Version: "v1.0.0",
		Wait: func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil },
		Observe: func(context.Context) (EnrollmentObservation, error) {
			observations++
			return EnrollmentObservation{Machine: m, Fresh: true, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol, Policy: HostMaintenance{Mode: "drained", CPUBudgetCores: &budget, MemoryBudget: &HostMemoryBudget{}}}, nil
		},
		Command: func(context.Context, string, ...string) ([]byte, error) {
			return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", os.Getpid())), nil
		}}
	result, err := WaitEnrollment(context.Background(), ServiceConfig{State: dir, Detection: Detection{OS: "linux"}}, Client{}, options)
	if err != nil || result.State != "drained" || observations < 31 {
		t.Fatal("restored release was not verified with full stability", result.State, observations, err)
	}
	options.Version = "v1.0.1"
	if _, err = WaitEnrollment(context.Background(), ServiceConfig{State: dir, Detection: Detection{OS: "linux"}}, Client{}, options); err == nil {
		t.Fatal("wrong rollback release passed readiness")
	}
}
