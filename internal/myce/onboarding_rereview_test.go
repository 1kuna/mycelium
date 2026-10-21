package myce

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReleaseApprovalMonotonic(t *testing.T) {
	for _, version := range []string{"v1.0.0", "v1.0.1", "v1.0.1+rebuild"} {
		t.Run(version, func(t *testing.T) {
			q, _ := fixture(t)
			pub, key, _ := ed25519.GenerateKey(rand.Reader)
			_, _, m := signedTestRelease(t)
			sign := func() []byte {
				raw, _ := json.Marshal(m)
				b, _ := json.Marshal(SignedRelease{raw, ed25519.Sign(key, raw)})
				return b
			}
			if err := q.ConfigureEnrollmentRelease(sign(), pub, "https://publisher.invalid"); err != nil {
				t.Fatal(err)
			}
			m.Version = version
			if err := q.ConfigureEnrollmentRelease(sign(), pub, "https://publisher.invalid"); err == nil {
				t.Fatal("queue approved equal or older manifest without explicit override")
			}
			if q.s.EnrollmentRelease.Manifest.Version != "v1.0.1" {
				t.Fatal("rejected approval changed current manifest")
			}
			if err := q.ConfigureEnrollmentRelease(sign(), pub, "https://publisher.invalid", true); err != nil {
				t.Fatal("explicit re-approval refused", err)
			}
			if q.s.EnrollmentRelease.Manifest.Version != version {
				t.Fatal("explicit approval not applied")
			}
		})
	}
}

func TestReleaseDevelopmentReplacement(t *testing.T) {
	if err := CheckReleaseUpgrade("v1.0.1", "development", false); err == nil || !strings.Contains(err.Error(), "--allow-downgrade") {
		t.Errorf("missing explicit recovery guidance: %v", err)
	}
	if err := CheckReleaseUpgrade("v1.0.1", "development", true); err != nil {
		t.Errorf("explicit development replacement refused: %v", err)
	}
}

func TestEnrollmentProbeErrorPreserved(t *testing.T) {
	s, err := (Worker{StateDir: t.TempDir()}).openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	w := Worker{Machine: Machine{GPU: "xpu"}, Clock: &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}, DeviceSettings: []DeviceInstall{{Selector: "0", Detection: Detection{Platform: "xpu", MemoryMode: "fixed", MemoryGB: 16}}}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return nil, errors.New("fixture driver unavailable") }}
	if err = w.initInventory(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if w.inventoryReason != "device_probe_unavailable" {
		t.Fatalf("probe error overwritten: %s", w.inventoryReason)
	}
}

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

func TestReleaseBuildMetadataValid(t *testing.T) {
	_, _, m := signedTestRelease(t)
	m.Version = "v1.0.1+rebuild"
	if err := m.Validate(); err != nil {
		t.Fatal("semver build metadata rejected", err)
	}
}

func TestReleaseApprovalSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "queue.db")
	root := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	data, pub, _ := signedTestRelease(t)
	if err = q.ConfigureEnrollmentRelease(data, pub, "https://publisher.invalid"); err != nil {
		q.Close()
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err = q.ConfigureEnrollmentRelease(data, pub, "https://publisher.invalid"); err == nil {
		t.Fatal("restart forgot approval monotonicity")
	}
	if err = q.ConfigureEnrollmentRelease(data, pub, "https://publisher.invalid", true); err != nil {
		t.Fatal("explicit restart re-approval refused", err)
	}
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
