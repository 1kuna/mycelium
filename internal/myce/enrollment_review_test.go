package myce

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnrollmentLaunchctlCapturedNestedState(t *testing.T) {
	b, err := os.ReadFile("testdata/launchctl-running.txt")
	if err != nil {
		t.Fatal(err)
	}
	health, err := EnrollmentServiceHealth(context.Background(), ServiceConfig{Detection: Detection{OS: "darwin"}}, func(context.Context, string, ...string) ([]byte, error) { return b, nil })
	if err != nil || health != "123/1" {
		t.Fatal("running top-level service rejected by nested state", health, err)
	}
}

func TestEnrollmentReceiptWrittenOnlyOnChange(t *testing.T) {
	dir := t.TempDir()
	m := readyEnrollmentMachine()
	m.Devices[0].CapabilityProfile = &CapabilityProfile{RecordedAt: RealClock{}.Now()}
	if err := saveEnrollmentReceipt(dir, m); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "enrollment.json")
	stamp := time.Unix(100, 0)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if err := saveEnrollmentReceipt(dir, m); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !st.ModTime().Equal(stamp) {
		t.Fatal("unchanged receipt rewritten")
	}
	m.Session = "new-session"
	if err := saveEnrollmentReceipt(dir, m); err != nil {
		t.Fatal(err)
	}
	r, err := ReadEnrollmentReceipt(dir)
	if err != nil || r.Session != m.Session {
		t.Fatal("changed receipt not saved", err)
	}
}

func TestEnrollmentReleaseVersionMustBeOrdered(t *testing.T) {
	_, _, m := signedTestRelease(t)
	m.Version = "opaque-label"
	if err := m.Validate(); err == nil {
		t.Fatal("unordered release version accepted")
	}
}

func TestEnrollmentConfiguredXPUAbsent(t *testing.T) {
	s, err := (Worker{StateDir: t.TempDir()}).openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	d := MachineDevice{ID: "0", Selector: "0", Identity: "xpu-first", Platform: "xpu", MemoryMode: "fixed", BudgetGB: 16, TotalMemoryGB: 16, FreeMemoryGB: 16}
	w := Worker{Machine: Machine{GPU: "xpu"}, Clock: RealClock{}, DeviceSettings: []DeviceInstall{{Selector: "0", Detection: Detection{Platform: "xpu", MemoryMode: "fixed", MemoryGB: 16}}, {Selector: "1", Detection: Detection{Platform: "xpu", MemoryMode: "fixed", MemoryGB: 16}}}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return []MachineDevice{d}, nil }}
	if err = w.initInventory(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if w.inventoryReason != "device_selector_unmatched" {
		t.Fatal("configured absent XPU was not reported", w.inventoryReason)
	}
}

func TestEnrollmentReceiptFailureDoesNotStopWorker(t *testing.T) {
	q, _ := fixture(t)
	dir := newTestStateRoot(t)
	// A directory in place of the receipt deterministically refuses atomic writes.
	if err := os.Mkdir(filepath.Join(dir, "enrollment.json"), 0700); err != nil {
		t.Fatal(err)
	}
	var checks atomic.Int32
	api := (API{Queue: q, Token: "test"}).Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/checkin" {
			checks.Add(1)
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	w := Worker{Enrollment: true, Machine: machine("receipt-test", "fake"), StateDir: dir, Clock: RealClock{}, Interval: 10 * time.Millisecond, Client: Client{URL: server.URL, Token: "test"}, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for checks.Load() < 3 {
		select {
		case err := <-done:
			t.Fatal("receipt failure aborted worker", err)
		case <-timer.C:
			t.Fatal("worker did not retry checkin", checks.Load())
		case <-ticker.C:
		}
	}
	// Make the filesystem writable again; the running worker must retry saving.
	if err := os.Remove(filepath.Join(dir, "enrollment.json")); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := ReadEnrollmentReceipt(dir); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatal("worker stopped before retry", err)
		case <-timer.C:
			t.Fatal("receipt never retried")
		case <-ticker.C:
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestEnrollmentSavedConfigRejectsMissingXPU(t *testing.T) {
	m := readyEnrollmentMachine()
	m.Devices[0].ID = "0"
	m.Devices[0].Selector = "0"
	m.Devices[0].Platform = "xpu"
	dir := t.TempDir()
	if err := saveEnrollmentReceipt(dir, m); err != nil {
		t.Fatal(err)
	}
	settings := []DeviceInstall{{Selector: "0", Detection: Detection{Platform: "xpu"}}, {Selector: "1", Detection: Detection{Platform: "xpu"}}}
	clock := &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	budget := 0
	// Deliberately circular reported inventory/receipt, with no worker reason:
	// the installer must independently require both configured selectors.
	c := ServiceConfig{State: dir, MachineDevices: settings, Detection: Detection{OS: "linux"}}
	result, err := WaitEnrollment(context.Background(), c, Client{}, EnrollmentWait{Clock: clock, Timeout: 35 * time.Second,
		Wait: func(context.Context, time.Duration) error { clock.Advance(time.Second); return nil },
		Observe: func(context.Context) (EnrollmentObservation, error) {
			return EnrollmentObservation{Machine: m, Fresh: true, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol, Policy: HostMaintenance{Mode: "enabled", CPUBudgetCores: &budget, MemoryBudget: &HostMemoryBudget{}}}, nil
		},
		Command: func(context.Context, string, ...string) ([]byte, error) {
			return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", os.Getpid())), nil
		},
	})
	if err == nil || result.State == "ready" {
		t.Fatal("two-XPU config accepted one visible card", err)
	}
}
