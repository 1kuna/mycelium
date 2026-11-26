package myce

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

// DRM fdinfo as the Xe driver printed it for a running attempt (#2185): two
// clients on the attempt's card and one on its sibling, which never counts.
func xeAttemptFD(client, pci string, vramKiB, gttKiB int) string {
	return fmt.Sprintf("drm-driver:\txe\ndrm-client-id:\t%s\ndrm-pdev:\t%s\ndrm-total-system:\t0\ndrm-resident-system:\t0\n"+
		"drm-total-gtt:\t%[3]d KiB\ndrm-resident-gtt:\t%[3]d KiB\ndrm-total-vram0:\t%[4]d KiB\ndrm-shared-vram0:\t0\ndrm-active-vram0:\t0\n"+
		"drm-resident-vram0:\t%[4]d KiB\ndrm-resident-stolen:\t0\ndrm-cycles-ccs:\t776543\ndrm-total-cycles-ccs:\t28228839389342\n", client, pci, gttKiB, vramKiB)
}

// The worker hands each attempt to a detached custodian as a saved request.
// The custodian, not the worker, samples the card, so the request round trip
// is the boundary under test. Returns that attempt's measured peak.
func custodianXPUAttemptPeak(t *testing.T) *AttemptMemoryPeak {
	t.Helper()
	m := Machine{ID: "xpu-host", GPU: "xpu", Devices: []MachineDevice{
		{ID: "0", Platform: "xpu", Identity: "card-a", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 30.3, BudgetGB: 30.05, FreeMemoryGB: 30.05, pciAddress: "0000:03:00.0"},
		{ID: "1", Platform: "xpu", Identity: "card-b", Selector: "1", MemoryMode: "fixed", TotalMemoryGB: 30.3, BudgetGB: 30.05, FreeMemoryGB: 30.05, pciAddress: "0000:07:00.0"},
	}}
	dir := t.TempDir()
	if err := saveWorkerJSON(dir, custodianName("c", "request"), custodianRequest{Protocol: custodianProtocol, ID: "c", Machine: m}); err != nil {
		t.Fatal(err)
	}
	var req custodianRequest
	if err := readCustodianJSON(dir, custodianName("c", "request"), &req); err != nil {
		t.Fatal(err)
	}
	// runCustodian adopts req.Machine; forAttempt binds the assigned card.
	w := Worker{Machine: req.Machine}
	i := slices.IndexFunc(w.Machine.Devices, func(d MachineDevice) bool { return d.ID == "1" })
	d := w.Machine.Devices[i]
	w.selectedDevice, w.Machine = &d, d.machine(w.Machine)

	a, err := fakeCeilings(t).create("attempt-xpu-card", 24<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	proc := t.TempDir()
	for name, body := range map[string]string{
		"7/fdinfo/3": xeAttemptFD("1071305", "0000:07:00.0", 2312168, 82088),
		"7/fdinfo/5": xeAttemptFD("1071272", "0000:07:00.0", 798352, 83176),
		"7/fdinfo/6": xeAttemptFD("1071297", "0000:03:00.0", 65628, 40),
		"7/fdinfo/0": "pos:\t0\nflags:\t02\nmnt_id:\t25\nino:\t4\n",
	} {
		path := filepath.Join(proc, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(a.path, "cgroup.procs"), []byte("7\n"), 0600); err != nil {
		t.Fatal(err)
	}
	a.fs = gpuHostFS{memoryFS: a.fs, proc: proc}
	s := cpuSpec(1, 0)
	s.Needs.Platform, s.Needs.Memory = Platform{Required: []string{"xpu"}}, "auto"
	// A short period reaches a sample without the five-second production cadence.
	// Stop at the first device verdict, measured or unsupported.
	verdict := make(chan struct{})
	var once sync.Once
	finish := w.observeAttemptMemoryWith(a, Job{Spec: s}, nil, 20*time.Millisecond, func(_, _ time.Time, p *AttemptMemoryPeak, _ time.Duration, device bool) {
		if device && p != nil && (p.DeviceGB != nil || p.DeviceUnavailable == deviceMemoryUnsupported) {
			once.Do(func() { close(verdict) })
		}
	})
	select {
	case <-verdict:
	case <-time.After(testWaitTimeout):
		t.Fatal("no device sample")
	}
	return finish()
}

func TestCustodianAttemptMeasuresXPUCardMemory(t *testing.T) {
	peak := custodianXPUAttemptPeak(t)
	want := float64(2312168+798352) / (1 << 20)
	if peak == nil || peak.DeviceGB == nil || math.Abs(*peak.DeviceGB-want) > 1e-9 || peak.DeviceUnavailable != "" {
		t.Fatalf("detached XPU attempt card memory = %+v, want %.4f GiB from its own card's clients", peak, want)
	}
}

// With card memory measured per attempt, the card pool learns what the job
// uses. Typed whole-card jobs (memory_gb 30 on a 30.05 GiB card) then share one
// card within its unchanged budget (#2123), as they never could unmeasured.
func TestXPUCardSharesFromCustodianMeasuredPeaks(t *testing.T) {
	measured := custodianXPUAttemptPeak(t)
	q, _ := fixture(t)
	m := sharedDiscreteMachine("xpu")
	d := &m.Devices[0]
	d.TotalMemoryGB, d.BudgetGB, d.FreeMemoryGB = 30.3, 30.05, 30.05
	s := sharedSpec("experiment", 30)
	host := 1.0
	// Enough runs of this one key for its first calibration (Rule Q at 0.9).
	for i := 0; i < 19; i++ {
		queued := submit(t, q, s)
		run := assignedJob(machineCheck(t, q, m))
		if run == nil || run.ID != queued.ID {
			t.Fatal("learning run not assigned", i, job(t, q, queued.ID).Reason)
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		r := result(run, true)
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, DeviceGB: measured.DeviceGB, DeviceUnavailable: measured.DeviceUnavailable}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	first := submit(t, q, s)
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != first.ID || a.Memory == nil || a.Memory.Card == nil || a.Memory.Card.Alone || a.Memory.Card.ChargeGB*2 > d.BudgetGB {
		t.Fatalf("measured XPU job still holds a whole card: %+v", a)
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	second := submit(t, q, s)
	b := assignedJob(machineCheck(t, q, m, a))
	if b == nil || b.ID != second.ID || !slices.Equal(a.AllocatedDevices, []string{"0"}) || !slices.Equal(b.AllocatedDevices, []string{"0"}) {
		t.Fatalf("second measured XPU job did not share the card: %+v; %s", b, job(t, q, second.ID).Reason)
	}
}
