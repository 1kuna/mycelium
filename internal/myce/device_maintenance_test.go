package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func changeDevice(t *testing.T, q *Queue, host, identity, action string) HostMaintenanceStatus {
	t.Helper()
	status, err := q.HostMaintenanceStatus(host)
	if err != nil {
		t.Fatal(err)
	}
	d, err := status.ResolveDevice(identity)
	if err != nil {
		t.Fatal(err)
	}
	var revision uint64
	for _, p := range status.DeviceMaintenance {
		if p.Platform == d.Platform && p.Identity == d.Identity {
			revision = p.Revision
		}
	}
	r := DeviceMaintenanceRequest{Action: action, Platform: d.Platform, Identity: d.Identity, IfRevision: &revision}
	if action == "disable" {
		r.Reason = "recurring driver faults"
	}
	status, err = q.ChangeDeviceMaintenance(host, r)
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func TestDeviceMaintenanceDrainPreservesAttemptAndSiblingCapacity(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || !slices.Equal(a.AllocatedDevices, []string{"0"}) {
		t.Fatal("missing initial device-0 attempt", a)
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	status := changeDevice(t, q, m.HostID, "0", "disable")
	if len(status.DeviceMaintenance) != 1 || !status.DeviceMaintenance[0].Disabled || status.DeviceMaintenance[0].Identity != "fake-0" || status.DeviceMaintenance[0].Reason != "recurring driver faults" || !status.DeviceMaintenance[0].UpdatedAt.Equal(clock.Now()) {
		t.Fatal("missing durable identity/reason/queue timestamp", status)
	}
	// A full-budget sibling job still fits; disabling does not lower either
	// card's reported allowance or consume extra shared CPU/RAM capacity.
	s := spec("experiment")
	s.Needs.MemoryGB = 8
	b := submit(t, q, s)
	r := machineCheck(t, q, m, a)
	var sibling *Job
	for _, d := range r.Attempts {
		if d.Stop || d.Kill {
			t.Fatal("device drain stopped a running attempt", d)
		}
		if d.Job != nil && d.Job.ID == b.ID {
			sibling = d.Job
		}
	}
	if sibling == nil || !slices.Equal(sibling.AllocatedDevices, []string{"1"}) {
		t.Fatal("sibling lost full capacity", r)
	}
	cpu := submit(t, q, cpuSpec(1, 1))
	var cpuAssigned *Job
	for _, d := range machineCheck(t, q, m, a, sibling).Attempts {
		if d.Job != nil && d.Job.ID == cpu.ID {
			cpuAssigned = d.Job
		}
	}
	if cpuAssigned == nil {
		t.Fatal("device disable fenced CPU capacity")
	}
	if err := q.Started(result(cpuAssigned, false)); err != nil {
		t.Fatal("device disable fenced CPU Started", err)
	}
	finish(t, q, a, true)
	blocked := spec("experiment")
	blocked.Needs.GPU = &GPUSelection{Devices: []string{"0"}}
	waiting := submit(t, q, blocked)
	machineCheck(t, q, m, sibling)
	requireState(t, q, waiting.ID, "queued")
	changeDevice(t, q, m.HostID, "fake-0", "enable")
	var resumed bool
	for _, d := range machineCheck(t, q, m, sibling).Attempts {
		resumed = resumed || d.Job != nil && d.Job.ID == waiting.ID
	}
	if !resumed {
		t.Fatal("enable did not restore device admission")
	}
}

func TestDeviceMaintenancePersistsByIdentityAcrossRestarts(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
	db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	machineCheck(t, q, m)
	disabled := changeDevice(t, q, m.HostID, "0", "disable").DeviceMaintenance[0]
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	oldSession := m.Session
	m.Session = "restarted-worker"
	// Simulate a fresh worker inventory assigning a different ordinal to the
	// same physical device, and a replacement taking the old ordinal.
	m.Devices[0].ID, m.Devices[0].Selector = "2", "2"
	m.Devices = append(m.Devices, MachineDevice{ID: "0", Platform: "fake", Identity: "replacement", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8})
	if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: oldSession, AttemptProtocol: keyedAttemptProtocol}); err != nil {
		t.Fatal(err)
	}
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || len(status.DeviceMaintenance) != 1 || status.DeviceMaintenance[0] != disabled {
		t.Fatal("queue/worker restart erased policy", status, err)
	}
	for _, id := range []string{"2", "0"} {
		s := spec("experiment")
		s.Needs.GPU = &GPUSelection{Devices: []string{id}}
		j := submit(t, q, s)
		r := assignedJob(machineCheck(t, q, m))
		if id == "2" {
			if r != nil {
				t.Fatal("disabled physical identity admitted under new ordinal", r)
			}
			requireState(t, q, j.ID, "queued")
		} else if r == nil || r.ID != j.ID || !slices.Equal(r.AllocatedDevices, []string{"0"}) {
			t.Fatal("replacement inherited ordinal policy", r)
		}
	}
	// Policy remains visible and operable by stable identity when absent.
	m.Devices = m.Devices[1:]
	machineCheck(t, q, m)
	snap, err := q.Snapshot()
	if err != nil || len(snap.DeviceMaintenance) != 1 || snap.DeviceMaintenance[0] != disabled {
		t.Fatal("absent disabled device hidden from status", snap.DeviceMaintenance, err)
	}
	changeDevice(t, q, m.HostID, "fake-0", "enable")
}

func TestDeviceMaintenanceRevisionAuthorityAndUnstartedFence(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	submit(t, q, spec("experiment"))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil {
		t.Fatal("missing offer")
	}
	server := httptest.NewServer(API{Queue: q, Token: "fixture-only"}.Handler())
	defer server.Close()
	zero := uint64(0)
	request := DeviceMaintenanceRequest{Action: "disable", Platform: "fake", Identity: "fake-0", Reason: "driver faults", IfRevision: &zero}
	client := Client{URL: server.URL}
	if err := client.Call(t.Context(), "POST", "/api/hosts/machine/devices/maintenance", request, nil); err == nil {
		t.Fatal("unauthenticated mutation accepted")
	}
	client.Token = "fixture-only"
	var status HostMaintenanceStatus
	if err := client.Call(t.Context(), "POST", "/api/hosts/machine/devices/maintenance", request, &status); err != nil {
		t.Fatal(err)
	}
	latencies := q.requestLatencies.snapshot()
	if len(latencies) != 1 || latencies[0].Route != "POST /api/hosts/{id}/devices/maintenance" || latencies[0].Count != 2 || latencies[0].Samples != 2 {
		t.Fatal("device route lost authenticated or unauthenticated request spans", latencies)
	}
	if err := q.Started(result(a, false)); !errors.Is(err, ErrStale) {
		t.Fatal("late Started bypassed device drain", err)
	}
	if err := q.AuthorizeLaunch(HostLaunch{Machine: a.Machine, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: "1"}); !errors.Is(err, ErrStale) {
		t.Fatal("cached batch offer bypassed device drain", err)
	}
	request.Action, request.Reason = "enable", ""
	if err := client.Call(t.Context(), "POST", "/api/hosts/machine/devices/maintenance", request, nil); err == nil {
		t.Fatal("stale revision overwrote disable at same fake-clock time")
	}
	request.IfRevision = nil
	if _, err := q.ChangeDeviceMaintenance(m.HostID, request); err == nil {
		t.Fatal("missing revision accepted")
	}
	// Device edits neither resume the host nor revoke an upgrade drain receipt.
	drained, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain", DrainOwner: "upgrade"})
	if err != nil {
		t.Fatal(err)
	}
	changeDevice(t, q, m.HostID, "fake-0", "enable")
	changeDevice(t, q, m.HostID, "fake-0", "disable")
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &drained.Policy.UpdatedAt, IfDrainOwner: "upgrade"}); err != nil {
		t.Fatal("device edit revoked host drain receipt", err)
	}
	status, err = q.HostMaintenanceStatus(m.HostID)
	if err != nil || !status.DeviceMaintenance[0].Disabled {
		t.Fatal("host resume erased device policy", status, err)
	}
	// The authenticated web status includes an escaped operator reason/time.
	page := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer fixture-only")
	API{Queue: q, Token: "fixture-only"}.Handler().ServeHTTP(page, req)
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "recurring driver faults") || !strings.Contains(page.Body.String(), "fake-0") || !strings.Contains(page.Body.String(), "disabled by operator since") {
		t.Fatal("web status hid device disable", page.Code, page.Body.String())
	}
}

func TestDeviceMaintenanceModelCallsAndLaunches(t *testing.T) {
	q, _, legacy := modelFixture(t)
	// Use the same model catalog on a two-device worker, with no legacy host
	// competing for calls. No model process or native accelerator is needed.
	legacy.Leaving = true
	check(t, q, legacy, nil)
	m := m3Machine()
	m.ModelURL, m.ModelConfigHash = "http://127.0.0.1:1234", modelConfigHash(q.models)
	machineCheck(t, q, m)
	first := addModelCall(t, q, "experiment")
	var slot *ModelSlot
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID {
			slot = s
		}
	}
	if slot == nil || slot.Device != "0" {
		t.Fatal("missing initial model placement", slot)
	}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "test", Device: "0", Attempt: slot.Attempt, State: "ready"}}}); err != nil {
		t.Fatal(err)
	}
	if first.Machine != m.ID || slot.InFlight != 1 {
		t.Fatal("initial call not routed", first)
	}
	changeDevice(t, q, m.HostID, "0", "disable")
	if slot.State != "ready" || slot.StopAt != nil || first.Error != "" || first.StopAt != nil {
		t.Fatal("disable interrupted resident engine/active call", slot, first)
	}
	next := addModelCall(t, q, "experiment")
	if next.Machine != "" {
		t.Fatal("new call routed to disabled ready engine", next)
	}
	sibling := q.s.ModelSlots[slotKey(m.ID, "test", "1")]
	if sibling == nil || sibling.State != "loading" {
		t.Fatal("sibling model capacity unavailable", sibling)
	}
	launch := HostLaunch{Machine: m.ID, Session: m.Session, Device: "1", Kind: "engine", Key: "test", Attempt: sibling.Attempt}
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal("enabled sibling engine launch refused", err)
	}
	changeDevice(t, q, m.HostID, "1", "disable")
	if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
		t.Fatal("cached engine offer bypassed device disable", err)
	}
	if first.Error != "" || first.StopAt != nil {
		t.Fatal("sibling disable interrupted active call", first)
	}
}

// Exercise the same real HTTP API/worker/command runner as myce dev with two
// fake accelerators: the sibling finishes while the disabled target waits.
func TestDeviceMaintenanceDev(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "dev-fixture", Workers: 1, Devices: 2, HostID: "two-gpu", MemoryGB: 8, Interval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	c := d.Client()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	await(t, func() bool {
		var status HostMaintenanceStatus
		if err := c.Call(ctx, "GET", "/api/hosts/two-gpu/maintenance", nil, &status); err != nil {
			t.Fatal(err)
		}
		return len(status.Workers) == 1 && len(status.Workers[0].Machine.Devices) == 2
	})
	zero := uint64(0)
	request := DeviceMaintenanceRequest{Action: "disable", Platform: "fake", Identity: "fake-0", Reason: "driver faults", IfRevision: &zero}
	if err := c.Call(ctx, "POST", "/api/hosts/two-gpu/devices/maintenance", request, nil); err != nil {
		t.Fatal(err)
	}
	jobs := make([]Job, 2)
	for i, id := range []string{"0", "1"} {
		s := m3Spec("experiment", id, 8, 1)
		s.Run.Command = []string{"sh", "-c", "printf device-admission-ok"}
		if err := c.Call(ctx, "POST", "/api/jobs", s, &jobs[i]); err != nil {
			t.Fatal(err)
		}
	}
	get := func(id string) Job {
		t.Helper()
		var j Job
		if err := c.Call(ctx, "GET", "/api/jobs/"+id, nil, &j); err != nil {
			t.Fatal(err)
		}
		return j
	}
	await(t, func() bool { return get(jobs[1].ID).State == "succeeded" })
	if got := get(jobs[0].ID); got.State != "queued" || got.Attempt != 0 {
		t.Fatal("disabled fake GPU received a placement", got)
	}
	one := uint64(1)
	request.Action, request.Reason, request.IfRevision = "enable", "", &one
	if err := c.Call(ctx, "POST", "/api/hosts/two-gpu/devices/maintenance", request, nil); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return get(jobs[0].ID).State == "succeeded" })
}

func TestDeviceMaintenanceKeepsStartedQualification(t *testing.T) {
	q, w, _, _, _, plans := qualificationFixture(t)
	d := m3Machine().Devices[0]
	d.ExtensionFingerprint, d.ExtensionReports = w.Machine.ExtensionFingerprint, w.Machine.ExtensionReports
	w.Machine.Devices = []MachineDevice{d}
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return w.Machine.Devices, nil }
	machineCheck(t, q, w.Machine)
	reconcileQualification(t, w, plans[0])
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	w.Machine.Devices[0].ExtensionReports = w.Machine.ExtensionReports
	a := assignedJob(machineCheck(t, q, w.Machine))
	if a == nil || automaticExtensionCheck(a) == nil {
		t.Fatal("missing automatic qualification attempt", a)
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	changeDevice(t, q, hostID(&w.Machine), "0", "disable")
	if got := job(t, q, a.ID); got.State != "running" || cancelled(got) || got.StopAt != nil {
		t.Fatal("device admission cancelled Started qualification", got)
	}
	for _, directive := range machineCheck(t, q, w.Machine, a).Attempts {
		if directive.ID == a.ID && directive.Stop {
			t.Fatal("Started qualification received stop", directive)
		}
	}
}

func TestDeviceMaintenanceRecoveryKeepsExactCopy(t *testing.T) {
	for _, device := range []string{"0", "1"} {
		t.Run("recover-device-"+device, func(t *testing.T) {
			q, clock := fixture(t)
			m := m3Machine()
			s := m3Spec("experiment", device, 8, 1)
			submit(t, q, s)
			a := assignedJob(machineCheck(t, q, m))
			if a == nil {
				t.Fatal("missing initial attempt")
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			changeDevice(t, q, m.HostID, "0", "disable")
			clock.Advance(LossAfter)
			check(t, q, observer(), nil)
			requireState(t, q, a.ID, "queued")
			for _, directive := range machineCheck(t, q, m, a).Attempts {
				if directive.ID == a.ID && (directive.Stop || directive.Kill) {
					t.Fatal("disabled primary rejected exact running copy", directive)
				}
			}
			got := job(t, q, a.ID)
			if got.State != "running" || got.Attempt != a.Attempt || got.StartedAt == nil || !slices.Equal(got.AllocatedDevices, []string{device}) {
				t.Fatal("recovery changed attempt or component custody", got)
			}
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			requireState(t, q, a.ID, "running")
		})
	}
}

func TestDeviceMaintenanceSharedModelCopy(t *testing.T) {
	q, _, legacy := modelFixture(t)
	legacy.Leaving = true
	check(t, q, legacy, nil)
	m := m3Machine()
	m.ModelConfigHash = modelConfigHash(q.models)
	machineCheck(t, q, m)
	changeDevice(t, q, m.HostID, "0", "disable")
	if err := q.SyncModel(SyncRequest{Model: "test", To: m.ID}); err != nil {
		t.Fatal(err)
	}
	r := machineCheck(t, q, m)
	if len(r.CopyPlan) != 1 || r.CopyPlan[0].Launch == nil {
		t.Fatal("shared model copy not offered", r.CopyPlan)
	}
	if err := q.AuthorizeLaunch(*r.CopyPlan[0].Launch); err != nil {
		t.Fatal("disabled component blocked host-shared model cache copy", err)
	}
}
