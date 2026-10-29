package myce

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestComponentExclusionDev(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "dev-fixture", Workers: 1, Devices: 3, HostID: "fake-host", MemoryGB: 8, Interval: 20 * time.Millisecond})
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
		var s HostMaintenanceStatus
		if err := c.Call(ctx, "GET", "/api/hosts/fake-host/maintenance", nil, &s); err != nil {
			t.Fatal(err)
		}
		return len(s.Workers) == 1 && len(s.Workers[0].Machine.Devices) == 3
	})
	zero := uint64(0)
	r := ComponentExclusionRequest{Action: "add", Selector: "host=fake-host;devices=0,1", SetBy: "dev-operator", Reason: "exercise list selector", IfRevision: &zero}
	var p ComponentExclusion
	if err := c.Call(ctx, "POST", "/api/exclusions", r, &p); err != nil {
		t.Fatal(err)
	}
	jobs := make([]Job, 3)
	for i, id := range []string{"0", "1", "2"} {
		s := m3Spec("experiment", id, 8, 1)
		s.Run.Command = []string{"sh", "-c", "printf selector-ok"}
		if err := c.Call(ctx, "POST", "/api/jobs", s, &jobs[i]); err != nil {
			t.Fatal(err)
		}
	}
	get := func(id string) Job {
		var j Job
		if err := c.Call(ctx, "GET", "/api/jobs/"+id, nil, &j); err != nil {
			t.Fatal(err)
		}
		return j
	}
	await(t, func() bool { return get(jobs[2].ID).State == "succeeded" })
	for _, j := range jobs[:2] {
		if got := get(j.ID); got.State != "queued" || got.Attempt != 0 {
			t.Fatal("excluded device ran", got)
		}
	}
	r.Action, r.Selector, r.Reason, r.IfRevision = "remove", p.Selector, "", &p.Revision
	if err := c.Call(ctx, "POST", "/api/exclusions", r, nil); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return get(jobs[0].ID).State == "succeeded" && get(jobs[1].ID).State == "succeeded" })
}

func changeExclusion(t *testing.T, q *Queue, selector, action string) ComponentExclusion {
	t.Helper()
	p, err := q.ResolveComponentSelector(selector)
	if err != nil {
		t.Fatal(err)
	}
	r := ComponentExclusionRequest{Action: action, Selector: p.Selector, SetBy: "test-operator", IfRevision: &p.Revision}
	if action == "add" {
		r.Reason = "driver investigation"
	}
	p, err = q.ChangeComponentExclusion(r)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func exclusionSpec(host, device string) Spec {
	s := spec("experiment")
	s.AllowedMachines = []string{host}
	s.Needs.GPU = &GPUSelection{Devices: []string{device}}
	return s
}

func TestComponentExclusionSelectors(t *testing.T) {
	for _, tc := range []struct {
		name, selector               string
		blocked                      []string
		otherHostBlocked, cpuBlocked bool
	}{
		{"single", "host=machine;devices=0", []string{"0"}, false, false},
		{"list", "host=machine;devices=1,0", []string{"0", "1"}, false, false},
		{"host-kind", "host=machine;kind=gpu", []string{"0", "1", "2"}, false, false},
		{"fleet-kind", "kind=gpu", []string{"0", "1", "2"}, true, false},
		{"host-platform", "host=machine;platform=fake", []string{"0", "1"}, false, false},
		{"fleet-platform", "platform=fake", []string{"0", "1"}, true, false},
		{"host", "host=machine", []string{"0", "1", "2"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
			db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
			q, err := Open(db, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { q.Close() })
			fakeCheckpointAvailability(t, q, true)
			m := m3Machine()
			d := m.Devices[1]
			d.ID, d.Selector, d.Identity, d.Platform = "2", "2", "cuda-2", "cuda"
			d.CapabilityProfile = &CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", MemoryLayout: "separate", Accelerators: []AcceleratorProfile{{Type: "cuda", MemoryBytes: 8 << 30}}}
			m.Devices = append(m.Devices, d)
			submit(t, q, exclusionSpec(m.ID, "0"))
			a := assignedJob(machineCheck(t, q, m))
			if a == nil {
				t.Fatal("initial attempt missing")
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			p := changeExclusion(t, q, tc.selector, "add")
			if !p.UpdatedAt.Equal(clock.Now()) || p.SetBy != "test-operator" || p.Reason != "driver investigation" {
				t.Fatal("missing attribution", p)
			}
			for _, directive := range machineCheck(t, q, m, a).Attempts {
				if directive.ID == a.ID && (directive.Stop || directive.Kill) {
					t.Fatal("Started attempt stopped", directive)
				}
			}
			finish(t, q, a, true)
			machineCheck(t, q, m)
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			q, err = Open(db, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			fakeCheckpointAvailability(t, q, true)
			if got := q.ComponentExclusions(); len(got) != 1 || got[0].Selector != p.Selector || got[0].Revision != p.Revision || !got[0].Active || !got[0].UpdatedAt.Equal(p.UpdatedAt) {
				t.Fatal("restart lost policy", got)
			}
			// Move the ordinal while keeping its physical identity. Device
			// lists must follow the identity; broad selectors match inventory.
			m.Devices[0].ID, m.Devices[0].Selector = "3", "3"
			machineCheck(t, q, m)
			var blockedJobs []*Job
			for _, device := range []string{"3", "1", "2"} {
				j := submit(t, q, exclusionSpec(m.ID, device))
				r := assignedJob(machineCheck(t, q, m))
				oldID := device
				if device == "3" {
					oldID = "0"
				}
				if slices.Contains(tc.blocked, oldID) {
					if r != nil {
						t.Fatal("excluded component admitted", device, r)
					}
					requireState(t, q, j.ID, "queued")
					blockedJobs = append(blockedJobs, j)
				} else {
					if r == nil || r.ID != j.ID {
						t.Fatal("unselected sibling/platform lost capacity", device, r)
					}
					finish(t, q, r, true)
					machineCheck(t, q, m)
				}
			}
			cpu := cpuSpec(1, 1)
			cpu.AllowedMachines = []string{m.ID}
			cj := submit(t, q, cpu)
			cr := assignedJob(machineCheck(t, q, m))
			if tc.cpuBlocked {
				if cr != nil {
					t.Fatal("host drain admitted CPU", cr)
				}
			} else {
				if cr == nil || cr.ID != cj.ID {
					t.Fatal("accelerator selector blocked CPU", cr)
				}
				finish(t, q, cr, true)
				machineCheck(t, q, m)
			}
			other := m3Machine()
			other.ID, other.HostID, other.Session = "other", "other", "other-session"
			machineCheck(t, q, other)
			oj := submit(t, q, exclusionSpec(other.ID, "0"))
			or := assignedJob(machineCheck(t, q, other))
			if tc.otherHostBlocked {
				if or != nil {
					t.Fatal("fleet selector missed another host", or)
				}
			} else {
				if or == nil || or.ID != oj.ID {
					t.Fatal("host selector affected other host", or)
				}
				finish(t, q, or, true)
				machineCheck(t, q, other)
			}
			changeExclusion(t, q, p.Selector, "remove")
			r := assignedJob(machineCheck(t, q, m))
			if r == nil || !slices.ContainsFunc(blockedJobs, func(j *Job) bool { return j.ID == r.ID }) {
				t.Fatal("removal failed to restore capacity", r)
			}
			if err := q.Started(result(r, false)); err != nil {
				t.Fatal("restored component could not start", err)
			}
		})
	}
}

func TestComponentExclusionRevisionAuthorityAndDrain(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	machineCheck(t, q, m)
	server := httptest.NewServer(API{Queue: q, Token: "fixture-only"}.Handler())
	defer server.Close()
	zero := uint64(0)
	r := ComponentExclusionRequest{Action: "add", Selector: "platform=fake", SetBy: "operator", IfRevision: &zero}
	c := Client{URL: server.URL}
	if err := c.Call(t.Context(), "POST", "/api/exclusions", r, nil); err == nil {
		t.Fatal("unauthenticated edit accepted")
	}
	c.Token = "fixture-only"
	if err := c.Call(t.Context(), "POST", "/api/exclusions", r, nil); err != nil {
		t.Fatal(err)
	}
	r.Action = "remove"
	if _, err := q.ChangeComponentExclusion(r); !errors.Is(err, ErrStale) {
		t.Fatal("stale revision accepted", err)
	}
	r.IfRevision = nil
	if _, err := q.ChangeComponentExclusion(r); err == nil {
		t.Fatal("missing revision accepted")
	}
	changeExclusion(t, q, "platform=fake", "remove")
	// New inventory is covered by a broad policy without expanding it into
	// a snapshot of today's device IDs.
	changeExclusion(t, q, "platform=cuda", "add")
	d := m.Devices[0]
	d.ID, d.Selector, d.Identity, d.Platform = "2", "2", "later", "cuda"
	d.CapabilityProfile = &CapabilityProfile{MemoryLayout: "separate", Accelerators: []AcceleratorProfile{{Type: "cuda", MemoryBytes: 8 << 30}}}
	m.Devices = append(m.Devices, d)
	machineCheck(t, q, m)
	j := submit(t, q, exclusionSpec(m.ID, "2"))
	machineCheck(t, q, m)
	requireState(t, q, j.ID, "queued")
	changeExclusion(t, q, "platform=cuda", "remove")
	if restored := assignedJob(machineCheck(t, q, m)); restored == nil || restored.ID != j.ID {
		t.Fatal("newly discovered platform capacity did not restore", restored)
	} else {
		finish(t, q, restored, true)
		machineCheck(t, q, m)
	}
	// Removing a narrow setting does not undo an overlapping broad one.
	changeExclusion(t, q, "kind=gpu", "add")
	narrow := changeExclusion(t, q, "host=machine;devices=0", "add")
	changeExclusion(t, q, narrow.Selector, "remove")
	if !q.deviceAdmissionBlocked(allocationMachine(q.s.Machines[m.ID], []string{"0"})) {
		t.Fatal("narrow remove bypassed broad fence")
	}
	changeExclusion(t, q, "kind=gpu", "remove")
	changeExclusion(t, q, "host=machine", "add")
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "resume"}); err == nil {
		t.Fatal("resume bypassed durable exclusion")
	}
	if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain", DrainOwner: "independent-upgrade"}); err != nil {
		t.Fatal(err)
	}
	changeExclusion(t, q, "host=machine", "remove")
	status, err := q.HostMaintenanceStatus(m.HostID)
	if err != nil || status.Policy.Mode != "drained" || status.Policy.DrainOwner != "independent-upgrade" {
		t.Fatal("removal resumed independent drain", status, err)
	}
	for _, selector := range []string{"", "kind=cpu", "kind=ram", "platform=cpu", "host=machine;devices=0;kind=gpu", "devices=0", "platform=missing", "kind=gpu;kind=gpu"} {
		if _, err := q.ResolveComponentSelector(selector); err == nil {
			t.Fatal("invalid selector accepted", selector)
		}
	}
}

func TestComponentExclusionUnstartedModelAndBenchmarkFences(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	submit(t, q, exclusionSpec(m.ID, "0"))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil {
		t.Fatal("missing offer")
	}
	changeExclusion(t, q, "host=machine;kind=gpu", "add")
	if err := q.Started(result(a, false)); !errors.Is(err, ErrStale) {
		t.Fatal("late Started bypassed exclusion", err)
	}
	if !q.benchmarkDeviceAdmissionBlocked(&m) {
		t.Fatal("benchmark missed selector")
	}
	q, _, legacy := modelFixture(t)
	legacy.Leaving = true
	check(t, q, legacy, nil)
	m = m3Machine()
	m.ModelURL, m.ModelConfigHash = "http://127.0.0.1:1234", modelConfigHash(q.models)
	machineCheck(t, q, m)
	first := addModelCall(t, q, "experiment")
	var slot *ModelSlot
	for _, s := range q.s.ModelSlots {
		if s.Machine == m.ID {
			slot = s
		}
	}
	if slot == nil {
		t.Fatal("missing engine")
	}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, ModelReports: []ModelReport{{Name: "test", Device: slot.Device, Attempt: slot.Attempt, State: "ready"}}}); err != nil {
		t.Fatal(err)
	}
	if first.Machine != m.ID {
		t.Fatal("call not routed")
	}
	changeExclusion(t, q, "platform=fake", "add")
	if first.Error != "" || first.StopAt != nil || slot.State != "ready" {
		t.Fatal("running model call interrupted", first, slot)
	}
	if next := addModelCall(t, q, "experiment"); next.Machine != "" {
		t.Fatal("new model call bypassed fleet selector", next)
	}
	// Selector and attribution are present in both JSON and escaped web status.
	snap, err := q.Snapshot()
	if err != nil || len(snap.ComponentExclusions) != 1 || snap.ComponentExclusions[0].SetBy != "test-operator" {
		t.Fatal("status omitted attribution", snap.ComponentExclusions, err)
	}
	page := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer fixture-only")
	API{Queue: q, Token: "fixture-only"}.Handler().ServeHTTP(page, req)
	if !strings.Contains(page.Body.String(), "platform=fake") || !strings.Contains(page.Body.String(), "test-operator") {
		t.Fatal("web status omitted exclusion", page.Body.String())
	}
}
