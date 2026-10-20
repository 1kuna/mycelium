package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// StartDev is the same real API/command-runner composition as `myce dev`.
func TestM3DevTwoGPUScenario(t *testing.T) {
	root := t.TempDir()
	cfg := DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "local-test", Workers: 1, Devices: 2, HostID: "two-gpu", MemoryGB: 8, Interval: 10 * time.Millisecond}
	d, err := StartDev(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if d != nil {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := d.Client()
	submitAPI := func(s Spec) *Job {
		t.Helper()
		var j Job
		if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		return &j
	}
	get := func(id string) *Job {
		t.Helper()
		var snap Snapshot
		if err := c.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
			current, _ := d.Queue.Snapshot()
			for _, job := range current.Jobs {
				t.Logf("job=%s state=%s attempt=%d release=%t reason=%s", job.ID, job.State, job.Attempt, job.ReleaseConfirmed, job.Reason)
			}
			t.Fatal(err)
		}
		for _, j := range snap.Jobs {
			if j.ID == id {
				return j
			}
		}
		t.Fatal("job disappeared")
		return nil
	}
	helper := func(device string) Spec {
		s := m3Spec("experiment", device, 8, 1)
		s.Run.Command = []string{"sh", "-c", `test -z "${MYCE_TOKEN+x}" || exit 91; printf '%s\n' "$$" > "$MYCE_CHECKPOINT_DIR/pid"; printf '%s\n' "$MYCE_DEVICES" > "$MYCE_CHECKPOINT_DIR/device"; trap 'exit 0' TERM; while [ ! -f "$MYCE_CHECKPOINT_DIR/finish" ]; do sleep .02; done`}
		return s
	}
	a := submitAPI(helper("0"))
	b := submitAPI(helper("1"))
	s := cpuSpec(1, 1)
	s.Resumable = true
	s.Run.Command = helper("").Run.Command
	cpu := submitAPI(s)
	await(t, func() bool {
		a, b, cpu = get(a.ID), get(b.ID), get(cpu.ID)
		return a.StartedAt != nil && b.StartedAt != nil && cpu.StartedAt != nil
	})
	if a.Machine != "two-gpu" || b.Machine != a.Machine || !slices.Equal(a.AllocatedDevices, []string{"0"}) || !slices.Equal(b.AllocatedDevices, []string{"1"}) || len(cpu.AllocatedDevices) != 0 {
		t.Fatal("real concurrent placement", a, b, cpu)
	}
	pid := func(j *Job) int {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(cfg.Checkpoints, j.ID, fmt.Sprintf("attempt-%d", j.Attempt), "pid"))
		if err != nil {
			t.Fatal(err)
		}
		value, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	pids := []int{pid(a), pid(b), pid(cpu)}
	neighborPID, neighborAttempt := pid(b), b.Attempt
	prodSpec := m3Spec("production", "0", 8, 1)
	prodSpec.Run.Command = []string{"sh", "-c", `printf production; sleep .05`}
	prodSpec.SubmissionKey = "production-replay"
	prod := submitAPI(prodSpec)
	await(t, func() bool { return get(prod.ID).State == "succeeded" })
	if neighbor := get(b.ID); neighbor.Attempt != neighborAttempt || neighbor.StartedAt == nil || pid(neighbor) != neighborPID || syscall.Kill(neighborPID, 0) != nil {
		t.Fatal("production disturbed neighboring device", neighbor)
	}
	// Kill precisely one helper through its real API cancellation, leaving CPU
	// and the other GPU payload alive. The supervisor owns the exact subtree.
	if err := c.Call(ctx, "POST", "/api/jobs/"+b.ID+"/cancel", nil, nil); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return get(b.ID).State == "cancelled" && get(b.ID).ReleaseConfirmed })
	if neighbor := get(cpu.ID); neighbor.State != "running" || pid(neighbor) != pids[2] || syscall.Kill(pids[2], 0) != nil {
		t.Fatal("kill-one disturbed CPU neighbor", neighbor)
	}
	// Explicit migration evidence, supplied by M4, refuses new intent over HTTP.
	if err := d.Queue.update(func() error {
		d.Queue.s.Machines["old-gpu0"] = &Machine{ID: "old-gpu0", Gone: true, Replacement: &MachineReplacement{Machine: "two-gpu", Device: "0"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	retired := helper("0")
	retired.AllowedMachines = []string{"old-gpu0"}
	var typed *HTTPError
	if err := c.Call(ctx, "POST", "/api/jobs", retired, nil); !errors.As(err, &typed) || typed.Code != "machine_renamed" || typed.Status != 400 {
		t.Fatal("retired-name rejection", err)
	}
	await(t, func() bool { a = get(a.ID); return a.State == "running" && a.Attempt > 1 && a.StartedAt != nil })
	pids = append(pids, pid(a))
	for _, j := range []*Job{a, cpu} {
		if err := c.Call(ctx, "POST", "/api/jobs/"+j.ID+"/cancel", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool {
		return get(a.ID).State == "cancelled" && get(a.ID).ReleaseConfirmed && get(cpu.ID).State == "cancelled" && get(cpu.ID).ReleaseConfirmed
	})
	oldURL := d.URL
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	assertClosed := func(raw string) {
		t.Helper()
		u, _ := url.Parse(raw)
		conn, err := net.DialTimeout("tcp", u.Host, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Fatal("listener leaked", u.Host)
		}
	}
	assertClosed(oldURL)
	for _, p := range append(pids, neighborPID) {
		if err := syscall.Kill(p, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("payload %d survived teardown: %v", p, err)
		}
	}
	d, err = StartDev(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c = d.Client()
	replayed := submitAPI(prodSpec)
	if replayed.ID != prod.ID || replayed.State != "succeeded" || replayed.Attempt != 1 {
		t.Fatal("restart/key replay", replayed)
	}
	finalURL := d.URL
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	assertClosed(finalURL)
}

func TestM3DevModelEnginesPerDevice(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "local-test", Workers: 1, Devices: 2, HostID: "model-machine", MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := d.Client()
	await(t, func() bool {
		var snap Snapshot
		if err := c.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
			t.Fatal(err)
		}
		return len(snap.Machines) > 0
	})
	for i, project := range []string{"first", "second"} {
		if err := c.Call(ctx, "POST", "/api/models/pin", ModelPin{Project: project, Model: "dev-model", Machine: "model-machine", Device: strconv.Itoa(i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	var slots []ModelSlot
	await(t, func() bool {
		var snap Snapshot
		if err := c.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
			t.Fatal(err)
		}
		slots = snap.LoadedModels
		return len(slots) == 2 && slots[0].State == "ready" && slots[1].State == "ready"
	})
	if slots[0].Device == slots[1].Device || slots[0].Attempt == slots[1].Attempt {
		t.Fatal("shared engine custody", slots)
	}
	for _, slot := range slots {
		req, err := http.NewRequestWithContext(ctx, "POST", slotURL(d.Queue, slot)+"/v1/completions", strings.NewReader(`{"model":"dev-model","prompt":"hello"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer local-test")
		req.Header.Set("X-Myce-Engine-Attempt", slot.Attempt)
		req.Header.Set("X-Myce-Session", slot.Session)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("exact device route %s: %d %s", slot.Device, resp.StatusCode, body)
		}
	}
}

func slotURL(q *Queue, s ModelSlot) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.s.Machines[s.Machine].ModelURL
}
