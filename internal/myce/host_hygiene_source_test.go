package myce

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestHostHygieneForeignConsumersExcludeOwnedTree(t *testing.T) {
	data := []byte("10 1 3.0 100 01:00 svc /usr/bin/owner\n11 10 99.0 100 00:30 svc /tmp/child\n12 11 99.0 100 00:10 svc /tmp/grandchild\n20 1 50.0 200 2-03:04:05 alice /usr/bin/foreign app\nbad line\n")
	out := foreignHealthInventory(parseHealthProcesses(data), map[int]bool{10: true}, nil, nil).processes
	if len(out) != 1 || out[0].PID != 20 || out[0].Executable != "foreign app" || out[0].RSSBytes != 200*1024 {
		t.Fatal(out)
	}
	if out[0].User != "alice" || out[0].AgeSeconds != 2*86400+3*3600+4*60+5 {
		t.Fatal("owner or age missing", out[0])
	}
	if slices.ContainsFunc(out, func(p ForeignConsumer) bool { return strings.Contains(p.Executable, "/") }) {
		t.Fatal("published executable paths")
	}
}

// The 2026-11-10 case: a multi-process foreign container held most of a
// unified-memory host through device allocations that RSS need not show.
func TestHostHygieneForeignServicesAndGPUMemory(t *testing.T) {
	var data strings.Builder
	// Twelve busy small processes would crowd a per-process view.
	for pid := 100; pid < 112; pid++ {
		data.WriteString(strings.Join([]string{strconv.Itoa(pid), "1", "90.0", "1000", "05:00", "bob", "/usr/bin/busy"}, " ") + "\n")
	}
	// The container: an old root process and workers with small RSS.
	data.WriteString("200 1 0.5 2000 02:10:00 root /usr/bin/containerd-shim\n")
	data.WriteString("201 200 1.0 3000 02:09:59 root /usr/bin/python3\n")
	data.WriteString("202 200 1.0 3000 02:09:58 root /usr/bin/python3\n")
	services := map[int]string{200: "docker-abc.scope"}
	gpu := parseForeignGPU([]byte("201, 40960\n202, 30720\n202, 1024\nmalformed\n999, [N/A]\n"))
	inv := foreignHealthInventory(parseHealthProcesses([]byte(data.String())), map[int]bool{}, services, gpu)
	if !slices.ContainsFunc(inv.processes, func(p ForeignConsumer) bool {
		return p.PID == 202 && p.GPUBytes == 31744<<20 && p.Service == "docker-abc.scope"
	}) {
		t.Fatal("device-memory leader missing from process context", inv.processes)
	}
	if len(inv.processes) > 10 {
		t.Fatal("unbounded process context", len(inv.processes))
	}
	if len(inv.services) != 1 {
		t.Fatal("unlabelled processes formed a service", inv.services)
	}
	s := inv.services[0]
	if s.Service != "docker-abc.scope" || s.Processes != 3 || s.User != "root" || s.AgeSeconds != 2*3600+10*60 || s.RSSBytes != 8000*1024 || s.GPUBytes != (40960+31744)<<20 {
		t.Fatal("container totals", s)
	}
}

func TestHostHygieneCgroupService(t *testing.T) {
	for in, want := range map[string]string{
		"0::/system.slice/docker-abc.scope\n":              "docker-abc.scope",
		"0::/system.slice/ollama.service\n":                "ollama.service",
		"12:memory:/docker/0123abcd\n0::/\n":               "docker-0123abcd",
		"0::/user.slice/user-1000.slice/session-2.scope\n": "session-2.scope",
		"0::/\n": "",
	} {
		if got := cgroupService([]byte(in)); got != want {
			t.Fatalf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestHostHygieneForeignDetailWaitsForQueueSupport(t *testing.T) {
	c := Checkin{Machine: Machine{ID: "m", Session: "s", HostHealth: &HostHealth{
		Foreign:         []ForeignConsumer{{PID: 1, Executable: "x", User: "u", AgeSeconds: 9, GPUBytes: 7}},
		ForeignServices: []ForeignService{{Service: "a.service", Processes: 1}},
	}}}
	local := c.Machine.HostHealth
	w := Worker{diskBudgetWire: &workerDiskBudgetWire{}}
	sent := w.diskBudgetCheckin(c)
	f := sent.Machine.HostHealth.Foreign[0]
	if hasForeignDetailFields(sent) || f.PID != 1 || f.Executable != "x" {
		t.Fatal("strict queue received detail or lost the original shape", sent.Machine.HostHealth)
	}
	if local.Foreign[0].User != "u" || len(local.ForeignServices) != 1 {
		t.Fatal("stripping changed local evidence")
	}
	*w.diskBudgetWire = workerDiskBudgetWire{machine: "m", session: "s", foreign: true}
	if !hasForeignDetailFields(w.diskBudgetCheckin(c)) {
		t.Fatal("advertising queue did not receive detail")
	}
	// A new session must renegotiate before sending detail.
	c.Machine.Session = "s2"
	if hasForeignDetailFields(w.diskBudgetCheckin(c)) {
		t.Fatal("detail sent before the new session's advertisement")
	}
}
