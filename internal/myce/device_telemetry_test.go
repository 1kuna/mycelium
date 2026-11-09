package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestXPUTelemetryIdentityAndUnknown(t *testing.T) {
	_, clock := fixture(t)
	for _, output := range []string{"0000:01:00.0, 2048\n", "0000:02:00.0, 2048\n", "0000:01:00.0, N/A\n"} {
		d := MachineDevice{Platform: "xpu", pciAddress: "0000:01:00.0", FreeMemoryGB: 8, ReserveMemoryGB: 1}
		p := MachineProbe{OS: "linux", Command: func(_ context.Context, n string, args ...string) ([]byte, error) {
			if n != "xpu-smi" || strings.Join(args, " ") != "--query-gpu=pci.bus_id,memory.free --format=csv,noheader,nounits" {
				t.Fatal("unexpected native query", n, args)
			}
			return []byte(output), nil
		}, XeActivity: func(context.Context, string) (float64, error) { return 25, nil }}
		devices := []MachineDevice{d}
		(Worker{Clock: clock}).sampleXPUTelemetry(context.Background(), p, devices)
		d = devices[0]
		if output == "0000:01:00.0, 2048\n" {
			if !deviceTelemetryReady(d.Telemetry, clock.Now()) || *d.Telemetry.FreeGB != 1 || *d.Telemetry.ActivePercent != 25 {
				t.Fatal("valid native counters not retained within allowance", d)
			}
		} else if d.Telemetry.FreeGB != nil || deviceTelemetryReady(d.Telemetry, clock.Now()) {
			t.Fatal("wrong-device or N/A memory became headroom", d)
		}
	}
}

func TestDeviceTelemetryAndHostProbeAdvertisement(t *testing.T) {
	q, _ := fixture(t)
	reply, err := q.Checkin(Checkin{Machine: sharedDiscreteMachine("xpu")})
	if err != nil {
		t.Fatal(err)
	}
	if reply.DeviceTelemetryProtocol != deviceTelemetryProtocol || reply.HostProbeProtocol != hostProbeProtocol || reply.DeviceRecoveryProtocol != deviceRecoveryProtocol {
		t.Fatal("check-in did not advertise both independent telemetry protocols", reply)
	}
}

func TestDeviceTelemetryWireNegotiationAndAge(t *testing.T) {
	_, clock := fixture(t)
	strict := false
	var reports []Checkin
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		reports = append(reports, c)
		if strict && (hasDeviceTelemetry(c) || hasDeviceRecovery(c)) {
			rw.WriteHeader(400)
			_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
			return
		}
		reply := Reply{AttemptProtocol: keyedAttemptProtocol}
		if !strict {
			reply.DeviceTelemetryProtocol = deviceTelemetryProtocol
			reply.DeviceRecoveryProtocol = deviceRecoveryProtocol
		}
		_ = json.NewEncoder(rw).Encode(reply)
	}))
	defer server.Close()
	m := sharedDiscreteMachine("xpu")
	free, active := 4.0, 0.0
	s := &DeviceTelemetry{At: clock.Now(), FreeGB: &free, ActivePercent: &active}
	m.Devices[0].Telemetry = s
	m.Devices[0].Recovery = &DeviceRecovery{FaultEvent: "synthetic"}
	w := Worker{Clock: clock, Client: Client{URL: server.URL}, unifiedMemoryWire: &workerUnifiedMemoryWire{}}
	send := func() {
		t.Helper()
		_, err := w.checkin(context.Background(), Checkin{Machine: m})
		if err != nil {
			t.Fatal(err)
		}
	}
	send()
	clock.Advance(deviceMemorySampleFreshFor)
	send()
	if hasDeviceTelemetry(reports[0]) || !hasDeviceTelemetry(reports[1]) || hasDeviceRecovery(reports[0]) || !hasDeviceRecovery(reports[1]) {
		t.Fatal("telemetry did not negotiate independently")
	}
	// A delayed inventory result carries its original sensor age. Rebase it
	// onto a queue clock with a different epoch; it must remain stale.
	c := receivedDeviceTelemetry(reports[1], clock.Now().Add(24*time.Hour))
	if deviceTelemetryReady(c.Machine.Devices[0].Telemetry, clock.Now().Add(24*time.Hour)) {
		t.Fatal("send or receipt renewed stale activity")
	}
	strict = true
	send()
	send()
	if len(reports) != 5 || !hasDeviceTelemetry(reports[2]) || hasDeviceTelemetry(reports[3]) || hasDeviceTelemetry(reports[4]) || hasDeviceRecovery(reports[3]) || hasDeviceRecovery(reports[4]) || m.Devices[0].Recovery == nil || s.AgeSeconds != 0 {
		t.Fatal("downgrade failed or mutated the original sensor evidence")
	}
}
