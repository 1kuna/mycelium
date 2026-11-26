package myce

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestXPUTelemetryUsesBrokerWithoutVendorInitialization(t *testing.T) {
	_, clock := fixture(t)
	body := fmt.Sprintf(`{"sampled_at":%q,"gtt_mm":{"0000:01:00.0":"usage: 999999"},"vram":{"0000:01:00.0":{"total_bytes":8589934592,"used_bytes":2147483648}}}`, clock.Now().Format(time.RFC3339Nano))
	w := Worker{Clock: clock}
	calls := 0
	p := MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) {
		calls++
		return []byte("0000:01:00.0, 4096\n"), nil
	}, XeActivity: func(context.Context, string) (float64, error) { return 25, nil }, XPUFreeMemory: xpuTelemetryTestSource(clock, body)}
	devices := []MachineDevice{{Platform: "xpu", pciAddress: "0000:01:00.0", ReserveMemoryGB: 1}}
	w.sampleXPUTelemetry(t.Context(), p, devices)
	if calls != 0 || !deviceTelemetryReady(devices[0].Telemetry, clock.Now()) || *devices[0].Telemetry.FreeGB != 5 {
		t.Fatal("broker VRAM not used independently of GTT; vendor initializations", calls, devices[0].Telemetry)
	}
}

func TestXPUTelemetryIdentityAndUnknown(t *testing.T) {
	_, clock := fixture(t)
	for _, vram := range []string{
		`"0000:01:00.0":{"total_bytes":8589934592,"used_bytes":6442450944}`,
		`"0000:02:00.0":{"total_bytes":8589934592,"used_bytes":6442450944}`,
		`"0000:01:00.0":{"total_bytes":8589934592}`,
		`"0000:01:00.0":{"total_bytes":8589934592,"used_bytes":8589934593}`,
		`"0000:01:00.0":{"total_bytes":8589934592,"used_bytes":0},"00000000:01:00.0":{"total_bytes":8589934592,"used_bytes":0}`,
	} {
		body := fmt.Sprintf(`{"sampled_at":%q,"gtt_mm":{"0000:01:00.0":"usage: 0"},"vram":{%s}}`, clock.Now().Format(time.RFC3339Nano), vram)
		w := Worker{Clock: clock}
		p := MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) {
			t.Fatal("telemetry initialized a vendor runtime")
			return nil, nil
		}, XeActivity: func(context.Context, string) (float64, error) { return 25, nil }, XPUFreeMemory: xpuTelemetryTestSource(clock, body)}
		devices := []MachineDevice{{Platform: "xpu", pciAddress: "0000:01:00.0", FreeMemoryGB: 8, ReserveMemoryGB: 1}}
		w.sampleXPUTelemetry(t.Context(), p, devices)
		d := devices[0]
		if strings.Contains(vram, `"used_bytes":6442450944`) && strings.HasPrefix(vram, `"0000:01:00.0"`) {
			if !deviceTelemetryReady(d.Telemetry, clock.Now()) || *d.Telemetry.FreeGB != 1 || *d.Telemetry.ActivePercent != 25 {
				t.Fatal("valid broker counters not retained within allowance", d)
			}
		} else if d.Telemetry.FreeGB != nil || deviceTelemetryReady(d.Telemetry, clock.Now()) {
			t.Fatal("wrong-device, incomplete or ambiguous memory became headroom", d)
		}
	}
}

// A DRM_XE_DEVICE_QUERY_MEM_REGIONS reply: {class, instance, size, used} each.
func xeRegionsReply(regions ...[4]uint64) []byte {
	b := make([]byte, 8+len(regions)*xeMemRegionSize)
	binary.NativeEndian.PutUint32(b, uint32(len(regions)))
	for i, r := range regions {
		e := b[8+i*xeMemRegionSize:]
		binary.NativeEndian.PutUint16(e, uint16(r[0]))
		binary.NativeEndian.PutUint16(e[2:], uint16(r[1]))
		binary.NativeEndian.PutUint64(e[8:], r[2])
		binary.NativeEndian.PutUint64(e[16:], r[3])
	}
	return b
}

// The installed broker may be GTT-only, leaving card VRAM unknown (#2185). The
// worker then reads the same kernel query itself: positive usage counts, a
// possibly censored zero or a malformed reply stays unknown, and an existing
// broker reading wins. No vendor runtime starts either way.
func TestXPUTelemetryKernelVRAMWhenBrokerOmitsIt(t *testing.T) {
	_, clock := fixture(t)
	// Live shape: one system region and one 31.89 GiB VRAM region in use.
	size, used := uint64(34242297856), uint64(244813824)
	live := xeRegionsReply([4]uint64{0, 0, 33511178240, 0}, [4]uint64{xeMemRegionClassVR, 1, size, used})
	gttOnly := fmt.Sprintf(`{"sampled_at":%q,"gtt_mm":{"0000:07:00.0":"  use_type: 1\n  use_tt: 1\n  size: 8180509\n  usage: 28672\n"}}`, clock.Now().Format(time.RFC3339Nano))
	withVRAM := fmt.Sprintf(`{"sampled_at":%q,"gtt_mm":{"0000:07:00.0":"usage: 0"},"vram":{"0000:07:00.0":{"total_bytes":8589934592,"used_bytes":2147483648}}}`, clock.Now().Format(time.RFC3339Nano))
	for _, c := range []struct {
		name, broker string
		reply        []byte
		known        bool
		want         float64
	}{
		{"gtt-only broker", gttOnly, live, true, float64(size-used)/(1<<30) - 0.25},
		{"censored zero", gttOnly, xeRegionsReply([4]uint64{xeMemRegionClassVR, 1, size, 0}), false, 0},
		{"no VRAM region", gttOnly, xeRegionsReply([4]uint64{0, 0, size, used}), false, 0},
		{"truncated", gttOnly, live[:len(live)-1], false, 0},
		{"broker VRAM", withVRAM, live, true, 5.75},
	} {
		t.Run(c.name, func(t *testing.T) {
			kernel := 0
			p := MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) {
				t.Fatal("telemetry initialized a vendor runtime")
				return nil, nil
			}, XPUFreeMemory: xpuTelemetryTestSource(clock, c.broker), XeFreeMemory: func(_ context.Context, pci string) (float64, error) {
				if pci != "0000:07:00.0" {
					t.Fatal("kernel query for the wrong card", pci)
				}
				kernel++
				return xeFreeVRAMGB(c.reply)
			}}
			devices := []MachineDevice{{Platform: "xpu", pciAddress: "0000:07:00.0", ReserveMemoryGB: 0.25}}
			(Worker{Clock: clock}).sampleXPUTelemetry(t.Context(), p, devices)
			got := devices[0].Telemetry.FreeGB
			if (got != nil) != c.known || got != nil && math.Abs(*got-c.want) > 1e-9 {
				t.Fatalf("card free VRAM = %v, want known=%v %.4f", got, c.known, c.want)
			}
			if c.broker == withVRAM && kernel != 0 {
				t.Fatal("kernel query replaced a broker reading")
			}
		})
	}
}

// Counter mapping does not depend on shell startup completing inside the real
// helper's 200ms collection deadline. Keep the production decoder and mapper;
// TestGPUHostMemoryTelemetryHelperBoundary covers executable I/O and its budget.
func xpuTelemetryTestSource(clock Clock, body string) func(context.Context) (map[string]float64, error) {
	return func(context.Context) (map[string]float64, error) {
		at := clock.Now()
		sample, err := decodeGPUTelemetrySnapshot([]byte(body), at, at)
		if err != nil {
			return nil, err
		}
		return xpuSnapshotFreeMemory(sample), nil
	}
}

func TestDeviceTelemetryAndHostProbeAdvertisement(t *testing.T) {
	q, _ := fixture(t)
	reply, err := q.Checkin(Checkin{Machine: sharedDiscreteMachine("xpu")})
	if err != nil {
		t.Fatal(err)
	}
	if reply.DeviceTelemetryProtocol != deviceTelemetryProtocol || reply.HostProbeProtocol != hostProbeProtocol || reply.DeviceRecoveryProtocol != deviceRecoveryProtocol || reply.DeviceHealthProtocol != deviceHealthProtocol {
		t.Fatal("check-in did not advertise independent health/telemetry protocols", reply)
	}
}

func TestDeviceTelemetryCurrentWireAndAge(t *testing.T) {
	_, clock := fixture(t)
	m := sharedDiscreteMachine("xpu")
	free, active := 4.0, 0.0
	sensor := &DeviceTelemetry{At: clock.Now(), FreeGB: &free, ActivePercent: &active}
	m.Devices[0].Telemetry = sensor
	m.Devices[0].Recovery = &DeviceRecovery{FaultEvent: "synthetic"}
	m.Devices[0].RecoveryBlocked = &DeviceHealthFailure{Code: "health_component_unknown", Component: "memory"}
	m.Devices[0].UnsupportedOnDevice = []string{"memory"}
	reports := []Checkin{}
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
		}
		reports = append(reports, c)
		_ = json.NewEncoder(rw).Encode(Reply{AttemptProtocol: keyedAttemptProtocol})
	}))
	defer server.Close()
	w := Worker{Clock: clock, Client: Client{URL: server.URL}}
	for range 2 {
		if _, err := w.checkin(context.Background(), Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		clock.Advance(deviceMemorySampleFreshFor)
	}
	for _, c := range reports {
		d := c.Machine.Devices[0]
		if d.Telemetry == nil || d.Recovery == nil || d.RecoveryBlocked == nil || len(d.UnsupportedOnDevice) != 1 {
			t.Fatal("current device evidence omitted")
		}
	}
	c := receivedDeviceTelemetry(reports[1], clock.Now().Add(24*time.Hour))
	if deviceTelemetryReady(c.Machine.Devices[0].Telemetry, clock.Now().Add(24*time.Hour)) {
		t.Fatal("send or receipt renewed stale activity")
	}
	if sensor.AgeSeconds != 0 {
		t.Fatal("wire changed local sensor")
	}
}
