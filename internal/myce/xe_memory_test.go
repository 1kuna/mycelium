package myce

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"
)

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
	if p := LocalMachineProbe(); p.XeFreeMemory == nil || reflect.ValueOf(p.XeFreeMemory).Pointer() != reflect.ValueOf(nativeXeFreeMemory).Pointer() {
		t.Fatal("production probe does not read Xe kernel VRAM")
	}
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
