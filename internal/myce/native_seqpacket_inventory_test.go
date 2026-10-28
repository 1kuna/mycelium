package myce

import (
	"strings"
	"testing"
)

func TestNativeSeqpacketStartupInventory(t *testing.T) {
	data := []byte("Num RefCount Protocol Flags Type St Inode Path\n" +
		"0000: 00000002 00000000 00010000 0005 01 10 /run/udev/control\n" +
		"0000: 00000002 00000000 00010000 0005 01 11 @cuda-uvmfd-4026531836-123@\n" +
		"0000: 00000002 00000000 00010000 0005 01 12 /tmp/new control.sock\n" +
		"0000: 00000002 00000000 00010000 0005 01 13 @new-service\n" +
		"0000: 00000002 00000000 00000000 0005 03 14 @connected\n" +
		"0000: 00000002 00000000 00010000 0001 01 15 /run/docker.sock\n")
	endpoints, err := nativeSeqpacketEndpoints(data)
	if err != nil || len(endpoints) != 4 {
		t.Fatalf("inventory=%+v error=%v", endpoints, err)
	}
	unexpected := nativeUnexpectedSeqpacket(endpoints, func(name string) (uint32, bool) {
		switch name {
		case "/run/udev/control":
			return 0, true
		case "/tmp/new control.sock":
			return 1000, true
		default:
			return 0, false
		}
	})
	if len(unexpected) != 2 || unexpected[0].Name != "/tmp/new control.sock" || unexpected[1].Name != "@new-service" {
		t.Fatalf("unexpected listeners/unknown ownership were not exposed: %+v", unexpected)
	}
	if nativeCUDASeqpacketName("/tmp/cuda-uvmfd-1-2") || nativeCUDASeqpacketName("@cuda-uvmfd-not-a-driver") {
		t.Fatal("unrelated endpoint was classified as the CUDA naming convention")
	}
	if _, err := nativeSeqpacketEndpoints([]byte("incomplete socket dump\n")); err == nil {
		t.Fatal("incomplete inventory was accepted")
	}
	if _, err := nativeSeqpacketEndpoints(nil); err == nil {
		t.Fatal("empty inventory was accepted")
	}
}

func TestNativeIsolationStatusSeqpacketResidual(t *testing.T) {
	m := Machine{Capabilities: []string{"native_isolation_landlock"}, NativeIsolationWarning: "1 unexpected listening SEQPACKET endpoint(s)"}
	status := NativeIsolationStatus(m)
	for _, want := range []string{"same-UID pathname SEQPACKET endpoints reachable (partial)", "WARNING: " + m.NativeIsolationWarning, "general egress unavailable"} {
		if !strings.Contains(status, want) {
			t.Fatalf("status missing %q: %s", want, status)
		}
	}
	m.Capabilities, m.NativeIsolationWarning = []string{"native_isolation_namespace"}, ""
	if strings.Contains(NativeIsolationStatus(m), "SEQPACKET") {
		t.Fatal("fallback exception attributed to namespace mode")
	}
}
