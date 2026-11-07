package myce

import (
	"encoding/binary"
	"testing"
)

func TestXePMUCompleteAndUnavailable(t *testing.T) {
	b := make([]byte, 40)
	put := func(i int, n uint64) { binary.NativeEndian.PutUint64(b[i*8:], n) }
	put(0, 2)
	put(1, 200)
	put(2, 200)
	put(3, 40)
	put(4, 100)
	if n, err := xeEngineActivity(b); err != nil || n != 40 {
		t.Fatal("valid paired engine counters lost", n, err)
	}
	put(2, 100)
	if _, err := xeEngineActivity(b); err == nil {
		t.Fatal("multiplexed counters established activity headroom")
	}
	put(2, 200)
	put(4, 0)
	if _, err := xeEngineActivity(b); err == nil {
		t.Fatal("nonadvancing total counter became idle evidence")
	}
	// Kernel inventory contains a copy engine and a compute engine on a
	// different GT/instance. Neither may silently disappear from the sample.
	engines := make([]byte, 8+2*32)
	binary.NativeEndian.PutUint32(engines, 2)
	binary.NativeEndian.PutUint16(engines[8:], 1)
	binary.NativeEndian.PutUint16(engines[40:], 4)
	binary.NativeEndian.PutUint16(engines[42:], 2)
	binary.NativeEndian.PutUint16(engines[44:], 1)
	got, err := parseXeEngines(engines)
	if err != nil || len(got) != 2 || got[1] != (xeEngine{4, 2, 1}) {
		t.Fatal("engine census lost device coverage", got, err)
	}
	if _, err := parseXeEngines(engines[:len(engines)-1]); err == nil {
		t.Fatal("partial engine census became a full card measurement")
	}
}
