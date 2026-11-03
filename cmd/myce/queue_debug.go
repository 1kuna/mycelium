package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

// A literal loopback address avoids DNS changes or wildcard binding. This mux
// is deliberately independent of both authenticated fleet API listeners.
func queueDebugListener(address string) (net.Listener, *http.Server, error) {
	if address == "" {
		return nil, nil, nil
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, nil, fmt.Errorf("debug-listen: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return nil, nil, fmt.Errorf("debug-listen requires a literal loopback IP address")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /debug/runtime", func(w http.ResponseWriter, r *http.Request) {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"gc_pause_total_ns": stats.PauseTotalNs,
			"gc_cycles":         stats.NumGC,
			"heap_alloc_bytes":  stats.HeapAlloc,
			"heap_inuse_bytes":  stats.HeapInuse,
			"heap_sys_bytes":    stats.HeapSys,
			"goroutines":        runtime.NumGoroutine(),
		})
	})
	// Header timeout bounds incomplete requests; no write timeout truncates a
	// requested CPU profile. Server.Close below interrupts profiling on shutdown.
	return listener, &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}, nil
}
