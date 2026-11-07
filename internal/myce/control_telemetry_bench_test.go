package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// This file also compiles on the pre-telemetry baseline. Use fixed iteration
// counts below the existing telemetry envelope so overflow is not a shortcut.
// Both sides include the existing every-acquisition pressure sensor.
func controlBenchmarkQueue(b *testing.B) *Queue {
	b.Helper()
	clock := RealClock{}
	q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
	if err != nil {
		b.Fatal(err)
	}
	q.checkpointProbe = func() bool { return true }
	q.requestBudgets.memory = func(context.Context) (int64, error) { return 1 << 30, nil }
	b.Cleanup(func() {
		if err := q.Close(); err != nil {
			b.Error(err)
		}
	})
	return q
}
func BenchmarkControlTelemetryLock(b *testing.B) {
	q := controlBenchmarkQueue(b)
	_ = newObservationLimit(q.clock, q)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		unlock := requestLock(context.Background(), &q.mu)
		unlock()
	}
}
func BenchmarkControlTelemetryRequest(b *testing.B) {
	q := controlBenchmarkQueue(b)
	h := API{q, "benchmark-only"}.Handler()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		request := httptest.NewRequest("GET", "/api/ping", nil)
		request.Header.Set("Authorization", "Bearer benchmark-only")
		request.RemoteAddr = "192.0.2.1:1234"
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		if response.Code != 200 {
			b.Fatal(response.Code)
		}
	}
}
func BenchmarkControlTelemetryCheckin(b *testing.B) {
	q := controlBenchmarkQueue(b)
	h := API{q, "benchmark-only"}.Handler()
	body, _ := json.Marshal(Checkin{Machine: machine("benchmark-worker", "fake")})
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		request := httptest.NewRequest("POST", "/api/workers/checkin", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer benchmark-only")
		request.Header.Set("X-Myce-Start-Gate", startGateProtocol)
		request.RemoteAddr = "192.0.2.1:1234"
		response := httptest.NewRecorder()
		h.ServeHTTP(response, request)
		if response.Code != 200 {
			b.Fatal(response.Code, response.Body.String())
		}
		// Account for amortized durable maintenance, not just the in-memory append.
		// Synthetic cadence: one maintenance transaction per 100 requests.
		if (i+1)%100 == 0 {
			if err := q.Tick(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	// Optional endpoint keeps this identical benchmark source baseline-compatible.
	request := httptest.NewRequest("GET", "/api/control", nil)
	request.Header.Set("Authorization", "Bearer benchmark-only")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	if response.Code == 200 {
		var page struct {
			Telemetry struct {
				Pending                int    `json:"pending"`
				StorageDropped         uint64 `json:"storage_dropped"`
				DiagnosticTransactions uint64 `json:"diagnostic_transactions"`
				SerializedBytes        uint64 `json:"serialized_bytes"`
			} `json:"telemetry"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(page.Telemetry.DiagnosticTransactions)/float64(b.N), "diag-tx/op")
		b.ReportMetric(float64(page.Telemetry.SerializedBytes)/float64(b.N), "diag-json-B/op")
		b.ReportMetric(float64(page.Telemetry.StorageDropped)/float64(b.N), "diag-drops/op")
		b.ReportMetric(float64(page.Telemetry.Pending), "pending-events")
	}

}
