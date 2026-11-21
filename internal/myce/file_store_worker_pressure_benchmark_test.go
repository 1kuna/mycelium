package myce

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// Replay serialized minute-cadence hygiene polls with periodic long writers.
// Run with -run '^$' -bench '^BenchmarkWorkerDiskDemandRoute$' -benchtime=128x.
func BenchmarkWorkerDiskDemandRoute(b *testing.B) {
	q, clock := fixture(b)
	const reserve = 64 << 10
	const total = uint64(10 << 30)
	const volume = "filesystem"
	q.diskHost = "store"
	q.fileStoreProductionReserve = reserve
	if err := q.ConfigureFileStore(b.TempDir(), 4<<20, false); err != nil {
		b.Fatal(err)
	}
	q.files.diskProbe = func(string) (DiskReading, error) {
		return DiskReading{Volume: volume, TotalBytes: total, FreeBytes: diskFreeFloor(total) + reserve + 4096}, nil
	}
	if err := q.update(func() error {
		q.s.Machines["worker"] = &Machine{ID: "worker", HostID: "store", Session: "session",
			DiskVolumes: []DiskVolume{{Roots: []string{"data"}, TotalBytes: total, FreeBytes: diskFreeFloor(total)}}}
		for i := range 32 {
			id := fmt.Sprintf("queued-%d", i)
			j := &Job{ID: id, State: "queued", Spec: asyncDeliverySpec(), Queued: clock.Now()}
			j.PendingOutputRefs = FileManifest{"out.txt": {SHA256: fmt.Sprintf("%064x", i+1), SizeBytes: 1024}}
			q.putJob(id, j)
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "ordinary", Class: "experiment"})
	if _, err := q.preflightAdmission(ctx, q.files, FileManifest{"input": {SHA256: strings.Repeat("a", 64), SizeBytes: 128 << 10}}); !fileStoreSpaceRefusal(err) {
		b.Fatalf("expected capacity refusal, got %v", err)
	}
	refusal := *q.files.pressure.Load()
	var elapsed, wait, hold []time.Duration
	q.observeRequest = func(route string, d, w, h, _ time.Duration) {
		if route == "POST /api/workers/disk-demand" {
			elapsed, wait, hold = append(elapsed, d), append(wait, w), append(hold, h)
		}
	}
	handler := (API{Queue: q, Token: "test"}).Handler()
	path := "/api/workers/disk-demand?compiler_volume=" + diskVolumeID(q.diskHost, volume)
	want := fmt.Sprintf(`"shortfall_bytes":%d`, (128<<10)+(32<<10)-4096)
	// Warm the committed decode and the HTTP handler before timing.
	poll := func() {
		demand := refusal
		demand.at = clock.Now()
		q.files.pressure.Store(&demand)
		req := httptest.NewRequest("POST", path, strings.NewReader(`{"machine":"worker","session":"session"}`))
		req.Header.Set("Authorization", "Bearer test")
		req.Header.Set("X-Myce-Start-Gate", startGateProtocol)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), want) {
			b.Fatalf("unexpected hygiene response: %d %s", w.Code, w.Body.String())
		}
	}
	poll()
	elapsed, wait, hold = elapsed[:0], wait[:0], hold[:0]
	b.ResetTimer()
	for i := range b.N {
		clock.Advance(time.Minute)
		// A long writer every 32 samples gives the p99 repeatable overlap.
		// Wall time is used only to model writer occupancy and measure the route.
		if i%32 == 0 {
			q.mu.Lock()
			released := make(chan struct{})
			go func() {
				<-time.After(500 * time.Millisecond)
				q.mu.Unlock()
				close(released)
			}()
			poll()
			<-released
		} else {
			poll()
		}
	}
	b.StopTimer()
	for unit, samples := range map[string][]time.Duration{"route-p99-ms": elapsed, "wait-p99-ms": wait, "hold-p99-ms": hold} {
		slices.Sort(samples)
		n := len(samples) - len(samples)/100 - 1
		b.ReportMetric(float64(samples[n])/float64(time.Millisecond), unit)
	}
}
