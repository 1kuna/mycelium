package myce

import (
	"context"
	"encoding/xml"
	"math"
	"runtime"
	"sync"
	"unsafe"
)

// A process-wide reservation includes concurrent progress, Docker and failure
// captures. Available RAM excludes live allocations; outstanding grants cover
// their not-yet-allocated peak too. Unknown measurements withhold optional copies.
type diagnosticReservations struct {
	mu       sync.Mutex
	reserved uint64
}

var diagnosticMemory diagnosticReservations

// One input byte cannot introduce more than one of each decoded record/token.
// Account for raw/encoded strings, parser tokens, structured identifiers and
// both backing arrays during growth. This is a conservative representation
// bound, deliberately unrelated to a host's size or number of workers.
var diagnosticExpansion = uint64(unsafe.Sizeof(FailedTest{})+unsafe.Sizeof(ProgressReport{})+
	unsafe.Sizeof(xml.StartElement{})+unsafe.Sizeof(xml.Attr{})+unsafe.Sizeof(xml.Name{})+
	unsafe.Sizeof([]byte{})+unsafe.Sizeof(string(""))) * 4

func (p *diagnosticReservations) reserve(capacity RetentionCapacity, heap, requested, expansion uint64) (uint64, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	available := evidenceBudget(capacity, 0, heap)
	available -= min(available, p.reserved)
	grant := min(requested, available/max(1, expansion))
	bytes := grant * max(1, expansion)
	p.reserved += bytes
	var once sync.Once
	return grant, func() { once.Do(func() { p.mu.Lock(); p.reserved -= bytes; p.mu.Unlock() }) }
}
func reserveDiagnosticBytes(dir string, requested uint64) (int64, func()) {
	capacity, err := queueRetentionCapacity(context.Background(), dir)
	if err != nil {
		return 0, func() {}
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	grant, release := diagnosticMemory.reserve(capacity, memory.HeapAlloc, min(uint64(math.MaxInt64-1), requested), diagnosticExpansion)
	return int64(grant), release
}
