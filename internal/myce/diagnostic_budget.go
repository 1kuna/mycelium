package myce

import (
	"bytes"
	"context"
	"encoding/xml"
	"math"
	"runtime"
	"sync"
	"unsafe"
)

// boundedTail keeps the newest limit bytes, including one arbitrarily long line.
type boundedTail struct {
	data  []byte
	limit int
}

func (b *boundedTail) Write(p []byte) (int, error) {
	n := len(p)
	if b.limit <= 0 {
		return n, nil
	}
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
	} else {
		if extra := len(b.data) + len(p) - b.limit; extra > 0 {
			b.data = append(b.data[:0], b.data[extra:]...)
		}
		b.data = append(b.data, p...)
	}
	return n, nil
}

// Streams reserve bytes as they arrive. Reserving all measured headroom for an
// idle event subscription would starve the same attempt's exit/stderr capture.
type diagnosticTail struct {
	boundedTail
	dir      string
	reserve  func(uint64) (int64, func())
	releases []func()
}

func (b *diagnosticTail) reserveBytes(n uint64) (int64, func()) {
	if b.reserve != nil {
		return b.reserve(n)
	}
	dir := b.dir
	if dir == "" {
		dir = "." // Transient observers without a spool use their working volume.
	}
	return reserveDiagnosticBytes(dir, n)
}

func (b *diagnosticTail) Write(p []byte) (int, error) {
	needed := len(b.data) + len(p) - b.limit
	if needed > 0 {
		grant, release := b.reserveBytes(uint64(needed))
		if grant > 0 {
			b.releases = append(b.releases, release)
		} else {
			release()
		}
		b.limit += int(grant)
	}
	return b.boundedTail.Write(p)
}

// A progress parser retains only its incomplete line. Exhaustion suppresses that
// optional observation until the delimiter; output still reaches its sink. A
// truncated suffix must never masquerade as a complete progress record.
type diagnosticLine struct {
	tail     diagnosticTail
	dropping bool
}

func (l *diagnosticLine) clear() {
	l.tail.data = nil
	l.tail.limit = 0
	l.tail.release()
}

func (l *diagnosticLine) write(p []byte, consume func([]byte)) {
	for len(p) > 0 {
		end := bytes.IndexAny(p, "\r\n")
		n := len(p)
		if end >= 0 {
			n = end
		}
		if !l.dropping {
			want := len(l.tail.data) + n
			_, _ = l.tail.Write(p[:n])
			if len(l.tail.data) != want {
				l.clear()
				l.dropping = true
			}
		}
		if end < 0 {
			return
		}
		if !l.dropping {
			consume(l.tail.data)
		}
		l.clear()
		l.dropping = false
		p = p[end+1:]
	}
}

func (b *diagnosticTail) release() {
	for _, release := range b.releases {
		release()
	}
	b.releases = nil
}

// A process-wide reservation includes concurrent progress and failure
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
