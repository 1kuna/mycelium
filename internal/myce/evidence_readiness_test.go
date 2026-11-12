package myce

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestReadinessProgressUsesSharedDiagnosticCapacity(t *testing.T) {
	for _, kind := range []string{"model"} {
		for _, available := range []uint64{4096, 1 << 20} {
			t.Run(fmt.Sprintf("%s/%d", kind, available), func(t *testing.T) {
				var pool diagnosticReservations
				capacity := RetentionCapacity{MemoryAvailableBytes: available * diagnosticExpansion, DiskAvailableBytes: available * diagnosticExpansion}
				reserve := func(n uint64) (int64, func()) {
					grant, release := pool.reserve(capacity, 0, n, diagnosticExpansion)
					return int64(grant), release
				}
				clock := &manualWorkerClock{now: time.Unix(1, 0)}
				history := readinessHistory{}
				history.Load.observe(time.Millisecond)
				progress := newReadinessProgress(clock, history)
				var sink bytes.Buffer
				var output io.Writer
				var closeOutput func()
				line := diagnosticLine{tail: diagnosticTail{reserve: reserve}}
				long, short := "", ""
				padding := strings.Repeat("x", 80<<10) // Beyond the retired 64 KiB cutoff.
				w := &modelReadinessOutput{output: &sink, progress: progress, line: line}
				output, closeOutput = w, w.close
				long = fmt.Sprintf("MYCE_LOAD_PROGRESS {\"padding\":%q,\"stage\":1,\"completed\":1,\"total\":2}\n", padding)
				short = "MYCE_LOAD_PROGRESS {\"stage\":1,\"completed\":2,\"total\":2}\n"
				t.Cleanup(closeOutput)
				clock.Advance(time.Hour)
				// Fragmented records share the same pool as another capture.
				_, releaseSibling := reserve(128)
				_, _ = io.WriteString(output, long[:len(long)/2])
				_, _ = io.WriteString(output, long[len(long)/2:])
				releaseSibling()
				_, stalled := progress.state()
				if stalled != (available == 4096) {
					t.Fatal("progress did not follow measured parser capacity", stalled)
				}
				_, _ = io.WriteString(output, short)
				if _, stalled := progress.state(); stalled {
					t.Fatal("a refused long line prevented later progress")
				}
				_, _ = io.WriteString(output, "unfinished")
				closeOutput()
				if pool.reserved != 0 || sink.String() != long+short+"unfinished" {
					t.Fatal("parser retained reservations or changed captured output", pool.reserved)
				}
			})
		}
	}
}
