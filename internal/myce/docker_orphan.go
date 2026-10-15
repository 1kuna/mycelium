package myce

import (
	"strconv"
	"sync"
	"time"
)

const dockerOrphanEvidenceNote = "exit code/stderr not retained: orphan without attempt record"

// Process lifetime, keyed by daemon and exact container ID, not by cleanup
// object or attempt label. Reconstructing cleanup must never loop graceful stop.
var dockerOrphanStops sync.Map // map[string]time.Time (original fallback deadline)

func (d *dockerAttempt) stopOrphan(id string, kill bool) ([]byte, error) {
	now := d.clock.Now()
	key := d.state.Endpoint + "\x00" + id
	if deadline, ok := dockerOrphanStops.Load(key); ok {
		if kill || !now.Before(deadline.(time.Time)) {
			return d.call("kill", "--signal=KILL", id)
		}
		return nil, nil
	}
	if kill {
		return d.call("kill", "--signal=KILL", id)
	}
	grace := time.Duration(0)
	if d.state.StopAt != nil {
		grace = max(0, d.state.StopAt.Sub(now))
	}
	// Docker accepts whole seconds. Round up so its own KILL is never early.
	seconds := int64(grace / time.Second)
	if grace%time.Second != 0 {
		seconds++
	}
	grace = time.Duration(seconds) * time.Second
	if _, loaded := dockerOrphanStops.LoadOrStore(key, now.Add(grace)); loaded {
		return nil, nil
	}
	// Docker sends TERM once and KILL at its timeout. On any error, subsequent
	// observations wait for the original grace, then retry only idempotent KILL.
	return d.callWithTimeout(grace+d.timeout, "stop", "--time", strconv.FormatInt(seconds, 10), id)
}
