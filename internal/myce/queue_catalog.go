package myce

import (
	"encoding/json"
	"reflect"
)

// One canonical encoding and detached value per retained catalog record. These
// records rarely change, but used to be encoded twice per heartbeat: once for
// undo and once for persistence. Compare against owned memory before reuse;
// neither a shared pointer nor an unchanged map identity proves unchanged data.
// All access is under q.mu. This never publishes state or authorizes a reader.
type catalogEncoding struct {
	value any
	data  []byte
}

func (q *Queue) encodeCatalog(key string, value any) (catalogEncoding, error) {
	if point, ok := value.(WorkObservation); ok {
		// Raw sampler counters are deliberately outside JSON. They can remain
		// on accepted peaks in memory; ignore them in the encoding comparison.
		point.MemoryPeak = catalogMemoryPeak(point.MemoryPeak)
		if prior := point.MemoryEscalation; prior != nil {
			copy := *prior
			copy.Peak = catalogMemoryPeak(prior.Peak)
			point.MemoryEscalation = &copy
		}
		value = point
	}
	if old, ok := q.records.encodings[key]; ok && reflect.DeepEqual(value, old.value) {
		return old, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return catalogEncoding{}, err
	}
	// Preserve in-memory timestamp representation (including monotonic data)
	// and empty containers for comparison. A JSON round trip would make an
	// unchanged live record compare unequal on every subsequent transaction.
	copy := copyObservationValue(reflect.ValueOf(value)).Interface()
	entry := catalogEncoding{value: copy, data: data}
	if q.records.encodings == nil {
		q.records.encodings = map[string]catalogEncoding{}
	}
	q.records.encodings[key] = entry
	return entry, nil
}

func catalogMemoryPeak(peak *AttemptMemoryPeak) *AttemptMemoryPeak {
	if peak == nil || peak.nonreclaimableSampleGB == nil && peak.pressureTotalUS == 0 && peak.pressureAvg10 == 0 {
		return peak
	}
	copy := *peak
	copy.nonreclaimableSampleGB, copy.pressureTotalUS, copy.pressureAvg10 = nil, 0, 0
	return &copy
}

// Undo borrows only the cache's detached values, never the live catalog. A
// rollback transfers their ownership to state and discards the encoding cache.
func (q *Queue) snapshotCatalogs() (state, error) {
	var out state
	if q.s.Extensions != nil {
		out.Extensions = make(map[string]Extension, len(q.s.Extensions))
		for id, value := range q.s.Extensions {
			entry, err := q.encodeCatalog("extensions/"+id, value)
			if err != nil {
				return state{}, err
			}
			out.Extensions[id] = entry.value.(Extension)
		}
	}
	if q.s.WorkObservations != nil {
		out.WorkObservations = make(map[string]WorkObservation, len(q.s.WorkObservations))
		for id, value := range q.s.WorkObservations {
			entry, err := q.encodeCatalog("work_observations/"+id, value)
			if err != nil {
				return state{}, err
			}
			out.WorkObservations[id] = entry.value.(WorkObservation)
		}
	}
	if q.s.StorageObservations != nil {
		out.StorageObservations = make(map[string]StorageObservation, len(q.s.StorageObservations))
		for id, value := range q.s.StorageObservations {
			entry, err := q.encodeCatalog("storage_observations/"+id, value)
			if err != nil {
				return state{}, err
			}
			out.StorageObservations[id] = entry.value.(StorageObservation)
		}
	}
	return out, nil
}

type recordUndo struct {
	inventories map[string]*toolInventory
	base        []byte
	catalogs    state
}
