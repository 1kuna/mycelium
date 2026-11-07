package myce

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"
)

// FleetQueuePreservation proves the normalized committed queue records, not
// remote file bytes, logs/usage tables, process survival or downgrade safety.
type FleetQueuePreservation struct {
	State      string    `json:"state"`
	Revision   int       `json:"revision"`
	Session    string    `json:"session"`
	SHA256     string    `json:"sha256,omitempty"`
	Records    int       `json:"records,omitempty"`
	Bytes      int64     `json:"bytes,omitempty"`
	VerifiedAt time.Time `json:"verified_at,omitempty"`
}

type fleetQueueSeal struct {
	Protocol  int    `json:"protocol"`
	Operation string `json:"operation"`
	FleetQueuePreservation
	records map[string][]byte
}

// Called under q.mu, after the shutdown fence. Committed slices are immutable;
// owning the map is enough to move all hashing outside the queue mutex.
func (q *Queue) fleetQueueSeal() *fleetQueueSeal {
	u, h := q.s.FleetUpgrade, q.fleetActiveHost()
	if q.fleetService == nil || q.preservationPath == "" || u == nil || h == nil || h.Preservation == nil || h.Preservation.State != "required" {
		return nil
	}
	return &fleetQueueSeal{Protocol: 1, Operation: u.Operation, FleetQueuePreservation: *h.Preservation, records: maps.Clone(q.persisted)}
}

func fleetRecordDigest(records map[string][]byte) (string, int64) {
	h := sha256.New()
	// Length framing separates keys and values even when they contain arbitrary
	// JSON strings. Sorting makes the proof independent of map/SQLite row order.
	var size [8]byte
	var total int64
	for _, key := range slices.Sorted(maps.Keys(records)) {
		for _, data := range [][]byte{[]byte(key), records[key]} {
			binary.BigEndian.PutUint64(size[:], uint64(len(data)))
			h.Write(size[:])
			h.Write(data)
			total += int64(len(data))
		}
	}
	return hex.EncodeToString(h.Sum(nil)), total
}

func (q *Queue) writeFleetQueueSeal(seal *fleetQueueSeal) error {
	if len(seal.records) == 0 {
		return errors.New("fleet_preservation_records_missing")
	}
	seal.SHA256, seal.Bytes = fleetRecordDigest(seal.records)
	seal.Records = len(seal.records)
	data, err := json.Marshal(seal)
	if err == nil {
		err = writeInstallFile(q.preservationPath, data, 0600, installFiles{}, nil)
	}
	if err != nil {
		return errors.New("fleet_preservation_seal_failed")
	}
	return nil
}

// Runs before inventory resets, recovery/retention or any startup transaction.
// Missing/mismatched evidence stops startup; it never restores an older DB.
func (q *Queue) verifyFleetQueuePreservation() error {
	u, h := q.s.FleetUpgrade, q.fleetActiveHost()
	if u == nil || h == nil || h.Preservation == nil || h.Preservation.State != "required" {
		return nil // Older/unmanaged handoffs remain explicitly unverified.
	}
	if fleetPrivateFile(q.preservationPath, true) != nil {
		return errors.New("fleet_preservation_seal_missing")
	}
	data, err := os.ReadFile(q.preservationPath)
	var seal fleetQueueSeal
	if err != nil || json.Unmarshal(data, &seal) != nil || seal.Protocol != 1 || seal.State != "required" || seal.Operation != u.Operation || seal.Revision != h.Preservation.Revision || seal.Session != h.Preservation.Session || !validPin(seal.SHA256) {
		return errors.New("fleet_preservation_seal_invalid")
	}
	digest, size := fleetRecordDigest(q.persisted)
	if digest != seal.SHA256 || size != seal.Bytes || len(q.persisted) != seal.Records {
		return errors.New("fleet_preservation_records_changed")
	}
	// Raw SQLite equality cannot prove the new binary understands its fields.
	// Require every retained JSON value to survive decode/encode; additive new
	// fields are allowed. This includes terminal jobs, keys and custody, not a
	// filtered public status view. No remote I/O or hashing occurs under q.mu.
	readJSON := func(data []byte, out *any) error {
		d := json.NewDecoder(bytes.NewReader(data))
		d.UseNumber() // Preserve exact integer identities/counters beyond 2^53.
		return d.Decode(out)
	}
	// Check one normalized record at a time rather than serializing and
	// expanding a second whole-queue snapshot alongside the live state.
	for key, raw := range q.persisted {
		var value any
		group, id, _ := strings.Cut(key, "/")
		switch group {
		case "state":
			value = normalizedQueueBase(q.s)
		case "jobs":
			value = q.s.Jobs[id]
		case "submissions":
			value = q.s.Submissions[id]
		case "extensions":
			value = q.s.Extensions[id]
		case "work_observations":
			value = q.s.WorkObservations[id]
		case "storage_observations":
			value = q.s.StorageObservations[id]
		default:
			return errors.New("fleet_preservation_decode_loss")
		}
		decoded, encodeErr := json.Marshal(value)
		var before, after any
		if encodeErr != nil || readJSON(raw, &before) != nil || readJSON(decoded, &after) != nil || !preservesJSON(before, after) {
			return errors.New("fleet_preservation_decode_loss")
		}
	}
	h.Preservation = &FleetQueuePreservation{State: "verified", Revision: seal.Revision, Session: seal.Session, SHA256: seal.SHA256, Records: seal.Records, Bytes: seal.Bytes, VerifiedAt: q.clock.Now()}
	return nil
}

func preservesJSON(before, after any) bool {
	switch b := before.(type) {
	case map[string]any:
		a, ok := after.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range b {
			v, exists := a[key]
			if !exists || !preservesJSON(value, v) {
				return false
			}
		}
		return true
	case []any:
		a, ok := after.([]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for i := range b {
			if !preservesJSON(b[i], a[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(before, after)
	}
}
