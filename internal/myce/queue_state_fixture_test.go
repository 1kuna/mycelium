package myce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// readQueueState is shared by persistence tests and the adjacent-release fixture.
func readQueueState(db queueRecordReader, data []byte) ([]byte, error) {
	if !bytes.Equal(data, normalizedQueueMarker) {
		return nil, fmt.Errorf("unsupported queue persistence format; current and previous releases require normalized-state-v3")
	}
	records, err := readQueueRecords(db)
	if err != nil {
		return nil, err
	}
	var base map[string]json.RawMessage
	groups := map[string]map[string]json.RawMessage{"jobs": {}, "submissions": {}, "extensions": {}, "work_observations": {}, "storage_observations": {}, "external_memory": {}, "health_events": {}, "machines": {}, "file_input_claims": {}}
	for key, value := range records {
		if key == "state" {
			if err = json.Unmarshal(value, &base); err != nil {
				return nil, err
			}
			continue
		}
		group, id, ok := strings.Cut(key, "/")
		if !ok || groups[group] == nil {
			return nil, fmt.Errorf("unknown queue state fragment")
		}
		groups[group][id] = value
	}
	if base == nil {
		return nil, fmt.Errorf("queue state fragment missing")
	}
	if len(groups["file_input_claims"]) > 0 && base["file_input_claims"] != nil {
		return nil, fmt.Errorf("mixed queue input claim formats")
	}
	if events := groups["health_events"]; len(events) > 0 {
		var health map[string]json.RawMessage
		if err := json.Unmarshal(base["health_alerts"], &health); err != nil {
			return nil, err
		}
		ordered := make([]json.RawMessage, len(events))
		for key, value := range events {
			index, e := strconv.Atoi(key)
			if e != nil || index < 0 || index >= len(ordered) {
				return nil, fmt.Errorf("invalid health evidence index")
			}
			ordered[index] = value
		}
		health["events"], err = json.Marshal(ordered)
		if err != nil {
			return nil, err
		}
		base["health_alerts"], err = json.Marshal(health)
		if err != nil {
			return nil, err
		}
	}
	for id, data := range groups["jobs"] {
		if groups["jobs"][id], err = expandJobRecord(data); err != nil {
			return nil, err
		}
	}
	// Live keyed answers reference the same committed job instead of duplicating
	// its payload on every check-in. Removed jobs keep their full retained answer.
	for id, data := range groups["submissions"] {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, err
		}
		if record == nil || len(record["job"]) > 0 {
			continue
		}
		var jobID string
		if err := json.Unmarshal(record["job_id"], &jobID); err != nil {
			continue
		}
		if job := groups["jobs"][jobID]; job != nil {
			record["job"] = job
			encoded, err := json.Marshal(record)
			if err != nil {
				return nil, err
			}
			groups["submissions"][id] = encoded
		}
	}
	delete(groups, "health_events")
	for key, group := range groups {
		if len(group) == 0 && key != "jobs" && key != "machines" {
			continue
		}
		base[key], err = json.Marshal(group)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(base)
}

// Reassemble logical records for fixture assertions.
func readQueueRecords(db queueRecordReader, only ...string) (map[string][]byte, error) {
	return readQueueRecordsVisit(db, nil, only...)
}
