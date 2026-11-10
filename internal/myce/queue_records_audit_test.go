package myce

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// Allocation probes measure the production path, excluding the full test diff.
var recordAuditExempt sync.Map

// Enable for targeted scheduling/reopen tests. This full diff is test-only;
// production's journal never discovers changes by serializing untouched state.
func init() {
	if os.Getenv("MYCELIUM_RECORD_AUDIT") != "1" {
		return
	}
	auditQueueRecords = func(q *Queue) func() error {
		if _, exempt := recordAuditExempt.Load(q); exempt {
			return nil
		}
		before := map[string][]byte{}
		put := func(key string, value any) { before[key], _ = json.Marshal(value) }
		for id, j := range q.s.Jobs {
			copy := *j
			if q.startupPending(j) || q.startCapacityPaused(j) {
				copy.Reason = "recomputing admission diagnostics"
			}
			put("jobs/"+id, &copy)
		}
		for id, r := range q.s.Submissions {
			put("submissions/"+id, auditSubmission(q, r))
		}
		for id, m := range q.s.Machines {
			put("machines/"+id, m)
		}
		for _, group := range []string{"extensions/", "work_observations/", "storage_observations/", "external_memory/"} {
			var ids []string
			switch group {
			case "extensions/":
				for id := range q.s.Extensions {
					ids = append(ids, id)
				}
			case "work_observations/":
				for id := range q.s.WorkObservations {
					ids = append(ids, id)
				}
			case "storage_observations/":
				for id := range q.s.StorageObservations {
					ids = append(ids, id)
				}
			case "external_memory/":
				for id := range q.s.ExternalMemory {
					ids = append(ids, id)
				}
			}
			for _, id := range ids {
				v, _ := q.catalogValue(group + id)
				if p, ok := v.(WorkObservation); ok {
					v = durableWorkObservation(p)
				}
				put(group+id, v)
			}
		}
		return func() error { return checkQueueRecordsAgainst(q, before, true) }
	}
}

func checkQueueRecords(q *Queue) error {
	return checkQueueRecordsAgainst(q, q.persisted, q.machineRecords)
}

func checkQueueRecordsAgainst(q *Queue, beforeRecords map[string][]byte, machines bool) error {
	if beforeRecords == nil {
		return nil
	}
	equal := func(old []byte, value any) bool {
		data, err := json.Marshal(value)
		if err != nil {
			return false
		}
		var before, after any
		if json.Unmarshal(old, &before) != nil || json.Unmarshal(data, &after) != nil {
			return false
		}
		return reflect.DeepEqual(before, after)
	}
	if machines {
		for id, m := range q.s.Machines {
			if _, dirty := q.records.machines[id]; dirty {
				continue
			}
			// These fields are recomputed by refreshHostMemory, never decisions.
			data, _ := json.Marshal(m)
			var before, after map[string]any
			_ = json.Unmarshal(beforeRecords["machines/"+id], &before)
			_ = json.Unmarshal(data, &after)
			for _, key := range []string{"host_memory_derivation", "host_memory_policy_mismatch", "memory_allowance_bytes", "memory_allowance_qualification", "effective_host_memory_budget_gb", "host_memory_in_use_gb", "free_host_memory_gb", "tool_inventory", "effective_cpu_cores", "cpu_in_use_cores", "free_cpu_cores", "cpu_capacity_reason", "shared_host_reason"} {
				delete(before, key)
				delete(after, key)
			}
			if !reflect.DeepEqual(before, after) {
				fields := []string{}
				for key, value := range after {
					if !reflect.DeepEqual(before[key], value) {
						fields = append(fields, key)
					}
				}
				for key := range before {
					if _, ok := after[key]; !ok {
						fields = append(fields, key)
					}
				}
				return fmt.Errorf("machine changed without touchMachine: %s (%v)", id, fields)
			}
		}
		for key := range beforeRecords {
			if id, ok := strings.CutPrefix(key, "machines/"); ok && q.s.Machines[id] == nil {
				if _, dirty := q.records.machines[id]; !dirty {
					return fmt.Errorf("machine removed without touchMachine: %s", id)
				}
			}
		}
	}
	for id, r := range q.s.Submissions {
		if _, dirty := q.records.submissions[id]; dirty {
			continue
		}
		if !equal(beforeRecords["submissions/"+id], auditSubmission(q, r)) {
			return fmt.Errorf("submission changed without touchSubmission: %s", id)
		}
	}
	for key := range beforeRecords {
		if id, ok := strings.CutPrefix(key, "submissions/"); ok {
			if _, exists := q.s.Submissions[id]; exists {
				continue
			}
			if _, dirty := q.records.submissions[id]; !dirty {
				return fmt.Errorf("submission removed without touchSubmission: %s", id)
			}
		}
	}
	for id, j := range q.s.Jobs {
		if _, dirty := q.records.jobs[id]; dirty {
			continue
		}
		copy := *j
		if q.startupPending(j) || q.startCapacityPaused(j) {
			copy.Reason = "recomputing admission diagnostics"
		}
		if !equal(beforeRecords["jobs/"+id], &copy) {
			return fmt.Errorf("job decision changed without touchJob: %s", id)
		}
	}
	for key := range beforeRecords {
		if id, ok := strings.CutPrefix(key, "jobs/"); ok && q.s.Jobs[id] == nil {
			if _, dirty := q.records.jobs[id]; !dirty {
				return fmt.Errorf("job removed without touchJob: %s", id)
			}
		}
	}
	for _, group := range []string{"extensions/", "work_observations/", "storage_observations/", "external_memory/"} {
		keys := map[string]bool{}
		for key := range beforeRecords {
			if strings.HasPrefix(key, group) {
				keys[key] = true
			}
		}
		switch group {
		case "extensions/":
			for id := range q.s.Extensions {
				keys[group+id] = true
			}
		case "work_observations/":
			for id := range q.s.WorkObservations {
				keys[group+id] = true
			}
		case "storage_observations/":
			for id := range q.s.StorageObservations {
				keys[group+id] = true
			}
		case "external_memory/":
			for id := range q.s.ExternalMemory {
				keys[group+id] = true
			}
		}
		for key := range keys {
			if _, dirty := q.records.catalogUndo[key]; dirty {
				continue
			}
			value, exists := q.catalogValue(key)
			if !exists {
				return fmt.Errorf("catalog removed without touchCatalog: %s", key)
			}
			if p, ok := value.(WorkObservation); ok {
				value = durableWorkObservation(p)
			}
			if !equal(beforeRecords[key], value) {
				return fmt.Errorf("catalog decision changed without touchCatalog: %s", key)
			}
		}
	}
	return nil
}

func auditSubmission(q *Queue, r *submissionRecord) *submissionRecord {
	if r == nil {
		return nil
	}
	copy := *r
	if copy.Job != nil && q.s.Jobs[copy.JobID] == copy.Job {
		copy.Job = nil
	}
	return &copy
}

// Guard against the principal failure mode of an explicit journal: a future
// mutator changes a decision but forgets to enroll its undo/durable record.
func TestQueueRecordsAuditDetectsMissingTouches(t *testing.T) {
	q, _ := fixture(t)
	check(t, q, machine("worker", "fake"), nil)
	j := submit(t, q, spec("experiment"))
	drainAdmissionWait(t, q)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	for _, kind := range []string{"job", "machine", "catalog"} {
		t.Run(kind, func(t *testing.T) {
			old, err := q.beginRecords()
			if err != nil {
				t.Fatal(err)
			}
			var restore func()
			switch kind {
			case "job":
				previous := q.s.Jobs[j.ID].DiagnosticCode
				q.s.Jobs[j.ID].DiagnosticCode = "missing_touch"
				restore = func() { q.s.Jobs[j.ID].DiagnosticCode = previous }
			case "machine":
				previous := q.s.Machines["worker"].Leaving
				q.s.Machines["worker"].Leaving = !previous
				restore = func() { q.s.Machines["worker"].Leaving = previous }
			case "catalog":
				q.s.WorkObservations["missing"] = WorkObservation{StaleReason: "identity changed"}
				restore = func() { delete(q.s.WorkObservations, "missing") }
			}
			if err := checkQueueRecords(q); err == nil {
				t.Fatal("unmarked decision passed full-diff assertion")
			}
			restore()
			q.rollbackRecords(old)
		})
	}
}
