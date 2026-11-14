package myce

import (
	"encoding/json"
	"reflect"
	"strings"
)

// The journal owns detached before-values only for records a mutator touches.
// External-memory generations are immutable and can be borrowed for undo.
type catalogPrevious struct {
	value  any
	exists bool
}

func durableWorkObservation(point WorkObservation) WorkObservation {
	if point.StaleReason == "component unavailable" || point.StaleReason == "resource allowance or power mode changed" {
		point.StaleReason = ""
	}
	return point
}

func (q *Queue) touchCatalog(key string) {
	if q.records == nil || !q.records.updating {
		return
	}
	if _, touched := q.records.catalogUndo[key]; touched {
		return
	}
	value, exists := q.catalogValue(key)
	group, _, _ := strings.Cut(key, "/")
	if exists && group != "external_memory" {
		value = copyObservationValue(reflect.ValueOf(value)).Interface()
	}
	if point, ok := value.(WorkObservation); ok {
		_, id, _ := strings.Cut(key, "/")
		if reason, touched := q.records.staleness[id]; touched {
			point.StaleReason = reason
			value = point
		}
	}
	q.records.catalogUndo[key] = catalogPrevious{value, exists}
}

func (q *Queue) catalogValue(key string) (any, bool) {
	group, id, _ := strings.Cut(key, "/")
	var value any
	var exists bool
	switch group {
	case "extensions":
		value, exists = q.s.Extensions[id]
	case "work_observations":
		value, exists = q.s.WorkObservations[id]
	case "storage_observations":
		value, exists = q.s.StorageObservations[id]
	case "external_memory":
		value, exists = q.s.ExternalMemory[id]
	}
	return value, exists
}

func (q *Queue) restoreCatalogs() {
	for key, previous := range q.records.catalogUndo {
		group, id, _ := strings.Cut(key, "/")
		switch group {
		case "extensions":
			if previous.exists {
				q.s.Extensions[id] = previous.value.(Extension)
			} else {
				delete(q.s.Extensions, id)
			}
		case "work_observations":
			if previous.exists {
				q.s.WorkObservations[id] = previous.value.(WorkObservation)
			} else {
				delete(q.s.WorkObservations, id)
			}
		case "storage_observations":
			if previous.exists {
				q.s.StorageObservations[id] = previous.value.(StorageObservation)
			} else {
				delete(q.s.StorageObservations, id)
			}
		case "external_memory":
			if previous.exists {
				q.s.ExternalMemory[id] = previous.value.(*externalMemoryRing)
			} else {
				delete(q.s.ExternalMemory, id)
			}
		}
	}
}

func (q *Queue) touchMachine(id string) *Machine {
	m := q.s.Machines[id]
	if q.records != nil && q.records.updating {
		if _, touched := q.records.machines[id]; !touched {
			var old *Machine
			if m != nil {
				copy := undoMachine(m)
				old = &copy
			}
			q.records.machines[id] = old
		}
	}
	return m
}

// Each check-in journals its machine. Copy it in memory, as catalogs do; a
// JSON round trip of the whole worker report held q.mu on every heartbeat.
// Private fields are dropped as JSON dropped them, except the two cloneMachine keeps.
func undoMachine(m *Machine) Machine {
	out := copyObservationValue(reflect.ValueOf(*m)).Interface().(Machine)
	out.toolInventory = m.toolInventory
	if out.HostHealth != nil {
		out.HostHealth.delivery = m.HostHealth.delivery
	}
	return out
}

// View journals preserve rollback without enrolling a job or measurement in
// durable writes. A permanent invalidation uses touchCatalog instead.
func (q *Queue) touchJobReason(j *Job) {
	if q.records != nil && q.records.updating {
		if _, touched := q.records.reasons[j.ID]; !touched {
			q.records.reasons[j.ID] = j.Reason
		}
	}
}
func (q *Queue) touchCurveLabel(id string) {
	if q.records != nil && q.records.updating {
		if _, touched := q.records.staleness[id]; !touched {
			q.records.staleness[id] = q.s.WorkObservations[id].StaleReason
		}
	}
}

// The small global control record is always dirty: usage accounting advances
// its transaction anchor. Jobs, inventories and catalogs have separate journals.
func queueBase(s state, machines bool) state {
	s.Jobs, s.Submissions = nil, nil
	s.Extensions, s.WorkObservations, s.StorageObservations, s.ExternalMemory = nil, nil, nil, nil
	s.Health.Events = nil
	if !machines {
		s.Machines = nil
	}
	return s
}

type recordUndo struct {
	base           state
	catalogs       state // Borrow map containers; touched entries have detached undo.
	machines       map[string]*Machine
	health         []HealthAlert
	efficiencyJSON json.RawMessage
}
