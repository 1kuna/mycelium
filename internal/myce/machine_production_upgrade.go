package myce

import (
	"encoding/json"
	"errors"
	"time"
)

// A one-time projection of the production queue's persisted close fence. No old
// host-control route or runtime decoder is restored. Never infer release from age.
type productionDrain struct {
	Sessions map[string]string `json:"sessions"`
}

func (q *Queue) importProductionDrain(data []byte) error {
	var old struct {
		Hosts map[string]struct {
			Desired   string            `json:"desired"`
			Revision  uint64            `json:"revision"`
			Operation string            `json:"operation_id"`
			Boot      string            `json:"boot_id"`
			At        time.Time         `json:"accepted_at"`
			Sessions  map[string]string `json:"closed_leaving"`
		} `json:"host_intents"`
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &old); err != nil {
		return err
	}
	for host, closed := range old.Hosts {
		if closed.Desired != "closed" {
			continue
		}
		if host == "" || closed.Revision == 0 || closed.Operation == "" || closed.Boot == "" {
			return errors.New("invalid production host close fence")
		}
		if q.s.HostMaintenance == nil {
			q.s.HostMaintenance = map[string]HostMaintenance{}
		}
		policy, known := q.s.HostMaintenance[host]
		// New queue-owned policy always wins on subsequent startup.
		if known {
			continue
		}
		policy.Mode = "disabled"
		policy.UpdatedAt = q.clock.Now()
		q.s.HostMaintenance[host] = policy
		for _, m := range q.s.Machines {
			if hostID(m) == host {
				if q.s.HostMaintenance[host].MemoryBudget != nil && q.s.HostMaintenance[host].MemoryBudget.GiB != m.HostMemoryBudgetGB {
					return migrationError("machine_migration_conflict", "production host memory allowances disagree")
				}
				if err := q.registerMaintenanceHost(m); err != nil {
					return err
				}
			}
		}
		if q.s.ProductionDrains == nil {
			q.s.ProductionDrains = map[string]productionDrain{}
		}
		q.s.ProductionDrains[host] = productionDrain{Sessions: closed.Sessions}
	}
	return nil
}
func (q *Queue) productionSourceSettled(m *Machine) bool {
	p := q.s.ProductionDrains[hostID(m)]
	return m.Gone && m.Session != "" && p.Sessions[m.ID] == m.Session && len(q.hostObligations(hostID(m))) == 0
}
