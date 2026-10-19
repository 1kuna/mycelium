package myce

import "testing"

func setMaintenance(t *testing.T, q *Queue, host, action string) error {
	t.Helper()
	_, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: action, Stop: action == "disable"})
	return err
}
