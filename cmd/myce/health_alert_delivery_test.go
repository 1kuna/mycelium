package main

import (
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestHealthAlertDeliveryStatusExplanation(t *testing.T) {
	output := captureJoiningOutput(t, func() error {
		printStatus(myce.Snapshot{HealthAlertDelivery: &myce.HealthAlertDelivery{Pending: 3}}, false)
		return nil
	})
	if !strings.Contains(output, "health notifier is not configured") || !strings.Contains(output, "3 alert openings or closings have no delivery acknowledgement") {
		t.Fatal(output)
	}
	// A reply from an older queue has no configuration evidence.
	legacy := captureJoiningOutput(t, func() error {
		printStatus(myce.Snapshot{}, false)
		return nil
	})
	if strings.Contains(legacy, "health notifier") {
		t.Fatal(legacy)
	}
}

func TestStatusAlertSectionText(t *testing.T) {
	output := captureJoiningOutput(t, func() error {
		printStatus(myce.Snapshot{HealthAlerts: []myce.HealthAlert{{Kind: "efficiency_regression", State: "open", Reason: "reservation_gap_gb", Host: "h", Count: 3}}}, false)
		return nil
	})
	if !strings.Contains(output, "alert efficiency_regression: open host=h job= reason=reservation_gap_gb count=3 since=") {
		t.Fatal(output)
	}
}
