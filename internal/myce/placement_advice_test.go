package myce

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPlacementAdvisoriesPreserveIntentAndPrivacy(t *testing.T) {
	s := Spec{AllowedMachines: []string{"private-host"}, Needs: ResourceNeeds{GPU: &GPUSelection{Devices: []string{"private-device"}}}}
	before, _ := json.Marshal(s)
	advice := PlacementAdvisories(s, nil)
	if len(advice) != 2 || !strings.Contains(advice[0], "omit allowed_machines") || !strings.Contains(advice[1], "needs.gpu.count: 1") {
		t.Fatal(advice)
	}
	after, _ := json.Marshal(s)
	if string(before) != string(after) || strings.Contains(strings.Join(advice, " "), "private-") {
		t.Fatal("advice changed intent or disclosed selectors")
	}
	reasons := map[string]string{"host": "private parity evidence", "device": "private hardware requirement"}
	if got := PlacementAdvisories(s, reasons); len(got) != 0 {
		t.Fatal(got)
	}
	delete(reasons, "device")
	if got := PlacementAdvisories(s, reasons); len(got) != 1 || !strings.HasPrefix(got[0], "Device pin") {
		t.Fatal(got)
	}
	s.ExtensionCheck = &ExtensionCheckRequest{}
	if got := PlacementAdvisories(s, nil); len(got) != 0 {
		t.Fatal("qualification is an intentional exact target", got)
	}
	if got := PlacementAdvisories(Spec{}, nil); len(got) != 0 {
		t.Fatal("unrestricted submission warned", got)
	}
}
