package myce

import (
	"strings"
	"testing"
)

func TestWorkerUnitCgroupSettings(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{OS: "linux"}}
	texts := []string{c.ServiceText()}
	for _, text := range texts {
		for _, setting := range []string{"Delegate=memory", "KillMode=control-group", "SendSIGKILL=yes", "TimeoutStopSec=10s", "Restart=on-failure"} {
			if !strings.Contains(text, setting) {
				t.Errorf("missing %s", setting)
			}
		}
	}
}
