package myce

import (
	"context"
	"testing"
)

func TestEnrollmentPreflightStorageWarningAndMisconfiguration(t *testing.T) {
	c, _ := onboardingNAS(t, "linux")
	c.AllowDeviceless = true
	for _, unavailable := range []bool{false, true} {
		command := func(_ context.Context, name string, _ ...string) ([]byte, error) {
			if name != "cat" {
				return nil, nil
			}
			if unavailable {
				return nil, context.DeadlineExceeded
			}
			return []byte("source " + c.NAS + " ext4 rw 0 0\n"), nil
		}
		err := PreflightEnrollmentInstall(context.Background(), c, "unused", command)
		if unavailable {
			if err != nil {
				t.Fatal("transient NAS failure blocked enrollment preflight", err)
			}
		} else {
			requirePreflightCode(t, err, "nas_not_mounted")
		}
	}
}
