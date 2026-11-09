package myce

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestOnboardingLegacyMountFlagCannotCreateLocalNAS(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		for _, allow := range []bool{false, true} {
			home := t.TempDir()
			c, _ := PlanInstall("worker", home, "", home+"/absent-nas", "http://queue.invalid", "hypha", Detection{OS: osname, Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
			c.RequireMountedNAS = false
			c.AllowDeviceless = allow
			err := prepareInstallNAS(context.Background(), c, func(context.Context, string, ...string) ([]byte, error) { return nil, nil })
			if err == nil {
				t.Fatalf("%s created local storage for an absent NAS", osname)
			}
			if _, err = os.Stat(c.NAS); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s created missing NAS root", osname)
			}
		}
	}
}
