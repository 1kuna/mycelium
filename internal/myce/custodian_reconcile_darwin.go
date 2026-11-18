package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
)

func custodianManagerAbsent(ctx context.Context, id string) error {
	cmd := exec.CommandContext(ctx, "launchctl", "print", custodianDomain()+"/"+custodianLabel(id))
	cmd.Env = withoutMyceToken(os.Environ())
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if ctx.Err() == nil && errors.As(err, &exit) && exit.ExitCode() == 113 && strings.Contains(string(out), `Could not find service "`+custodianLabel(id)+`"`) {
		return nil
	}
	return errors.New("custodian launchd job absence unconfirmed; custody retained")
}

func fenceCustodianStartup(context.Context, string, string) error {
	return errors.New("custodian startup recovery is Linux-only")
}

func privilegedCustodianStartupFence(context.Context, string, string) error {
	return errors.New("custodian startup recovery is Linux-only")
}
func InspectCustodianHandles(context.Context, string, string, int) error {
	return errors.New("custodian handle inspection is Linux-only")
}
