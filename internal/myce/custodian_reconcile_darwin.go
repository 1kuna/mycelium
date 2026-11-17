package myce

import (
	"context"
	"errors"
)

func fenceCustodianStartup(context.Context, string, string) error {
	return errors.New("custodian startup recovery is Linux-only")
}

func privilegedCustodianStartupFence(context.Context, string, string) error {
	return errors.New("custodian startup recovery is Linux-only")
}
func InspectCustodianHandles(context.Context, string, string, int) error {
	return errors.New("custodian handle inspection is Linux-only")
}
