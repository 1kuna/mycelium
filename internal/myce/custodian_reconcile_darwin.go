package myce

import (
	"context"
	"errors"
)

func fenceCustodianStartup(context.Context, string, string) error {
	return errors.New("custodian startup recovery is Linux-only")
}
