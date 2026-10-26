//go:build !darwin

package myce

import (
	"context"
	"errors"
)

func cachedMountInventory(context.Context) ([]byte, error) {
	return nil, errors.New("cached Darwin mount inventory unavailable on this OS")
}
