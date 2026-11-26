//go:build !linux

package myce

import (
	"context"
	"errors"
)

func nativeXeFreeMemory(context.Context, string) (float64, error) {
	return 0, errors.New("Xe memory query unavailable")
}
