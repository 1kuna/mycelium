//go:build !linux

package myce

import (
	"context"
	"errors"
)

func nativeXeActivity(context.Context, string) (float64, error) {
	return 0, errors.New("Xe PMU unavailable")
}
