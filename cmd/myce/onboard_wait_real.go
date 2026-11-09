//go:build !myce_test

package main

import (
	"context"

	"mycelium/internal/myce"
)

func waitEnrollment(ctx context.Context, config myce.ServiceConfig, client myce.Client, options myce.EnrollmentWait) (myce.EnrollmentResult, error) {
	return myce.WaitEnrollment(ctx, config, client, options)
}
