//go:build !windows

package main

import (
	"context"
	"errors"
)

func platformRun(context.Context, string) error {
	return errors.New("host helper runs only on Windows; WSL uses myce join")
}
